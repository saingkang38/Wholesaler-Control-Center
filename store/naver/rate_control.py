"""
Naver Commerce API 공통 호출 래퍼

응답 헤더 기반 동적 속도 제어:
- RateLimit-Remaining / Quota-Remaining 읽어서 선제 감속
- 429 발생 시 GW.RATE_LIMIT / GW.QUOTA_LIMIT 분기 후 지수 백오프 + 지터
- 엔드포인트 그룹별 독립 상태 관리
- 모든 호출에 구조화 로그 (Trace-ID 포함)
"""

import json
import logging
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# 보수적 초기 안전값 (실제 제어는 응답 헤더로 보정)
_DEFAULT_MIN_INTERVAL = 0.3   # 초 (초당 ~3건)
_LOW_REMAINING_THRESHOLD = 5  # Remaining 이 이 값 이하면 선제 감속
_MAX_RETRIES = 4


@dataclass
class _EndpointState:
    """엔드포인트 그룹별 rate/quota 상태"""
    replenish_rate: Optional[int] = None     # 초당 최대 요청 수
    burst_capacity: Optional[int] = None
    rate_remaining: Optional[int] = None
    quota_period: Optional[str] = None       # SECONDS or ROUND
    quota_limit: Optional[int] = None
    quota_remaining: Optional[int] = None
    last_call_at: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)


# 엔드포인트 그룹 → 상태 (스레드 안전)
_states: dict[str, _EndpointState] = {}
_states_lock = threading.Lock()


def _get_state(endpoint_group: str) -> _EndpointState:
    with _states_lock:
        if endpoint_group not in _states:
            _states[endpoint_group] = _EndpointState()
        return _states[endpoint_group]


def _endpoint_group(path: str) -> str:
    """URL 경로에서 엔드포인트 그룹 키 추출 (상위 2단계)"""
    parts = path.strip("/").split("/")
    return "/".join(parts[:3]) if len(parts) >= 3 else path.strip("/")


def _parse_headers(state: _EndpointState, headers: dict):
    """응답 헤더에서 rate/quota 상태 갱신"""
    def _int(key):
        v = headers.get(key)
        return int(v) if v is not None else None

    rr = _int("GNCP-GW-RateLimit-Replenish-Rate")
    if rr is not None:
        state.replenish_rate = rr
    bc = _int("GNCP-GW-RateLimit-Burst-Capacity")
    if bc is not None:
        state.burst_capacity = bc
    rem = _int("GNCP-GW-RateLimit-Remaining")
    if rem is not None:
        state.rate_remaining = rem

    qp = headers.get("GNCP-GW-Quota-Period")
    if qp:
        state.quota_period = qp
    ql = _int("GNCP-GW-Quota-Limit")
    if ql is not None:
        state.quota_limit = ql
    qr = _int("GNCP-GW-Quota-Remaining")
    if qr is not None:
        state.quota_remaining = qr


def _min_interval(state: _EndpointState) -> float:
    """헤더 기반 최소 호출 간격 계산 (replenish_rate 기준)"""
    if state.replenish_rate and state.replenish_rate > 0:
        return 1.0 / state.replenish_rate
    return _DEFAULT_MIN_INTERVAL


def _pre_throttle(state: _EndpointState):
    """호출 전 선제 대기"""
    now = time.monotonic()
    interval = _min_interval(state)

    # Remaining이 낮으면 추가 감속
    if state.rate_remaining is not None and state.rate_remaining <= _LOW_REMAINING_THRESHOLD:
        interval = max(interval, 1.0)
    if state.quota_remaining is not None and state.quota_remaining <= _LOW_REMAINING_THRESHOLD:
        interval = max(interval, 2.0)

    elapsed = now - state.last_call_at
    if elapsed < interval:
        time.sleep(interval - elapsed)
    state.last_call_at = time.monotonic()


def _backoff_wait(attempt: int, state: _EndpointState, error_code: str):
    """429 발생 시 대기 시간 계산"""
    if error_code == "GW.QUOTA_LIMIT":
        # Quota 초과: period가 ROUND면 장기 대기, SECONDS면 짧게
        if state.quota_period == "ROUND":
            wait = 60.0 + random.uniform(0, 10)
        else:
            wait = 10.0 * (2 ** attempt) + random.uniform(0, 3)
    else:
        # Rate Limit: 지수 백오프
        wait = (2 ** attempt) + random.uniform(0, 1)

    wait = min(wait, 120.0)  # 최대 2분
    logger.warning(
        f"[naver_api] 429 {error_code} → {wait:.1f}초 대기 (attempt={attempt})"
    )
    time.sleep(wait)


def call(
    method: str,
    url: str,
    token: str,
    *,
    json_body=None,
    data=None,
    files=None,
    params=None,
    timeout: int = 15,
    extra_headers: dict = None,
) -> requests.Response:
    """
    Naver Commerce API 공통 호출.
    응답 헤더 파싱, 선제 감속, 429 처리, 구조화 로그 포함.
    성공 시 Response 반환, 실패 시 Exception.
    """
    from urllib.parse import urlparse
    path = urlparse(url).path
    group = _endpoint_group(path)
    state = _get_state(group)

    headers = {"Authorization": f"Bearer {token}"}
    if json_body is not None and files is None:
        headers["Content-Type"] = "application/json"
    if extra_headers:
        headers.update(extra_headers)

    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        with state.lock:
            _pre_throttle(state)

        t0 = time.monotonic()
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                json=json_body,
                data=data,
                files=files,
                params=params,
                timeout=timeout,
            )
        except requests.RequestException as e:
            logger.error(f"[naver_api] 네트워크 오류: {method} {url} → {e}")
            last_exc = e
            time.sleep(2 ** attempt)
            continue

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        trace_id = resp.headers.get("GNCP-GW-Trace-ID", "-")

        with state.lock:
            _parse_headers(state, resp.headers)

        # 구조화 로그
        log_extra = {
            "method": method,
            "url": url,
            "status": resp.status_code,
            "trace_id": trace_id,
            "elapsed_ms": elapsed_ms,
            "rate_remaining": state.rate_remaining,
            "quota_remaining": state.quota_remaining,
            "quota_period": state.quota_period,
        }

        if resp.status_code == 429:
            # 오류 코드 파싱
            error_code = "GW.RATE_LIMIT"
            try:
                body = resp.json()
                error_code = body.get("code", error_code)
            except Exception:
                pass
            log_extra["error_code"] = error_code
            logger.warning(f"[naver_api] 429 | {json.dumps(log_extra, ensure_ascii=False)}")

            if attempt >= _MAX_RETRIES:
                raise Exception(f"429 {error_code}: {resp.text[:300]}")
            _backoff_wait(attempt, state, error_code)
            last_exc = Exception(f"429 {error_code}")
            continue

        if not resp.ok:
            logger.warning(
                f"[naver_api] {resp.status_code} | {json.dumps(log_extra, ensure_ascii=False)} | {resp.text[:200]}"
            )
            resp.raise_for_status()

        logger.debug(f"[naver_api] OK | {json.dumps(log_extra, ensure_ascii=False)}")
        return resp

    raise last_exc or Exception(f"[naver_api] {_MAX_RETRIES}회 재시도 실패: {method} {url}")
