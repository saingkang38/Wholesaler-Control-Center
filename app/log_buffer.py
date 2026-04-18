"""
전역 실시간 로그 버퍼.
어느 모듈에서든 push()를 호출하면 /logs/live 페이지에서 실시간으로 확인 가능.
"""
import collections
import threading
import time

_buf: collections.deque = collections.deque(maxlen=500)
_lock = threading.Lock()


def push(msg: str) -> None:
    """로그 메시지를 버퍼에 추가한다."""
    with _lock:
        _buf.append({"t": time.time(), "msg": msg})


def get_since(since_ts: float) -> list:
    """since_ts 이후에 추가된 메시지 목록을 반환한다."""
    with _lock:
        return [e for e in _buf if e["t"] > since_ts]


def get_all() -> list:
    """현재 버퍼의 모든 메시지를 반환한다 (최신 순)."""
    with _lock:
        return list(reversed(_buf))
