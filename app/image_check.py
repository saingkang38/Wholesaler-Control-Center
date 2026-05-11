"""이미지 엑박 검사 도구.

도매처 상품의 메인 이미지·추가 이미지·상세페이지 HTML 안의 이미지 URL이
아직 살아있는지 HEAD 요청으로 점검. 운영자가 수동으로 트리거.

설계 원칙:
- 단순한 즉시 실행 (백그라운드 X) — 페이지 응답 안에서 끝나야 함
- 안전한 검사 한도 (limit ≤ 200) — 한 번에 너무 많이 점검하지 않음
- 검사 결과는 메모리/응답만 — DB에 영구 저장 안 함 (재실행 부담 없음)
"""
import re
import logging
import requests
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

_IMG_SRC_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)
_TIMEOUT = 5.0
_MAX_WORKERS = 20

_UA = {"User-Agent": "Mozilla/5.0 (compatible; ImageCheck/1.0)"}


def _extract_img_urls(html: str) -> list[str]:
    if not html:
        return []
    return _IMG_SRC_RE.findall(html)


def _check_url(url: str) -> dict:
    """HEAD 요청. 일부 서버가 HEAD 거부하면 GET stream으로 재시도."""
    try:
        r = requests.head(url, timeout=_TIMEOUT, allow_redirects=True, headers=_UA)
        if r.status_code in (405, 403):
            r = requests.get(url, timeout=_TIMEOUT, allow_redirects=True, stream=True, headers=_UA)
            r.close()
        return {"url": url, "status": r.status_code, "ok": 200 <= r.status_code < 400}
    except Exception as e:
        return {"url": url, "status": 0, "ok": False, "error": str(e)[:80]}


def collect_master_image_urls(master) -> list[str]:
    """master의 모든 이미지 URL 수집 (중복 제거 + http(s)만)."""
    urls: list[str] = []
    if master.image_url:
        urls.append(master.image_url.strip())
    if master.additional_images:
        urls.extend(line.strip() for line in master.additional_images.split('\n') if line.strip())
    urls.extend(_extract_img_urls(master.detail_description or ""))
    seen: set[str] = set()
    unique: list[str] = []
    for u in urls:
        if not u or u in seen or not u.lower().startswith(('http://', 'https://')):
            continue
        seen.add(u)
        unique.append(u)
    return unique


def check_master_images(master) -> dict:
    """단일 master의 이미지 일괄 검사 (내부적으로 URL 단위 병렬)."""
    urls = collect_master_image_urls(master)
    if not urls:
        return {
            "master_id": master.id,
            "supplier_code": master.supplier_product_code,
            "product_name": master.product_name,
            "total": 0, "broken_count": 0, "broken": [], "all_ok": True,
        }
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        results = list(ex.map(_check_url, urls))
    broken = [r for r in results if not r["ok"]]
    return {
        "master_id": master.id,
        "supplier_code": master.supplier_product_code,
        "product_name": master.product_name,
        "total": len(urls),
        "broken_count": len(broken),
        "broken": broken,
        "all_ok": len(broken) == 0,
    }


def check_batch(masters) -> list[dict]:
    """여러 master 검사 → 깨진 이미지 있는 상품만 결과 반환."""
    results: list[dict] = []
    for m in masters:
        try:
            r = check_master_images(m)
            if not r["all_ok"]:
                results.append(r)
        except Exception as e:
            logger.warning(f"[image_check] master#{m.id} 검사 실패: {e}")
    return results
