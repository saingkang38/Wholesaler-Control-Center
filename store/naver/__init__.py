import os
import time
import threading
import bcrypt
import pybase64
import requests

from store.naver.rate_control import call as _api_call

API_BASE = "https://api.commerce.naver.com/external"

# 토큰 캐시: client_id → (access_token, expires_at)
_token_cache: dict = {}
_token_cache_lock = threading.Lock()
_TOKEN_TTL = 50 * 60  # 50분 (토큰 유효기간 1시간보다 보수적으로)


def _get_access_token(client_id: str = None, client_secret: str = None) -> str:
    if not client_id:
        client_id = os.getenv("NAVER_CLIENT_ID")
    if not client_secret:
        client_secret = os.getenv("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise Exception("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정")

    # 캐시 확인
    with _token_cache_lock:
        cached = _token_cache.get(client_id)
        if cached and time.time() < cached[1]:
            return cached[0]

    timestamp = str(int(time.time() * 1000))
    password = f"{client_id}_{timestamp}"
    hashed = bcrypt.hashpw(password.encode("utf-8"), client_secret.encode("utf-8"))
    signature = pybase64.standard_b64encode(hashed).decode("utf-8")

    resp = requests.post(
        f"{API_BASE}/v1/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "timestamp": timestamp,
            "client_secret_sign": signature,
            "type": "SELF",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=10,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]

    with _token_cache_lock:
        _token_cache[client_id] = (token, time.time() + _TOKEN_TTL)

    return token


def get_products(page: int = 1, size: int = 100, token: str = None, client_id: str = None, client_secret: str = None) -> dict:
    if not token:
        token = _get_access_token(client_id, client_secret)
    resp = _api_call("POST", f"{API_BASE}/v1/products/search", token,
                     json_body={"page": page, "size": size}, timeout=15)
    return resp.json()


def get_all_products(client_id: str = None, client_secret: str = None, on_page=None) -> list:
    """on_page(page, total_pages, fetched_count) — 페이지 수신 시 호출되는 콜백"""
    all_items = []
    page = 1
    size = 100

    import logging
    _logger = logging.getLogger(__name__)

    token = _get_access_token(client_id, client_secret)

    while True:
        retries = 0
        while retries < 5:
            try:
                data = get_products(page=page, size=size, token=token, client_id=client_id, client_secret=client_secret)
                break
            except Exception as e:
                # 401: 토큰 만료 → 즉시 재발급 후 재시도
                if "401" in str(e):
                    _logger.warning(f"[naver] 페이지 {page} 토큰 만료, 재발급 후 재시도")
                    try:
                        token = _get_access_token(client_id, client_secret)
                    except Exception as te:
                        _logger.error(f"[naver] 토큰 재발급 실패: {te}")
                        retries = 5
                        break
                retries += 1
                wait = 10 * retries
                _logger.warning(f"[naver] 페이지 {page} 오류({e}), {wait}초 후 재시도 ({retries}/5)")
                time.sleep(wait)
        else:
            _logger.error(f"[naver] 페이지 {page} 5회 재시도 실패, 건너뜀")
            break

        items = data.get("contents", [])
        all_items.extend(items)

        total_pages = data.get("totalPages", 1)
        if on_page:
            on_page(page, total_pages, len(all_items))

        if data.get("last", True) or page >= total_pages:
            break
        page += 1
        time.sleep(1)

    return all_items


def change_status(origin_product_no: int, status: str, client_id: str = None, client_secret: str = None) -> bool:
    token = _get_access_token(client_id, client_secret)
    _api_call("PUT",
              f"{API_BASE}/v1/products/origin-products/{origin_product_no}/change-status",
              token, json_body={"statusType": status}, timeout=10)
    return True


def update_seller_management_code(origin_product_no: int, seller_management_code: str, client_id: str = None, client_secret: str = None) -> bool:
    token = _get_access_token(client_id, client_secret)
    _api_call("PATCH", f"{API_BASE}/v1/products/origin-products/multi-update", token,
              json_body={
                  "multiProductUpdateRequestVos": [{
                      "originProductNo": int(origin_product_no),
                      "multiUpdateTypes": ["SELLER_MANAGEMENT_CODE"],
                      "sellerManagementCode": seller_management_code,
                  }]
              }, timeout=10)
    return True


def update_price(origin_product_no: int, sale_price: int, client_id: str = None, client_secret: str = None) -> bool:
    token = _get_access_token(client_id, client_secret)
    _api_call("PATCH", f"{API_BASE}/v1/products/origin-products/multi-update", token,
              json_body={
                  "multiProductUpdateRequestVos": [{
                      "originProductNo": int(origin_product_no),
                      "multiUpdateTypes": ["SALE_PRICE"],
                      "productSalePrice": {"salePrice": int(sale_price)},
                  }]
              }, timeout=10)
    return True
