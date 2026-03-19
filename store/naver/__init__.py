import os
import time
import bcrypt
import pybase64
import requests

API_BASE = "https://api.commerce.naver.com/external"


def _get_access_token(client_id: str = None, client_secret: str = None) -> str:
    if not client_id:
        client_id = os.getenv("NAVER_CLIENT_ID")
    if not client_secret:
        client_secret = os.getenv("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise Exception("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 미설정")

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
    return resp.json()["access_token"]


def get_products(page: int = 1, size: int = 100, token: str = None, client_id: str = None, client_secret: str = None) -> dict:
    if not token:
        token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v1/products/search",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"page": page, "size": size},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_all_products(client_id: str = None, client_secret: str = None) -> list:
    all_items = []
    page = 1
    size = 100

    token = _get_access_token(client_id, client_secret)

    while True:
        retries = 0
        while retries < 5:
            try:
                data = get_products(page=page, size=size, token=token, client_id=client_id, client_secret=client_secret)
                break
            except Exception as e:
                retries += 1
                wait = 10 * retries
                print(f"[naver] 페이지 {page} 오류({e}), {wait}초 후 재시도 ({retries}/5)")
                time.sleep(wait)
        else:
            print(f"[naver] 페이지 {page} 5회 재시도 실패, 건너뜀")
            break

        items = data.get("contents", [])
        all_items.extend(items)

        total_pages = data.get("totalPages", 1)
        if data.get("last", True) or page >= total_pages:
            break
        page += 1
        time.sleep(1)

    return all_items


def change_status(origin_product_no: int, status: str, client_id: str = None, client_secret: str = None) -> bool:
    token = _get_access_token(client_id, client_secret)
    resp = requests.put(
        f"{API_BASE}/v1/products/origin-products/{origin_product_no}/change-status",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"statusType": status},
        timeout=10,
    )
    resp.raise_for_status()
    return True


def update_price(origin_product_no: int, sale_price: int, client_id: str = None, client_secret: str = None) -> bool:
    token = _get_access_token(client_id, client_secret)
    resp = requests.patch(
        f"{API_BASE}/v1/products/origin-products/multi-update",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "originProductNos": [origin_product_no],
            "salePrice": sale_price,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return True
