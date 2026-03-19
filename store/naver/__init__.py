import os
import time
import bcrypt
import pybase64
import requests

API_BASE = "https://api.commerce.naver.com/external"


def _get_access_token() -> str:
    client_id = os.getenv("NAVER_CLIENT_ID")
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


def get_products(page: int = 1, size: int = 100) -> dict:
    token = _get_access_token()
    resp = requests.post(
        f"{API_BASE}/v1/products/search",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"pageNum": page, "pageSize": size},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_all_products() -> list:
    all_items = []
    page = 1
    size = 100

    while True:
        data = get_products(page=page, size=size)
        items = data.get("contents", [])
        all_items.extend(items)

        total = data.get("totalCount", 0)
        if len(all_items) >= total or not items:
            break
        page += 1

    print(f"[naver] 스토어 상품 조회 완료: {len(all_items)}개")
    return all_items


def change_status(origin_product_no: int, status: str) -> bool:
    """
    status: SALE / SUSPENSION / CLOSE (판매중 / 판매중지 / 품절)
    """
    token = _get_access_token()
    resp = requests.put(
        f"{API_BASE}/v1/products/origin-products/{origin_product_no}/change-status",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"statusType": status},
        timeout=10,
    )
    resp.raise_for_status()
    return True


def update_price(origin_product_no: int, sale_price: int) -> bool:
    token = _get_access_token()
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
