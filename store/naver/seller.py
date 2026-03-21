import requests
from store.naver import API_BASE, _get_access_token


def get_seller_account(client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/seller/account",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_categories(category_id: str = "root", client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/categories/{category_id}/sub-categories",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def search_origin_areas(keyword: str, client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/product-origin-areas/query",
        headers={"Authorization": f"Bearer {token}"},
        params={"keyword": keyword},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_inspection_status(client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/product-inspections/channel-products",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()
