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


def _get_addressbooks(client_id: str = None, client_secret: str = None) -> list:
    """주소록 전체 조회 (/v1/seller/addressbooks-for-page)"""
    token = _get_access_token(client_id, client_secret)
    all_items = []
    page = 1
    while True:
        resp = requests.get(
            f"{API_BASE}/v1/seller/addressbooks-for-page",
            headers={"Authorization": f"Bearer {token}"},
            params={"page": page, "pageSize": 100},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        contents = data.get("addressBooks") or []
        all_items.extend(contents)
        if page >= data.get("totalPage", 1):
            break
        page += 1
    return all_items


def get_return_locations(client_id: str = None, client_secret: str = None) -> list:
    """반품지 조회 (addressType: REFUND_OR_EXCHANGE)"""
    items = _get_addressbooks(client_id, client_secret)
    return [i for i in items if i.get("addressType") == "REFUND_OR_EXCHANGE"]


def get_departure_locations(client_id: str = None, client_secret: str = None) -> list:
    """출고지 조회 (addressType: RELEASE)"""
    items = _get_addressbooks(client_id, client_secret)
    return [i for i in items if i.get("addressType") == "RELEASE"]


def get_delivery_templates(client_id: str = None, client_secret: str = None) -> list:
    """배송비 템플릿 목록 조회"""
    token = _get_access_token(client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}"}

    # 후보 엔드포인트 순서대로 시도
    candidates = [
        f"{API_BASE}/v1/delivery-fee/templates",
        f"{API_BASE}/v1/products/delivery-fee/templates",
    ]
    for url in candidates:
        try:
            resp = requests.get(url, headers=headers, params={"page": 1, "pageSize": 100}, timeout=10)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            for key in ("deliveryFeeTemplates", "templates", "contents"):
                if key in data:
                    return data[key]
            return []
        except requests.HTTPError:
            continue
    raise RuntimeError("배송비 템플릿 API를 찾을 수 없습니다 (404). 네이버 커머스 API 문서에서 정확한 엔드포인트를 확인해주세요.")


def get_inspection_status(client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/product-inspections/channel-products",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()
