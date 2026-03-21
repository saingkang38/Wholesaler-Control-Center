import requests
from store.naver import API_BASE, _get_access_token


def create_group_product(payload: dict, client_id: str, client_secret: str) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v2/standard-group-products",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text[:400]}")
    return resp.json()


def get_group_product(group_product_no: int, client_id: str, client_secret: str) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v2/standard-group-products/{group_product_no}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text[:400]}")
    return resp.json()


def update_group_product(group_product_no: int, payload: dict, client_id: str, client_secret: str) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.put(
        f"{API_BASE}/v2/standard-group-products/{group_product_no}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text[:400]}")
    return resp.json() if resp.text else {}


def delete_group_product(group_product_no: int, client_id: str, client_secret: str) -> None:
    token = _get_access_token(client_id, client_secret)
    resp = requests.delete(
        f"{API_BASE}/v2/standard-group-products/{group_product_no}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text[:400]}")


def get_group_product_status(request_id: str, client_id: str, client_secret: str) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v2/standard-group-products/status",
        headers={"Authorization": f"Bearer {token}"},
        params={"requestId": request_id},
        timeout=15,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text[:400]}")
    return resp.json()
