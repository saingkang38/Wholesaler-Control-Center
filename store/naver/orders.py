import requests
from store.naver import API_BASE, _get_access_token


def get_changed_order_statuses(since: str, client_id: str = None, client_secret: str = None) -> dict:
    """since: ISO8601 문자열 (예: 2024-01-01T00:00:00.000Z)"""
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/pay-order/seller/product-orders/last-changed-statuses",
        headers={"Authorization": f"Bearer {token}"},
        params={"lastChangedFrom": since},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def query_product_orders(product_order_ids: list, client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v1/pay-order/seller/product-orders/query",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"productOrderIds": product_order_ids},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def dispatch_orders(dispatch_list: list, client_id: str = None, client_secret: str = None) -> dict:
    """dispatch_list: [{productOrderId, deliveryCompanyCode, trackingNumber}]"""
    token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v1/pay-order/seller/product-orders/dispatch",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"dispatchProductOrders": dispatch_list},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def cancel_order(product_order_id: str, reason: str, client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v1/pay-order/seller/product-orders/{product_order_id}/claim/cancel/approve",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"cancelReason": reason},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def approve_return(product_order_id: str, client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v1/pay-order/seller/product-orders/{product_order_id}/claim/return/approve",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def approve_exchange(product_order_id: str, client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v1/pay-order/seller/product-orders/{product_order_id}/claim/exchange/approve",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def search_orders(start_date: str, end_date: str, statuses: list = None,
                  client_id: str = None, client_secret: str = None) -> dict:
    """start_date, end_date: YYYY-MM-DD. statuses: 상태 코드 리스트 (빈 리스트=전체)"""
    token = _get_access_token(client_id, client_secret)
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "pageSize": 300,
    }
    if statuses:
        params["productOrderStatuses"] = ",".join(statuses)
    resp = requests.get(
        f"{API_BASE}/v1/pay-order/seller/product-orders",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=15,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text[:400]}")
    return resp.json()
