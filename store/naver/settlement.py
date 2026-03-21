import requests
from store.naver import API_BASE, _get_access_token


def get_daily_settlement(start_date: str, end_date: str, client_id: str = None, client_secret: str = None) -> dict:
    """start_date, end_date: YYYY-MM-DD 형식"""
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/pay-settle/settle/daily",
        headers={"Authorization": f"Bearer {token}"},
        params={"startDate": start_date, "endDate": end_date},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_vat_daily(year_month: str, client_id: str = None, client_secret: str = None) -> dict:
    """year_month: YYYY-MM 형식. startDate/endDate로 변환해서 호출."""
    import calendar
    token = _get_access_token(client_id, client_secret)
    ym = year_month[:7]  # YYYY-MM 보장
    year, month = int(ym[:4]), int(ym[5:7])
    last_day = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"
    resp = requests.get(
        f"{API_BASE}/v1/pay-settle/vat/daily",
        headers={"Authorization": f"Bearer {token}"},
        params={"startDate": start_date, "endDate": end_date},
        timeout=15,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text}")
    return resp.json()
