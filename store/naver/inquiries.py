import requests
from store.naver import API_BASE, _get_access_token


def get_qnas(answered_type: str = "UNANSWERED", client_id: str = None, client_secret: str = None) -> dict:
    """answered_type: ANSWERED | UNANSWERED"""
    from datetime import date, timedelta
    token = _get_access_token(client_id, client_secret)
    end = date.today()
    start = end - timedelta(days=90)
    resp = requests.get(
        f"{API_BASE}/v1/pay-user/inquiries",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "answeredType": answered_type,
            "startSearchDate": start.isoformat(),
            "endSearchDate": end.isoformat(),
            "size": 100,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def answer_qna(question_id: str, answer_content: str, client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.put(
        f"{API_BASE}/v1/pay-user/inquiries/{question_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"answerContent": answer_content},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_answer_templates(client_id: str = None, client_secret: str = None) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = requests.get(
        f"{API_BASE}/v1/pay-user/inquiries/templates",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    if not resp.ok:
        return {}
    return resp.json()
