import io
import requests
from store.naver import API_BASE, _get_access_token
from store.naver.rate_control import call as _api_call


def get_origin_product(origin_product_no: int, client_id: str, client_secret: str) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = _api_call("GET",
                     f"{API_BASE}/v2/products/origin-products/{origin_product_no}",
                     token, timeout=15)
    return resp.json()


def upload_image_from_url(image_url: str, client_id: str, client_secret: str) -> str:
    """외부 이미지 URL을 네이버 CDN에 업로드하고 네이버 URL 반환"""
    token = _get_access_token(client_id, client_secret)
    img_resp = requests.get(image_url, timeout=15)
    img_resp.raise_for_status()
    content_type = img_resp.headers.get("Content-Type", "image/jpeg")
    ext = "jpg" if "jpeg" in content_type else content_type.split("/")[-1]
    resp = _api_call(
        "POST", f"{API_BASE}/v2/products/images/upload", token,
        files={"imageFiles": (f"image.{ext}", io.BytesIO(img_resp.content), content_type)},
        timeout=30,
    )
    data = resp.json()
    images = data.get("images", [])
    if not images:
        raise Exception(f"이미지 업로드 응답 없음: {data}")
    return images[0].get("url", "")


def register_product(payload: dict, client_id: str, client_secret: str) -> dict:
    """상품 신규 등록. payload: originProduct + smartstoreChannelProduct"""
    token = _get_access_token(client_id, client_secret)
    resp = requests.post(
        f"{API_BASE}/v2/products",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if not resp.ok:
        raise Exception(f"{resp.status_code} {resp.text[:600]}")
    return resp.json()


def update_origin_product(origin_product_no: int, payload: dict, client_id: str, client_secret: str) -> dict:
    token = _get_access_token(client_id, client_secret)
    resp = _api_call("PUT",
                     f"{API_BASE}/v2/products/origin-products/{origin_product_no}",
                     token, json_body=payload, timeout=15)
    return resp.json() if resp.text else {}
