import os
import re
import requests
from xml.etree import ElementTree as ET
from app.collectors.base import BaseCollector

API_URL = "https://www.3mro.co.kr/shop/api_out.php"
PRODUCT_URL = "https://www.3mro.co.kr/shop/goods/goods_view.php?goodsno={code}"

STATUS_MAP = {
    "0": "active",
    "1": "out_of_stock",
}


class Mro3Collector(BaseCollector):
    wholesaler_code = "mro3"

    def run(self, mode: str = None, **kwargs) -> dict:
        m_no = os.getenv("MRO3_M_NO")
        if not m_no:
            return self._error("MRO3_M_NO 미설정")

        mode = mode or os.getenv("MRO3_COLLECT_MODE", "full_all")

        if mode == "full_all":
            return self._collect(m_no, div="all", label="전체")
        elif mode == "incremental":
            return self._collect(m_no, div="mod", label="변동")
        else:
            return self._error(f"알 수 없는 mode: {mode}")

    def _collect(self, m_no: str, div: str, label: str) -> dict:
        print(f"[mro3] {label}상품 수집 (div={div})")
        try:
            raw_xml = self._call_api({"div": div, "m_no": m_no})
            items = self._parse_xml(raw_xml)
        except Exception as e:
            return self._error(str(e)[:300])

        print(f"[mro3] {label}상품 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": 1,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _call_api(self, params: dict) -> str:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        try:
            resp = requests.get(API_URL, params=params, headers=headers, timeout=60)
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise Exception(f"HTTP {resp.status_code}: {e}")
        except requests.RequestException as e:
            raise Exception(f"요청 오류: {e}")

        try:
            text = resp.content.decode("euc-kr", errors="replace")
        except Exception:
            text = resp.text

        text = re.sub(r'<\?xml[^>]*\?>', '<?xml version="1.0" encoding="utf-8"?>', text)

        if not text.strip():
            raise Exception("응답이 비어있음")

        return text

    def _parse_xml(self, raw_xml: str) -> list:
        try:
            root = ET.fromstring(raw_xml.encode("utf-8"))
        except ET.ParseError:
            sanitized = re.sub(r"<!\[CDATA\[(.*?)\]\]>", lambda m: m.group(1), raw_xml, flags=re.DOTALL)
            root = ET.fromstring(sanitized.encode("utf-8"))

        items = []
        for product in root.findall("product"):
            try:
                item = self._normalize(product)
                if item:
                    items.append(item)
            except Exception as e:
                code = product.get("code", "unknown")
                print(f"[mro3] 상품 파싱 오류 (code={code}): {e}")

        return items

    def _normalize(self, product: ET.Element) -> dict:
        # 상품번호
        code = product.get("code")
        if not code:
            return None

        # 상태
        status_el = product.find("status")
        runout = status_el.get("runout", "0") if status_el is not None else "0"
        status = STATUS_MAP.get(runout, "active")

        # 가격
        price_el = product.find("price")
        price = self._parse_price(price_el.get("buyprice") if price_el is not None else None)
        consumer_price = self._parse_price(price_el.get("consumerprice") if price_el is not None else None)
        tax_mode = price_el.get("taxmode") if price_el is not None else None

        # 이미지
        listimg_el = product.find("listimg")
        image_url = None
        if listimg_el is not None:
            url = (listimg_el.get("url") or "").strip()
            if url:
                image_url = url

        # 카테고리명
        category_name = self._cdata_text(product, "mrocatenm")

        # 상품명
        product_name = self._cdata_text(product, "prdtname")

        # 상품 상세 URL
        detail_url = PRODUCT_URL.format(code=code)

        # baseinfo: 원산지, 제조사, 브랜드, 모델명
        origin = None
        productcom = None
        brand = None
        model = None
        baseinfo_el = product.find("baseinfo")
        if baseinfo_el is not None:
            origin = baseinfo_el.get("madein") or None
            productcom = baseinfo_el.get("productcom") or None
            brand = baseinfo_el.get("brand") or None
            model = baseinfo_el.get("model") or None

        # 상세설명
        content_el = product.find("content")
        detail_description = ""
        if content_el is not None and content_el.text:
            detail_description = content_el.text.strip()

        # 키워드
        keywords = []
        for i in range(1, 6):
            kw_el = product.find(f"keyword{i}")
            if kw_el is not None and kw_el.text:
                kw = kw_el.text.strip()
                if kw:
                    keywords.append(kw)

        # 옵션: option1price는 절대가격 → 차액 = option_price - buyprice
        options_text, option_prices_text = self._parse_options(product, price)

        return {
            "source_product_code": code,
            "product_name": product_name,
            "price": price,
            "supply_price": None,
            "status": status,
            "image_url": image_url,
            "detail_url": detail_url,
            "stock_qty": None,
            "category_name": category_name,
            "origin": origin,
            "own_code": None,
            "detail_description": detail_description,
            "shipping_fee": None,
            "shipping_condition": None,
            "extra": {
                "소비자가": consumer_price,
                "과세여부": tax_mode,
                "제조사": productcom,
                "브랜드": brand,
                "모델명": model,
                "키워드": " / ".join(keywords) if keywords else None,
                "옵션": options_text,
                "옵션가": option_prices_text,
            },
        }

    def _parse_options(self, product: ET.Element, base_price: int):
        option_names = []
        option_diffs = []
        i = 1
        while True:
            opt_name_el = product.find(f"option{i}")
            if opt_name_el is None:
                break
            name = (opt_name_el.text or "").strip()
            if not name:
                break
            opt_price_el = product.find(f"option{i}price")
            opt_price = self._parse_price(opt_price_el.text if opt_price_el is not None else None)

            if base_price is not None and opt_price is not None:
                diff = opt_price - base_price
                diff_str = f"+{diff}" if diff > 0 else str(diff)
            else:
                diff_str = "0"

            option_names.append(name)
            option_diffs.append(diff_str)
            i += 1

        if not option_names:
            return None, None

        return "\n".join(option_names), "\n".join(option_diffs)

    def _cdata_text(self, element: ET.Element, tag: str) -> str:
        node = element.find(tag)
        if node is None:
            return None
        text = node.text
        if text is None:
            return None
        return str(text).strip() or None

    def _parse_price(self, text) -> int:
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        return int(cleaned) if cleaned else None

    def _error(self, msg: str) -> dict:
        return {
            "success": False,
            "total_items": 0,
            "total_pages": 0,
            "success_count": 0,
            "fail_count": 1,
            "error_summary": msg,
            "items": [],
        }
