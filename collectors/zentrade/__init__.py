import logging
logger = logging.getLogger(__name__)
import os
import re
import requests
from xml.etree import ElementTree as ET
from app.collectors.base import BaseCollector

API_URL = "https://www.zentrade.co.kr/shop/proc/product_api.php"

STATUS_MAP = {
    "0": "active",
    "1": "out_of_stock",
}


class ZentraldeCollector(BaseCollector):
    wholesaler_code = "zentrade"

    def run(self, mode: str = None, **kwargs) -> dict:
        """
        mode:
          - "full_all" (기본): 정상(runout=0) + 품절(runout=1) 전체 수집
          - "single": goodsno 단건 조회 (kwargs에 goodsno 필요)
          - "opendate": 신상품 오픈일 기준 (kwargs에 opendate_s, opendate_e 필요)
        """
        api_id = os.getenv("ZENTRADE_ID")
        api_key = os.getenv("ZENTRADE_API_KEY")

        if not api_id or not api_key:
            return self._error("ZENTRADE_ID / ZENTRADE_API_KEY 미설정")

        mode = mode or os.getenv("ZENTRADE_COLLECT_MODE", "full_all")
        base_params = {"id": api_id, "m_skey": api_key}

        if mode == "full_all":
            return self._run_full_all(base_params)

        params = dict(base_params)

        if mode == "single":
            goodsno = kwargs.get("goodsno")
            if not goodsno:
                return self._error("single 모드: goodsno 필요")
            params["goodsno"] = goodsno
            logger.info(f"[zentrade] 단건조회: goodsno={goodsno}")

        elif mode == "opendate":
            opendate_s = kwargs.get("opendate_s")
            opendate_e = kwargs.get("opendate_e")
            if not opendate_s or not opendate_e:
                return self._error("opendate 모드: opendate_s, opendate_e 필요")
            params["opendate_s"] = opendate_s
            params["opendate_e"] = opendate_e
            logger.info(f"[zentrade] 신상품 오픈일 수집: {opendate_s} ~ {opendate_e}")

        else:
            return self._error(f"알 수 없는 mode: {mode}")

        try:
            raw_xml = self._call_api(params)
            items = self._parse_xml(raw_xml)
        except Exception as e:
            return self._error(str(e)[:300])

        logger.info(f"[zentrade] 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": 1,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _run_full_all(self, base_params: dict) -> dict:
        all_items = []
        errors = []

        for runout_val, label in [("0", "정상"), ("1", "품절")]:
            params = {**base_params, "runout": runout_val}
            logger.info(f"[zentrade] 전체수집 - {label}상품 (runout={runout_val})")
            try:
                raw_xml = self._call_api(params)
                items = self._parse_xml(raw_xml)
                logger.info(f"[zentrade] {label}상품: {len(items)}건")
                all_items.extend(items)
            except Exception as e:
                msg = f"{label}상품 수집 실패: {str(e)[:200]}"
                logger.info(f"[zentrade] {msg}")
                errors.append(msg)

        seen = set()
        deduped = []
        for item in all_items:
            key = item["source_product_code"]
            if key not in seen:
                seen.add(key)
                deduped.append(item)

        success = len(deduped) > 0
        logger.info(f"[zentrade] 전체수집 완료: {len(deduped)}건 (오류: {len(errors)}건)")

        return {
            "success": success,
            "total_items": len(deduped),
            "total_pages": 2,
            "success_count": len(deduped),
            "fail_count": len(errors),
            "error_summary": "; ".join(errors) if errors else None,
            "items": deduped,
        }

    # ──────────────────────────────────────────────
    # API 호출 (euc-kr 응답 처리)
    # ──────────────────────────────────────────────

    def _call_api(self, params: dict) -> str:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        try:
            resp = requests.post(API_URL, data=params, headers=headers, timeout=60)
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise Exception(f"HTTP {resp.status_code}: {e}")
        except requests.RequestException as e:
            raise Exception(f"요청 오류: {e}")

        # euc-kr 디코딩 후 encoding 선언 제거 (ET가 utf-8로 파싱하게)
        try:
            text = resp.content.decode("euc-kr", errors="replace")
        except Exception:
            text = resp.text

        text = re.sub(r'<\?xml[^>]*\?>', '<?xml version="1.0" encoding="utf-8"?>', text)

        if not text.strip():
            raise Exception("응답이 비어있음")

        return text

    # ──────────────────────────────────────────────
    # XML 파싱
    # ──────────────────────────────────────────────

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
                logger.warning(f"[zentrade] 상품 파싱 오류 (code={code}): {e}")

        return items

    # ──────────────────────────────────────────────
    # 정규화
    # ──────────────────────────────────────────────

    def _normalize(self, product: ET.Element) -> dict:
        # 상품번호: <product code="..."> 속성
        code = product.get("code")
        if not code:
            return None

        # 상태: <status runout="0|1">
        status_el = product.find("status")
        runout = status_el.get("runout", "0") if status_el is not None else "0"
        status = STATUS_MAP.get(runout, "active")

        # 가격: <price buyprice="..." consumerprice="...">
        price_el = product.find("price")
        price = self._parse_price(price_el.get("buyprice") if price_el is not None else None)
        consumer_price = self._parse_price(price_el.get("consumerprice") if price_el is not None else None)

        # 이미지: <listimg url1="..." url2="..." ...>
        listimg_el = product.find("listimg")
        image_url = None
        images = []
        if listimg_el is not None:
            for i in range(1, 6):
                url = (listimg_el.get(f"url{i}") or "").strip()
                if url:
                    images.append(url)
                    if image_url is None:
                        image_url = url

        # 카테고리: <dome_category> CDATA 텍스트
        category_name = self._cdata_text(product, "dome_category")

        # 상품명: <prdtname> CDATA 텍스트
        product_name = self._cdata_text(product, "prdtname")

        # 옵션 파싱 → 표준 문자열 형식 변환
        option_el = product.find("option")
        options = self._parse_options(option_el.text if option_el is not None else None)
        if options:
            # 절대 가격 → 상품 기준가 대비 차액으로 변환. 옵션 가격이 None이면 추가금 0으로 처리
            diffs = [
                (o["price"] - price if price else o["price"]) if o["price"] is not None else 0
                for o in options
            ]
            options_text = "\n".join(o["option_name"] for o in options)
            option_diffs = "\n".join(str(d) for d in diffs)
        else:
            options_text = None
            option_diffs = None

        # XML 전체 필드 extra에 저장
        extra = {}
        extra["소비자가"] = consumer_price
        extra["옵션"] = options_text
        extra["옵션가"] = option_diffs
        extra["이미지목록"] = images
        extra["오픈일"] = status_el.get("opendate") if status_el is not None else None
        for child in product:
            tag = child.tag
            if tag in ("listimg", "option", "status", "price"):
                continue
            val = (child.text or "").strip()
            if val:
                extra[tag] = val
        # product 속성도 저장
        for attr, val in product.attrib.items():
            if attr != "code":
                extra[f"attr_{attr}"] = val

        return {
            "source_product_code": code,
            "product_name": product_name,
            "price": price,
            "supply_price": None,
            "status": status,
            "image_url": image_url,
            "detail_url": None,
            "stock_qty": None,
            "category_name": category_name,
            "origin": None,
            "shipping_fee": None,
            "shipping_condition": None,
            "detail_description": self._cdata_text(product, "detailed_source"),
            "extra": extra,
        }

    # ──────────────────────────────────────────────
    # 옵션 파싱: "옵션명^|^판매가^|^소비자가^|^이미지URL↑=↑" 반복
    # ──────────────────────────────────────────────

    def _parse_options(self, raw: str) -> list:
        if not raw or not raw.strip():
            return []

        options = []
        for chunk in raw.split("↑=↑"):
            chunk = chunk.strip()
            if not chunk:
                continue
            parts = chunk.split("^|^")
            if len(parts) >= 1 and parts[0].strip():
                options.append({
                    "option_name": parts[0].strip(),
                    "price": self._parse_price(parts[1].strip()) if len(parts) > 1 else None,
                    "consumer_price": self._parse_price(parts[2].strip()) if len(parts) > 2 else None,
                    "option_image": parts[3].strip() if len(parts) > 3 else None,
                })

        return options

    # ──────────────────────────────────────────────
    # 공통 유틸
    # ──────────────────────────────────────────────

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
