import logging
logger = logging.getLogger(__name__)
import os
import re
import requests
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from app.collectors.base import BaseCollector

API_URL = "https://79dome.com/Api/ProductSelect_Api_UTF8.php"

STATUS_MAP = {
    "정상": "active",
    "품절": "out_of_stock",
    "일시품절": "out_of_stock",
    "단종": "discontinued",
    "판매중지": "discontinued",
}


class ChingudomeCollector(BaseCollector):
    wholesaler_code = "chingudome"

    def run(self, mode: str = None, **kwargs) -> dict:
        """
        mode:
          - "full_all" (기본): 정상(runout=0) + 품절(runout=1) 전체 수집 (API 2회)
          - "incremental": 어제 기준 modidate 증분수집 (API 1회)
          - "single": goodsno 단건 조회 (kwargs에 goodsno 필요, API 1회)
          - "daterange": 직접 날짜 지정 (kwargs에 modidate_s, modidate_e 필요, API 1회)
        일일 API 요청 5회 제한 주의.
        """
        api_id = os.getenv("CHINGUDOME_ID")
        api_key = os.getenv("CHINGUDOME_API_KEY")

        if not api_id or not api_key:
            return self._error("CHINGUDOME_ID / CHINGUDOME_API_KEY 미설정")

        mode = mode or os.getenv("CHINGUDOME_COLLECT_MODE", "full_all")
        base_params = {"id": api_id, "apiKey": api_key}

        # ── full_all: 정상 + 품절 합산 (API 2회) ──
        if mode == "full_all":
            return self._run_full_all(base_params)

        # ── 단일 호출 모드 ──
        params = dict(base_params)

        if mode == "incremental":
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            params["modidate_s"] = kwargs.get("modidate_s", yesterday)
            params["modidate_e"] = kwargs.get("modidate_e", yesterday)
            logger.info(f"[chingudome] 증분수집: {params['modidate_s']} ~ {params['modidate_e']}")

        elif mode == "single":
            goodsno = kwargs.get("goodsno")
            if not goodsno:
                return self._error("single 모드: goodsno 필요")
            params["goodsno"] = goodsno
            logger.info(f"[chingudome] 단건조회: goodsno={goodsno}")

        elif mode == "daterange":
            modidate_s = kwargs.get("modidate_s")
            modidate_e = kwargs.get("modidate_e")
            if not modidate_s or not modidate_e:
                return self._error("daterange 모드: modidate_s, modidate_e 필요")
            params["modidate_s"] = modidate_s
            params["modidate_e"] = modidate_e
            logger.info(f"[chingudome] 날짜범위 수집: {modidate_s} ~ {modidate_e}")

        else:
            return self._error(f"알 수 없는 mode: {mode}")

        try:
            raw_xml = self._call_api(params)
            items = self._parse_xml(raw_xml)
        except Exception as e:
            return self._error(str(e)[:300])

        logger.info(f"[chingudome] 수집 완료: {len(items)}건")
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
        """정상상품(runout=0) + 품절상품(runout=1) 순서로 수집 후 합산. API 2회 사용."""
        all_items = []
        errors = []

        for runout_val, label in [("0", "정상"), ("1", "품절")]:
            params = {**base_params, "runout": runout_val}
            logger.info(f"[chingudome] 전체수집 - {label}상품 (runout={runout_val})")
            try:
                raw_xml = self._call_api(params)
                items = self._parse_xml(raw_xml)
                logger.info(f"[chingudome] {label}상품: {len(items)}건")
                all_items.extend(items)
            except Exception as e:
                msg = f"{label}상품 수집 실패: {str(e)[:200]}"
                logger.info(f"[chingudome] {msg}")
                errors.append(msg)

        # goodsno 기준 중복 제거
        seen = set()
        deduped = []
        for item in all_items:
            key = item["source_product_code"]
            if key not in seen:
                seen.add(key)
                deduped.append(item)

        success = len(deduped) > 0
        logger.info(f"[chingudome] 전체수집 완료: {len(deduped)}건 (오류: {len(errors)}건)")

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
    # API 호출
    # ──────────────────────────────────────────────

    def _call_api(self, params: dict) -> str:
        try:
            resp = requests.post(API_URL, data=params, timeout=30)
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise Exception(f"HTTP {resp.status_code}: {e}")
        except requests.RequestException as e:
            raise Exception(f"요청 오류: {e}")

        # 응답이 비어있으면 오류
        if not resp.text.strip():
            raise Exception("응답이 비어있음 (일일 5회 제한 초과 가능)")

        return resp.text

    # ──────────────────────────────────────────────
    # XML 파싱
    # ──────────────────────────────────────────────

    def _parse_xml(self, raw_xml: str) -> list:
        try:
            root = ET.fromstring(raw_xml.encode("utf-8"))
        except ET.ParseError:
            # CDATA 등 파싱 실패 시 이스케이프 후 재시도
            sanitized = self._sanitize_xml(raw_xml)
            root = ET.fromstring(sanitized.encode("utf-8"))

        items = []
        for product in root.findall("product"):
            try:
                item = self._normalize(product)
                if item:
                    items.append(item)
            except Exception as e:
                goodsno = product.findtext("goodsno", "unknown")
                logger.warning(f"[chingudome] 상품 파싱 오류 (goodsno={goodsno}): {e}")

        return items

    def _sanitize_xml(self, xml_str: str) -> str:
        """CDATA 섹션을 텍스트로 변환"""
        return re.sub(r"<!\[CDATA\[(.*?)\]\]>", lambda m: m.group(1), xml_str, flags=re.DOTALL)

    # ──────────────────────────────────────────────
    # 상품 정규화
    # ──────────────────────────────────────────────

    def _normalize(self, product: ET.Element) -> dict:
        # goodsno는 child element가 아닌 XML attribute
        goodsno = product.get("goodsno") or self._text(product, "goodsno")
        if not goodsno:
            return None

        raw_status = self._text(product, "status") or ""
        status = STATUS_MAP.get(raw_status, "active")

        # 이미지: img_l 하위에 img_1~img_5가 중첩된 구조
        image_url = self._text_nested(product, "img_l", "img_1") or None

        # 옵션 파싱
        options = self._parse_options(self._text(product, "options"))

        # XML 전체 필드 extra에 저장
        extra = {}
        extra["옵션"] = options

        # 추가이미지 표준 키로 저장 (추가이미지1~5)
        imgs = self._collect_images(product)
        for i, url in enumerate(imgs[1:], start=2):  # img_1은 image_url로, img_2~5는 추가이미지
            extra[f"추가이미지{i - 1}"] = url

        # XML 태그 매핑 (브랜드/제조사/모델명/키워드)
        _TAG_MAP = {
            "brand": "브랜드", "brandnm": "브랜드", "goodsbrand": "브랜드",
            "maker": "제조사", "makernm": "제조사", "goodsmaker": "제조사", "goodsmakernm": "제조사",
            "modelnm": "모델명", "model": "모델명",
            "keyword": "키워드", "keywords": "키워드",
        }
        for child in product:
            tag = child.tag
            if tag in ("img_l",):
                continue
            val = (child.text or "").strip()
            if val:
                std_key = _TAG_MAP.get(tag.lower())
                extra[std_key if std_key else tag] = val
        for attr, val in product.attrib.items():
            if attr != "goodsno":
                extra[f"attr_{attr}"] = val

        return {
            "source_product_code": goodsno,
            "product_name": self._text(product, "goodsnm"),
            "price": self._parse_price(self._text(product, "goods_price")),
            "supply_price": None,
            "status": status,
            "image_url": image_url,
            "detail_url": None,
            "stock_qty": None,
            "category_name": self._text(product, "category"),
            "origin": None,
            "shipping_fee": None,
            "shipping_condition": None,
            "extra": extra,
        }

    # ──────────────────────────────────────────────
    # 옵션 파싱: "옵션명^|^가격||" 반복 구조
    # ──────────────────────────────────────────────

    def _parse_options(self, raw: str) -> list:
        if not raw or not raw.strip():
            return []

        options = []
        for chunk in raw.split("||"):
            chunk = chunk.strip()
            if not chunk:
                continue
            parts = chunk.split("^|^")
            if len(parts) >= 2:
                name = parts[0].strip()
                price = self._parse_price(parts[1].strip())
                if name:
                    options.append({"option_name": name, "price": price})
            elif len(parts) == 1 and parts[0].strip():
                # 가격 없는 옵션
                options.append({"option_name": parts[0].strip(), "price": None})

        return options

    # ──────────────────────────────────────────────
    # 이미지 목록 수집: img_1 ~ img_5
    # ──────────────────────────────────────────────

    def _collect_images(self, product: ET.Element) -> list:
        images = []
        for i in range(1, 6):
            url = self._text_nested(product, "img_l", f"img_{i}")
            if url:
                images.append(url)
        return images

    # ──────────────────────────────────────────────
    # 공통 유틸
    # ──────────────────────────────────────────────

    def _text(self, element: ET.Element, tag: str) -> str:
        node = element.find(tag)
        if node is None:
            return None
        text = node.text
        if text is None:
            return None
        return str(text).strip() or None

    def _text_nested(self, element: ET.Element, parent_tag: str, child_tag: str) -> str:
        parent = element.find(parent_tag)
        if parent is None:
            return None
        child = parent.find(child_tag)
        if child is None:
            return None
        text = child.text
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
