import logging
logger = logging.getLogger(__name__)
import re
import time
import requests
from bs4 import BeautifulSoup
from app.collectors.base import BaseCollector

BASE_URL = "https://www.1001094.com"
LIST_URL = BASE_URL + "/goods/goods_list.php"
DETAIL_URL = BASE_URL + "/goods/goods_view.php"

CATEGORIES = [
    ("084", "수입차 특수공구"),
    ("036", "국산차 공구"),
    ("009", "대형트럭 공구"),
    ("038", "엔진 관련공구"),
    ("003", "하체/서스펜션 공구"),
    ("011", "수공구"),
    ("014", "판금/도장/바디"),
    ("081", "자동차 범퍼/후크핀"),
    ("080", "작기/유압관련 공구"),
    ("012", "에어공구"),
    ("001", "공구함세트/공구함"),
    ("090", "공구함 폼 제작"),
    ("093", "DEFA 배터리충전기"),
    ("089", "전기차 안전용품"),
    ("007", "배터리/테스터기/전자"),
    ("053", "타이어/휠"),
    ("087", "유리/와이퍼/도어"),
    ("064", "에어릴/전선릴"),
    ("032", "에어컨 공구"),
    ("020", "절연공구"),
    ("085", "작업등"),
    ("021", "용접 관련공구"),
    ("033", "전동/전기 공구"),
    ("013", "정비소관련장비"),
    ("088", "신상품"),
    ("086", "탭/탭복원/절삭공구"),
    ("092", "기타 소모품"),
    ("074", "재입고요청/품절"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}


class JtckoreaCollector(BaseCollector):
    wholesaler_code = "jtckorea"

    def run(self) -> dict:
        seen_goods_nos = set()
        items = []

        try:
            for cate_cd, cate_name in CATEGORIES:
                logger.info(f"[jtckorea] 카테고리 수집: {cate_name} (cateCd={cate_cd})")
                page = 1
                while True:
                    resp = requests.get(
                        LIST_URL,
                        params={"cateCd": cate_cd, "page": page, "listCnt": 40},
                        headers=HEADERS,
                        timeout=15,
                    )
                    if not resp.ok:
                        break
                    resp.encoding = "utf-8"
                    soup = BeautifulSoup(resp.text, "html.parser")
                    product_items = self._parse_list_page(soup, cate_name, seen_goods_nos)
                    if not product_items:
                        break
                    items.extend(product_items)
                    if not self._has_next_page(soup, page):
                        break
                    page += 1
                    time.sleep(0.3)

        except Exception as e:
            logger.warning(f"[jtckorea] 목록 수집 오류: {e}")
            return {
                "success": False,
                "total_items": len(items), "total_pages": 0,
                "success_count": len(items), "fail_count": 1,
                "error_summary": str(e)[:500], "items": items,
            }

        logger.info(f"[jtckorea] 목록 수집 완료: {len(items)}건, 상세페이지 수집 시작...")

        for i, item in enumerate(items):
            try:
                detail = self._fetch_detail(item["source_product_code"])
                item.update(detail)
            except Exception as e:
                logger.warning(f"[jtckorea] 상세 오류 (goodsNo={item['source_product_code']}): {e}")
            if (i + 1) % 100 == 0:
                logger.info(f"[jtckorea] 상세 수집 진행: {i + 1}/{len(items)}")
            time.sleep(0.3)

        logger.info(f"[jtckorea] 전체 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items), "total_pages": 0,
            "success_count": len(items), "fail_count": 0,
            "error_summary": None, "items": items,
        }

    def _fetch_detail(self, goods_no: str) -> dict:
        resp = requests.get(
            DETAIL_URL,
            params={"goodsNo": goods_no},
            headers=HEADERS,
            timeout=15,
        )
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        # 가격: item_info_box 안의 dl 판매가
        price = None
        info_box = soup.select_one("div.item_info_box")
        if info_box:
            for dl in info_box.select("dl"):
                for dt, dd in zip(dl.select("dt"), dl.select("dd")):
                    if dt.get_text(strip=True) == "판매가":
                        price = self._parse_price(dd.get_text(strip=True))
                        break
                if price is not None:
                    break

        # 상태: 기본 active (이 사이트는 HTML에 신뢰할 품절 지표 없음)
        status = "active"

        # dl > dt/dd 에서 원산지, 자체코드, 배송비 파싱
        origin = None
        own_code = None
        shipping_fee = None
        shipping_condition = None

        SKIP_KEYS = {"추천상품", "최근검색어", "판매가", "네이버마일리지",
                     "총 상품금액", "총 할인금액", "총 합계금액", "상품가격", "할인금액", "총 결제 예정금액"}
        for dl in soup.select("dl"):
            for dt, dd in zip(dl.select("dt"), dl.select("dd")):
                key = dt.get_text(strip=True)
                if key in SKIP_KEYS:
                    continue
                val = dd.get_text(" ", strip=True)
                if key == "원산지":
                    origin = val or None
                elif key == "자체상품코드":
                    own_code = val if val else None
                elif key == "배송비":
                    shipping_fee, shipping_condition = self._parse_shipping(val)

        # 상세설명 HTML (관련상품 제외, 가운데 정렬, 이미지 절대경로)
        detail_html = ""
        detail_box = soup.select_one("div.detail_explain_box")
        if detail_box:
            # js_recom_box 이후 내용 제거
            for el in detail_box.select("div.js_recom_box, div.recom_item_cont, div.relate_goods"):
                el.decompose()

            # 이미지 src 절대경로 변환
            for img in detail_box.select("img"):
                src = img.get("src", "")
                if src.startswith("//"):
                    img["src"] = "https:" + src
                elif src.startswith("/"):
                    img["src"] = BASE_URL + src

            inner_html = detail_box.decode_contents()
            detail_html = f'<div style="text-align:center;">{inner_html}</div>'

        result = {
            "status": status,
            "origin": origin,
            "own_code": own_code,
            "detail_description": detail_html,
            "shipping_fee": shipping_fee,
            "shipping_condition": shipping_condition,
            "extra": {},
        }
        if price is not None:
            result["price"] = price
        return result

    def _parse_shipping(self, text: str):
        """배송비 dd 텍스트에서 기본 배송비(int)와 조건 문자열 반환"""
        # 조건 텍스트 범위: "금액별배송비" ~ "배송비 계산" 사이
        condition_text = text
        if "금액별배송비" in text:
            condition_text = text.split("금액별배송비")[-1]
        if "배송비 계산" in condition_text:
            condition_text = condition_text.split("배송비 계산")[0]

        # 기본 배송비: "미만 X원" 패턴에서 X 추출 (임계금액 이하 구간의 실제 배송비)
        fee = None
        fee_match = re.search(r'미만\s*([\d,]+)원', condition_text)
        if fee_match:
            fee = self._parse_price(fee_match.group(1) + "원")
        else:
            # "미만" 패턴 없으면 첫 번째 non-zero non-threshold 금액
            amounts = [self._parse_price(n) for n in re.findall(r'[\d,]+원', condition_text)]
            non_threshold = [a for a in amounts if a and 0 < a < 50000]
            fee = non_threshold[0] if non_threshold else None

        # 조건 문자열: 각 구간 패턴 추출
        parts = re.findall(r'[\d,]+원\s*(?:이상[^이상미만]*?(?:미만|이상)\s*[\d,]+원)', condition_text)
        if parts:
            condition = " / ".join(p.strip() for p in parts[:3])
        else:
            condition = condition_text.strip()[:80]

        return fee, condition

    def _parse_list_page(self, soup: BeautifulSoup, cate_name: str, seen: set) -> list:
        items = []
        container = soup.select_one("div.item_hover_type")
        if not container:
            return items

        for li in container.select("ul > li"):
            links = li.select("a[href*='goods_view.php']")
            if not links:
                continue
            href = links[0].get("href", "")
            m = re.search(r'goodsNo=(\d+)', href)
            if not m:
                continue
            goods_no = m.group(1)
            if goods_no in seen:
                continue
            seen.add(goods_no)

            product_name = ""
            for a in links:
                strong = a.select_one("strong")
                if strong:
                    product_name = strong.get_text(strip=True)
                    break
            if not product_name:
                continue

            image_url = ""
            img_tag = links[0].select_one("img")
            if img_tag:
                image_url = img_tag.get("src") or img_tag.get("data-src") or ""
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
                elif image_url.startswith("/"):
                    image_url = BASE_URL + image_url

            items.append({
                "source_product_code": goods_no,
                "product_name": product_name,
                "price": None,
                "supply_price": None,
                "status": "active",
                "image_url": image_url,
                "detail_url": BASE_URL + "/goods/goods_view.php?goodsNo=" + goods_no,
                "stock_qty": None,
                "category_name": cate_name,
                "origin": None,
                "own_code": None,
                "detail_description": "",
                "shipping_fee": None,
                "shipping_condition": None,
                "extra": {},
            })
        return items

    def _parse_price(self, text) -> int:
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        return int(cleaned) if cleaned else None

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        pagination = soup.select_one("div.pagination")
        if not pagination:
            return False
        return pagination.select_one(f'a[href*="page={current_page + 1}"]') is not None
