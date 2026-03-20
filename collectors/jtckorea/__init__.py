import re
import time
import requests
from bs4 import BeautifulSoup
from app.collectors.base import BaseCollector

BASE_URL = "https://www.1001094.com"
LIST_URL = BASE_URL + "/goods/goods_list.php"

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

# 상품명에서 코드 패턴 추출 (예: JTC-1234, JTC1234, PRO230 등)
CODE_PATTERN = re.compile(r'\b([A-Z]{1,5}[-]?\d{2,6}[A-Z0-9]*)\b')


class JtckoreaCollector(BaseCollector):
    wholesaler_code = "jtckorea"

    def run(self) -> dict:
        seen_goods_nos = set()
        items = []

        try:
            for cate_cd, cate_name in CATEGORIES:
                print(f"[jtckorea] 카테고리 수집: {cate_name} (cateCd={cate_cd})")
                page = 1
                while True:
                    resp = requests.get(
                        LIST_URL,
                        params={"cateCd": cate_cd, "page": page, "listCnt": 40},
                        headers=HEADERS,
                        timeout=15,
                    )
                    if not resp.ok:
                        print(f"[jtckorea] 페이지 요청 실패: {resp.status_code}")
                        break

                    soup = BeautifulSoup(resp.text, "html.parser")
                    product_items = self._parse_list_page(soup, cate_name, seen_goods_nos)

                    if not product_items:
                        break

                    items.extend(product_items)

                    # 다음 페이지 존재 여부 확인
                    if not self._has_next_page(soup, page):
                        break

                    page += 1
                    time.sleep(0.5)

        except Exception as e:
            print(f"[jtckorea] 오류 발생: {e}")
            return {
                "success": False,
                "total_items": len(items),
                "total_pages": 0,
                "success_count": len(items),
                "fail_count": 1,
                "error_summary": str(e)[:500],
                "items": items,
            }

        print(f"[jtckorea] 수집 완료: {len(items)}건 (중복 제거 후)")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": 0,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _parse_list_page(self, soup: BeautifulSoup, cate_name: str, seen: set) -> list:
        items = []

        # 실제 HTML 구조: div.item_hover_type > ul > li
        container = soup.select_one("div.item_hover_type")
        if not container:
            return items

        for li in container.select("ul > li"):
            # 상품 링크 (goodsNo 포함) - 두 번째 a 태그에 상품명 있음
            links = li.select("a[href*='goods_view.php']")
            if not links:
                continue

            href = links[0].get("href", "")
            goods_no_match = re.search(r'goodsNo=(\d+)', href)
            if not goods_no_match:
                continue

            goods_no = goods_no_match.group(1)

            # 중복 제거
            if goods_no in seen:
                continue
            seen.add(goods_no)

            # 상품명: 두 번째 a > strong
            product_name = ""
            for a in links:
                strong = a.select_one("strong")
                if strong:
                    product_name = strong.get_text(strip=True)
                    break
            if not product_name:
                continue

            # 가격: "원" 포함된 strong 태그 찾기
            price = None
            for strong in li.select("strong"):
                text = strong.get_text(strip=True)
                if "원" in text:
                    price = self._parse_price(text)
                    break

            # 이미지: 첫 번째 a > img
            image_url = ""
            img_tag = links[0].select_one("img")
            if img_tag:
                image_url = img_tag.get("src") or img_tag.get("data-src") or ""
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
                elif image_url.startswith("/"):
                    image_url = BASE_URL + image_url

            extracted_code = self._extract_code_from_name(product_name)
            detail_url = BASE_URL + "/goods/goods_view.php?goodsNo=" + goods_no

            items.append({
                "source_product_code": goods_no,
                "product_name": product_name,
                "price": price,
                "supply_price": None,
                "status": "active",
                "image_url": image_url,
                "detail_url": detail_url,
                "stock_qty": None,
                "category_name": cate_name,
                "extracted_code": extracted_code,
            })

        return items

    def _extract_code_from_name(self, name: str) -> str:
        """상품명에서 제품코드 패턴 추출. 없으면 None."""
        match = CODE_PATTERN.search(name)
        return match.group(1) if match else None

    def _parse_price(self, text) -> int:
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        return int(cleaned) if cleaned else None

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        pagination = soup.select_one("div.pagination")
        if not pagination:
            return False
        next_link = pagination.select_one(f'a[href*="page={current_page + 1}"]')
        return next_link is not None
