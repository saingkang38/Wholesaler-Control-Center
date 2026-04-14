import logging
logger = logging.getLogger(__name__)
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector

BASE_URL = "https://www.onch3.co.kr"
LOGIN_URL = BASE_URL + "/login/login_web.php"
LIST_URL = BASE_URL + "/mypage/sale_products.php"
DETAIL_URL_TEMPLATE = BASE_URL + "/dbcenter_renewal/dbcenter_view.html?num={}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": BASE_URL,
}

CODE_PATTERN = re.compile(r'CH\d+')
STOCK_PATTERN = re.compile(r'^\((\d+)\)$')


class Onch3Collector(BaseCollector):
    wholesaler_code = "onch3"

    def run(self) -> dict:
        session = requests.Session()
        login_id = os.getenv("ONCH3_LOGIN_ID")
        login_pw = os.getenv("ONCH3_LOGIN_PASSWORD")
        if not login_id or not login_pw:
            return self._error("ONCH3_LOGIN_ID / ONCH3_LOGIN_PASSWORD 환경변수 없음")

        # 1. 로그인
        try:
            session.get(BASE_URL, headers=HEADERS, timeout=15)
            resp = session.post(
                LOGIN_URL,
                data={
                    "referer_url": BASE_URL,
                    "username": login_id.strip(),
                    "password": login_pw.strip(),
                    "login": "로그인",
                },
                headers={
                    **HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": BASE_URL,
                },
                timeout=15,
                allow_redirects=True,
            )
            if "로그아웃" not in resp.text:
                return self._error(f"로그인 실패 (status={resp.status_code})")
            logger.info("[onch3] 로그인 완료")
        except Exception as e:
            return self._error(f"로그인 오류: {e}")

        # 2. 페이지 순회
        items, seen, page = [], set(), 1
        try:
            while True:
                logger.info(f"[onch3] 페이지 {page} 수집 중...")
                resp = session.get(
                    LIST_URL,
                    params={"pageSize": 100, "page": page},
                    headers=HEADERS,
                    timeout=60,
                )
                if not resp.ok:
                    logger.warning(f"[onch3] 요청 실패: {resp.status_code}")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                batch = self._parse_page(soup, seen)

                if not batch:
                    logger.info(f"[onch3] 페이지 {page}: 상품 없음, 종료")
                    break

                items.extend(batch)
                logger.info(f"[onch3] 페이지 {page}: {len(batch)}건 (누계 {len(items)}건)")

                if not self._has_next_page(soup, page):
                    break

                page += 1
                time.sleep(1)

        except Exception as e:
            logger.warning(f"[onch3] 수집 오류: {e}")
            return {
                "success": False,
                "total_items": len(items),
                "total_pages": page,
                "success_count": len(items),
                "fail_count": 1,
                "error_summary": str(e)[:500],
                "items": items,
            }

        # 3. 상세 페이지 수집 (옵션/고시/재고)
        if items:
            self._enrich_with_details(session, items)

        logger.info(f"[onch3] 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": page,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _parse_page(self, soup: BeautifulSoup, seen: set) -> list:
        items = []
        for chk in soup.select("input.chkbox_list"):
            source_code = chk.get("value", "").strip()
            if not source_code or not CODE_PATTERN.fullmatch(source_code):
                continue
            if source_code in seen:
                continue
            seen.add(source_code)

            tr = chk.find_parent("tr")
            if not tr:
                continue

            tds = tr.find_all("td", recursive=False)
            if len(tds) < 5:
                continue

            # td[2]: 이미지, 카테고리, 상품명
            td_info = tds[2]
            img_tag = td_info.find("img")
            img_url = ""
            if img_tag:
                src = img_tag.get("src") or img_tag.get("data-src") or ""
                img_url = BASE_URL + src if src.startswith("/") else src

            category_span = td_info.find("span", class_="small")
            category = category_span.get_text(strip=True) if category_span else ""

            name_div = td_info.find("div", class_="fw-bold")
            product_name = ""
            if name_div:
                inner = name_div.find("div", class_="fw-bold")
                if inner:
                    product_name = inner.get_text(strip=True)
                else:
                    product_name = name_div.get_text(strip=True)
                if category and product_name.startswith(category):
                    product_name = product_name[len(category):].strip()

            if not product_name:
                continue

            # td[3]: 가격 + 재고수량 (N)
            td_price = tds[3]
            price = None
            stock_qty = None
            for div in td_price.find_all("div"):
                t = div.get_text(strip=True)
                # 재고 패턴: (숫자)
                m_stock = STOCK_PATTERN.match(t)
                if m_stock:
                    stock_qty = int(m_stock.group(1))
                    continue
                # 가격 패턴: 숫자,숫자 형태
                if re.match(r'^[\d,]+$', t):
                    p = self._parse_price(t)
                    if p and price is None and 100 <= p <= 50_000_000:
                        price = p

            # td[4]: 상태
            status_text = tds[4].get_text(strip=True)
            status = "active" if status_text == "정상판매" else "out_of_stock"

            # prd_num (상세 URL 구성용)
            btn = tr.find("button", class_="baljuProcBtn")
            prd_num = btn.get("data-prd_num", "").strip() if btn else ""
            detail_url = DETAIL_URL_TEMPLATE.format(prd_num) if prd_num else ""

            items.append({
                "source_product_code": source_code,
                "product_name": product_name,
                "price": price,
                "supply_price": None,
                "status": status,
                "image_url": img_url,
                "detail_url": detail_url,
                "stock_qty": stock_qty,
                "category_name": category or None,
                "_prd_num": prd_num,  # 상세 수집 후 제거
            })

        return items

    def _enrich_with_details(self, session: requests.Session, items: list):
        fetch_detail = os.getenv("ONCH3_FETCH_DETAIL", "true").lower() != "false"
        if not fetch_detail:
            for item in items:
                item.pop("_prd_num", None)
            return

        fetchable = [item for item in items if item.get("_prd_num")]
        logger.info(f"[onch3] 상세 페이지 수집 시작 ({len(fetchable)}건)")

        def _fetch(item):
            prd_num = item["_prd_num"]
            try:
                resp = session.get(
                    DETAIL_URL_TEMPLATE.format(prd_num),
                    headers=HEADERS,
                    timeout=20,
                )
                if resp.ok:
                    self._parse_detail(item, BeautifulSoup(resp.text, "html.parser"))
            except Exception as e:
                logger.debug(f"[onch3] 상세 수집 실패 {prd_num}: {e}")

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_fetch, item) for item in fetchable]
            done = 0
            for _ in as_completed(futures):
                done += 1
                if done % 200 == 0:
                    logger.info(f"[onch3] 상세 수집 진행: {done}/{len(fetchable)}")

        logger.info(f"[onch3] 상세 수집 완료")
        for item in items:
            item.pop("_prd_num", None)

    def _parse_detail(self, item: dict, soup: BeautifulSoup):
        # 상세설명 HTML (content_section 내 이미지/설명 전체)
        content_section = soup.find("div", class_="content_section")
        if content_section:
            detail_html = content_section.decode_contents().strip()
            if detail_html:
                item["detail_description"] = detail_html

        # 옵션 파싱
        # detail_page_price_3 는 절대가격 → item["price"](기준가) 대비 차액으로 변환
        opt_container = soup.find("div", class_="detail_page_option")
        if opt_container:
            li_list = opt_container.select("ul li")
            names = []
            abs_prices = []
            for li in li_list:
                name_span = li.find("span", class_="detail_page_name")
                price_span = li.find("span", class_="detail_page_price_3")
                if not name_span:
                    continue
                name = name_span.get_text(strip=True)
                abs_price = self._parse_price(price_span.get_text(strip=True) if price_span else None)
                names.append(name)
                abs_prices.append(abs_price)  # None = 가격 정보 없음

            # 단일 "상품" / "기본" 은 실제 옵션 없음
            is_real_option = len(names) > 1 or (
                len(names) == 1 and names[0] not in ("상품", "기본", "")
            )
            if is_real_option and abs_prices:
                # 기준가: item["price"] (목록 페이지 가격) 또는 최저 옵션가
                known = [p for p in abs_prices if p is not None]
                base = item.get("price") or (min(known) if known else 0)
                # 가격 정보 없는 옵션은 추가금 0으로 처리
                diffs = [str((p - base) if p is not None else 0) for p in abs_prices]
                if not item.get("extra"):
                    item["extra"] = {}
                item["extra"]["옵션"] = "\n".join(names)
                item["extra"]["옵션가"] = "\n".join(diffs)

        # 고시 정보 파싱 (원산지, 제조사, 모델명)
        gosi = soup.find("div", class_="prod_gosi")
        if gosi:
            skip_vals = {"-", "해당없음", "상세페이지참고", "상세페이지 참고", ""}
            for row in gosi.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = td.get_text(strip=True)
                if val in skip_vals:
                    continue
                if "원산지" in key:
                    item["origin"] = val
                elif "제조사" in key or "제조업체" in key:
                    item["manufacturer"] = val
                elif "모델" in key:
                    item["model_name"] = val

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        for a in soup.find_all("a"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if f"page={current_page + 1}" in href:
                return True
            if text == str(current_page + 1):
                return True
        return False

    def _parse_price(self, text: str):
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        if not cleaned:
            return None
        val = int(cleaned)
        if val > 100_000_000:
            return None
        return val

    def _parse_price_diff(self, text: str) -> int:
        if not text:
            return 0
        text = text.strip().replace(",", "")
        sign = 1
        if text.startswith("-"):
            sign = -1
            text = text[1:]
        elif text.startswith("+"):
            text = text[1:]
        digits = "".join(c for c in text if c.isdigit())
        return sign * int(digits) if digits else 0

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
