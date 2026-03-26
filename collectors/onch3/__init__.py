import logging
logger = logging.getLogger(__name__)
import os
import re
import time

import requests
from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector

BASE_URL = "https://www.onch3.co.kr"
LOGIN_URL = BASE_URL + "/login/login_web.php"
LIST_URL = BASE_URL + "/mypage/sale_products.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": BASE_URL,
}

CODE_PATTERN = re.compile(r'CH\d+')


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

            # td[3]: 가격 (첫 번째 div, "(0)" 제외)
            td_price = tds[3]
            price = None
            for div in td_price.find_all("div"):
                t = div.get_text(strip=True)
                m = re.match(r'^[\d,]+$', t)
                if m:
                    p = self._parse_price(t)
                    if p and 100 <= p <= 50_000_000:
                        price = p
                        break

            # td[4]: 상태
            status_text = tds[4].get_text(strip=True)
            if status_text == "정상판매":
                status = "active"
            else:
                status = "out_of_stock"

            items.append({
                "source_product_code": source_code,
                "product_name": product_name,
                "price": price,
                "supply_price": None,
                "status": status,
                "image_url": img_url,
                "detail_url": "",
                "stock_qty": None,
                "category_name": category or None,
            })

        return items

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        # 다음 페이지 링크 탐색
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
