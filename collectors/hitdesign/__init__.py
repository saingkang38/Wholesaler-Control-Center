import logging
logger = logging.getLogger(__name__)
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from app.collectors.base import BaseCollector

BASE_URL = "https://b2b-hitdesign.com"
LOGIN_URL = f"{BASE_URL}/member/login.html"
LOGIN_ACTION = f"{BASE_URL}/exec/front/Member/login/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Referer": BASE_URL,
}


class HitdesignCollector(BaseCollector):
    wholesaler_code = "hitdesign"

    def run(self) -> dict:
        login_id = os.getenv("HITDESIGN_LOGIN_ID")
        login_pw = os.getenv("HITDESIGN_LOGIN_PASSWORD")

        if not login_id or not login_pw:
            return self._error("HITDESIGN_LOGIN_ID / HITDESIGN_LOGIN_PASSWORD 미설정")

        try:
            session = self._login_playwright(login_id, login_pw)
        except Exception as e:
            return self._error(f"로그인 실패: {str(e)[:200]}")

        categories = self._get_categories(session)
        logger.info(f"[hitdesign] 카테고리 수: {len(categories)}")

        items = []
        for cate_no, cate_name in categories:
            try:
                cat_items = self._collect_category(session, cate_no, cate_name)
                if cat_items:
                    items.extend(cat_items)
                    logger.info(f"[hitdesign] [{cate_name}] {len(cat_items)}개")
            except Exception as e:
                logger.warning(f"[hitdesign] 카테고리 오류 ({cate_name}/{cate_no}): {e}")

        # 상세페이지 수집 (가격, 옵션, 배송비, 원산지, 상세이미지)
        logger.info(f"[hitdesign] 목록 수집 완료: {len(items)}건, 상세페이지 수집 시작...")
        for i, item in enumerate(items):
            try:
                detail = self._fetch_detail(session, item["source_product_code"], item["detail_url"])
                item.update(detail)
                # 상세페이지 "상품코드" 테이블값이 있으면 source_product_code로 교체
                if item.get("own_code"):
                    item["source_product_code"] = item["own_code"]
            except Exception as e:
                logger.warning(f"[hitdesign] 상세 오류 (product_no={item['source_product_code']}): {e}")
            if (i + 1) % 100 == 0:
                logger.info(f"[hitdesign] 상세 수집 진행: {i + 1}/{len(items)}")
            time.sleep(0.2)

        logger.info(f"[hitdesign] 전체 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": len(categories),
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    # ──────────────────────────────────────────────
    # Playwright 로그인 → requests 세션 쿠키 전달
    # ──────────────────────────────────────────────

    def _login_playwright(self, login_id: str, login_pw: str) -> requests.Session:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = context.new_page()
            page.set_default_timeout(20000)

            page.goto(LOGIN_URL, wait_until="domcontentloaded")
            page.fill("input[name='member_id']", login_id)
            page.fill("input[name='member_passwd']", login_pw)

            # CAFE24 로그인 버튼 (onclick 기반)
            try:
                page.click("a[onclick*='login'], a[onclick*='Login']", timeout=5000)
            except Exception:
                page.click("input[type='submit'], button[type='submit']")

            page.wait_for_load_state("domcontentloaded")
            time.sleep(1.5)

            if "로그아웃" not in page.content():
                browser.close()
                raise Exception("로그인 실패 - 계정 정보 확인")

            logger.info("[hitdesign] 로그인 성공 (Playwright)")

            # Playwright 쿠키 → requests 세션으로 전달
            cookies = context.cookies()
            browser.close()

        session = requests.Session()
        session.headers.update(HEADERS)
        for c in cookies:
            session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        return session

    # ──────────────────────────────────────────────
    # 카테고리 목록
    # ──────────────────────────────────────────────

    def _get_categories(self, session: requests.Session) -> list:
        resp = self._get_with_retry(session, f"{BASE_URL}/index.html")
        soup = BeautifulSoup(resp.text, "html.parser")

        seen = set()
        categories = []
        for a in soup.find_all("a", href=re.compile(r"cate_no=\d+")):
            m = re.search(r"cate_no=(\d+)", a.get("href", ""))
            if not m:
                continue
            cate_no = m.group(1)
            if cate_no in seen:
                continue
            seen.add(cate_no)
            name = a.get_text(strip=True)
            if name:
                categories.append((cate_no, name))

        return categories

    # ──────────────────────────────────────────────
    # 카테고리별 상품 목록 수집
    # ──────────────────────────────────────────────

    def _collect_category(self, session: requests.Session, cate_no: str, cate_name: str) -> list:
        items = []
        page_num = 1

        while True:
            url = f"{BASE_URL}/product/list.html?cate_no={cate_no}&page={page_num}"
            resp = self._get_with_retry(session, url)
            soup = BeautifulSoup(resp.text, "html.parser")

            seen_no = set()
            products = []

            # 상품 링크: /product/name/product_no/category/cate_no/
            for a in soup.find_all("a", href=re.compile(r"/product/[^/]+/\d+/category/")):
                href = a.get("href", "")
                m = re.search(r"/product/[^/]+/(\d+)/category/", href)
                if not m:
                    continue
                product_no = m.group(1)
                if product_no in seen_no:
                    continue
                seen_no.add(product_no)
                products.append((product_no, a, href))

            if not products:
                break

            for product_no, link, href in products:
                try:
                    li = link.find_parent("li")

                    # 상품명: strong.name 안의 마지막 span 텍스트
                    name = ""
                    name_el = li.select_one("strong.name") if li else None
                    if name_el:
                        spans = name_el.select("a span:not(.title)")
                        if spans:
                            name = spans[-1].get_text(strip=True)
                        if not name:
                            raw = name_el.get_text(" ", strip=True)
                            name = re.sub(r"^상품명\s*:\s*", "", raw).strip()

                    # 이미지
                    img = ""
                    img_el = (li.select_one("div.prdImg img") if li else None) or link.find("img")
                    if img_el:
                        img = img_el.get("src", "")
                        if img.startswith("//"):
                            img = "https:" + img

                    if href.startswith("/"):
                        href = BASE_URL + href

                    items.append({
                        "source_product_code": product_no,
                        "product_name": name,
                        "price": None,  # 상세페이지에서 수집
                        "supply_price": None,
                        "status": "active",
                        "image_url": img or None,
                        "detail_url": href,
                        "stock_qty": None,
                        "category_name": cate_name,
                        "origin": None,
                        "own_code": None,
                        "detail_description": "",
                        "shipping_fee": None,
                        "shipping_condition": None,
                        "extra": {},
                    })
                except Exception as e:
                    logger.warning(f"[hitdesign] 상품 파싱 오류: {e}")

            time.sleep(0.5)
            page_num += 1

        return items

    # ──────────────────────────────────────────────
    # 상세페이지 수집 (가격 / 옵션 / 배송비 / 원산지 / 상세이미지)
    # ──────────────────────────────────────────────

    def _fetch_detail(self, session: requests.Session, product_no: str, detail_url: str) -> dict:
        resp = self._get_with_retry(session, detail_url)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        # 가격: meta 태그
        price = None
        price_meta = soup.select_one("meta[property='product:sale_price:amount']")
        if price_meta:
            price = self._parse_price(price_meta.get("content", ""))

        # 상품 정보 테이블
        own_code = None
        origin = None
        shipping_fee = None

        for tr in soup.select("table tbody tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = td.get_text(" ", strip=True)

            if key == "상품코드" and not own_code:
                own_code = val or None
            elif key == "원산지" and not origin:
                origin = val or None
            elif key == "배송비" and shipping_fee is None:
                fee_el = td.select_one(".delv_price_B strong")
                if fee_el:
                    shipping_fee = self._parse_price(fee_el.get_text(strip=True))
                else:
                    nums = re.findall(r"[\d,]+원", val)
                    for n in nums:
                        v = self._parse_price(n)
                        if v and v > 0:
                            shipping_fee = v
                            break

        # 카테고리: 브레드크럼
        category = None
        crumb = soup.select(".xans-product-headcategory ol li a, .path ol li a")
        if crumb:
            parts = [
                a.get_text(strip=True) for a in crumb
                if a.get_text(strip=True) and a.get_text(strip=True) not in ("홈", "home", "Home")
            ]
            category = " > ".join(parts) if parts else None

        # 상세설명 HTML (이미지 가운데 정렬)
        detail_html = ""
        detail_el = soup.select_one("#prdDetail .cont")
        if detail_el:
            for img in detail_el.select("img"):
                src = img.get("src", "")
                if src.startswith("//"):
                    img["src"] = "https:" + src
                elif src.startswith("/"):
                    img["src"] = BASE_URL + src
            inner = detail_el.decode_contents().strip()
            if inner:
                detail_html = f'<div style="text-align:center;">{inner}</div>'

        # 옵션
        options_text = None
        option_prices_text = None
        option_rows = soup.select("tbody.xans-product-option tr")
        all_option_values = []
        all_option_prices = []

        for tr in option_rows:
            sel = tr.select_one("select")
            if not sel:
                continue
            for opt in sel.select("option"):
                val = opt.get("value", "")
                text = opt.get_text(strip=True)
                if val in ("*", "**") or not text:
                    continue
                all_option_values.append(text)
                # 옵션가 파싱: 텍스트에서 (+1,000원) 또는 (-500원) 형태 추출
                price_match = re.search(r"\(([+-][\d,]+)원\)", text)
                if price_match:
                    opt_diff = self._parse_price(price_match.group(1).replace("+", "").replace("-", ""))
                    sign = "-" if "-" in price_match.group(1) else "+"
                    all_option_prices.append(f"{sign}{opt_diff}" if opt_diff else "0")
                else:
                    all_option_prices.append("0")

        if all_option_values:
            options_text = "\n".join(all_option_values)
            option_prices_text = "\n".join(all_option_prices)

        result = {
            "origin": origin,
            "own_code": own_code,
            "detail_description": detail_html,
            "shipping_fee": shipping_fee,
            "shipping_condition": None,
            "extra": {
                "옵션": options_text,
                "옵션가": option_prices_text,
            },
        }
        if price is not None:
            result["price"] = price
        if category:
            result["category_name"] = category

        # 품절 감지 (cafe24 표준 OG 태그)
        avail_meta = soup.select_one("meta[property='product:availability']")
        if avail_meta:
            avail = avail_meta.get("content", "").lower()
            result["status"] = "out_of_stock" if ("out" in avail or "sold" in avail) else "active"

        return result

    # ──────────────────────────────────────────────
    # 공통 유틸
    # ──────────────────────────────────────────────

    def _get_with_retry(self, session: requests.Session, url: str, retries: int = 3) -> requests.Response:
        delay = 2
        for attempt in range(retries):
            try:
                resp = session.get(url, timeout=15)
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (403, 429):
                    time.sleep(delay)
                    delay *= 2
            except requests.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
        raise Exception(f"GET 실패 ({retries}회): {url}")

    def _parse_price(self, text) -> int:
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        return int(cleaned) if cleaned else None

    def _error(self, msg: str) -> dict:
        return {
            "success": False,
            "total_items": 0, "total_pages": 0,
            "success_count": 0, "fail_count": 1,
            "error_summary": msg,
            "items": [],
        }
