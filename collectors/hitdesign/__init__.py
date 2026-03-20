import os
import re
import time
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
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

        # 1차: requests
        try:
            return self._run_requests(login_id, login_pw)
        except Exception as e:
            print(f"[hitdesign] requests 실패 → Playwright 전환: {e}")

        # 2차: Playwright
        try:
            return self._run_playwright(login_id, login_pw)
        except Exception as e:
            print(f"[hitdesign] Playwright 실패: {e}")
            return self._error(f"모든 수집 방식 실패: {str(e)[:200]}")

    # ──────────────────────────────────────────────
    # requests 방식
    # ──────────────────────────────────────────────

    def _run_requests(self, login_id: str, login_pw: str) -> dict:
        session = requests.Session()
        session.headers.update(HEADERS)

        self._login_requests(session, login_id, login_pw)
        print("[hitdesign] 로그인 성공 (requests)")

        categories = self._get_categories(session)
        print(f"[hitdesign] 카테고리 수: {len(categories)}")

        items = []
        for cate_no, cate_name in categories:
            try:
                cat_items = self._collect_category_requests(session, cate_no, cate_name)
                if cat_items:
                    items.extend(cat_items)
                    print(f"[hitdesign] [{cate_name}] {len(cat_items)}개")
            except Exception as e:
                print(f"[hitdesign] 카테고리 오류 ({cate_name}/{cate_no}): {e}")

        return {
            "success": True,
            "total_items": len(items),
            "total_pages": len(categories),
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _login_requests(self, session: requests.Session, login_id: str, login_pw: str):
        resp = self._get_with_retry(session, LOGIN_URL)
        soup = BeautifulSoup(resp.text, "html.parser")

        # hidden field 전체 추출
        form = soup.find("form", {"id": re.compile(r"member_form_")})
        payload = {}
        if form:
            for inp in form.find_all("input"):
                name = inp.get("name")
                val = inp.get("value", "")
                if name:
                    payload[name] = val

        payload["member_id"] = login_id
        payload["member_passwd"] = login_pw
        payload["use_login_keeping"] = "T"

        resp = self._post_with_retry(
            session, LOGIN_ACTION, data=payload, allow_redirects=True
        )

        if "로그아웃" not in resp.text and "logout" not in resp.text.lower():
            raise Exception("로그인 실패 - 계정 정보 확인 필요")

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

    def _collect_category_requests(
        self, session: requests.Session, cate_no: str, cate_name: str
    ) -> list:
        items = []
        page_num = 1

        while True:
            url = f"{BASE_URL}/product/list.html?cate_no={cate_no}&page={page_num}"
            resp = self._get_with_retry(session, url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # 상품 링크 추출 (/product/{slug}/{product_no}/category/ 패턴)
            product_links = soup.select("a[href*='/product/'][href*='/category/']")
            # 중복 제거 (같은 상품에 이미지 링크 + 텍스트 링크 2개씩 있을 수 있음)
            seen_no = set()
            products = []
            for link in product_links:
                href = link.get("href", "")
                m = re.search(r"/product/[^/]+/(\d+)/", href)
                if not m:
                    continue
                product_no = m.group(1)
                if product_no in seen_no:
                    continue
                seen_no.add(product_no)
                products.append((product_no, link))

            if not products:
                break

            for product_no, link in products:
                try:
                    href = link.get("href", "")
                    li = link.find_parent("li")

                    # 상품명: strong 태그 우선
                    name_el = link.find("strong") or (
                        li.find("strong") if li else None
                    )
                    name = name_el.get_text(strip=True) if name_el else ""

                    # 가격
                    price_el = li.select_one(".price, .xans-product-price") if li else None
                    price = self._parse_price(price_el.get_text() if price_el else "")

                    # 이미지
                    img_el = li.find("img") if li else link.find("img")
                    img = img_el.get("src", "") if img_el else ""
                    if img.startswith("//"):
                        img = "https:" + img

                    if href.startswith("/"):
                        href = BASE_URL + href

                    items.append({
                        "source_product_code": product_no,
                        "product_name": name,
                        "price": price,
                        "supply_price": None,
                        "status": "active",
                        "image_url": img or None,
                        "detail_url": href,
                        "stock_qty": None,
                        "category_name": cate_name,
                    })
                except Exception as e:
                    print(f"[hitdesign] 상품 파싱 오류: {e}")

            time.sleep(0.8)
            page_num += 1

        return items

    # ──────────────────────────────────────────────
    # Playwright fallback
    # ──────────────────────────────────────────────

    def _run_playwright(self, login_id: str, login_pw: str) -> dict:
        items = []
        categories = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = context.new_page()
            page.set_default_timeout(30000)

            page.goto(LOGIN_URL, wait_until="domcontentloaded")
            page.fill("input[name='member_id']", login_id)
            page.fill("input[name='member_passwd']", login_pw)
            page.click("a[onclick*='MemberAction.login']")
            page.wait_for_load_state("domcontentloaded")
            time.sleep(1)

            if "로그아웃" not in page.content():
                browser.close()
                raise Exception("Playwright 로그인 실패")

            print("[hitdesign] 로그인 성공 (Playwright)")

            page.goto(f"{BASE_URL}/index.html", wait_until="domcontentloaded")
            categories = page.evaluate(r"""
                () => {
                    const seen = new Set();
                    const cats = [];
                    document.querySelectorAll('a[href*="cate_no="]').forEach(el => {
                        const m = el.href.match(/cate_no=(\d+)/);
                        const name = el.textContent.trim().replace(/\s+/g, ' ');
                        if (m && !seen.has(m[1]) && name) {
                            seen.add(m[1]);
                            cats.push([m[1], name]);
                        }
                    });
                    return cats;
                }
            """)

            for cate_no, cate_name in categories:
                try:
                    cat_items = self._collect_category_playwright(page, cate_no, cate_name)
                    if cat_items:
                        items.extend(cat_items)
                        print(f"[hitdesign][PW] [{cate_name}] {len(cat_items)}개")
                except Exception as e:
                    print(f"[hitdesign][PW] 카테고리 오류 ({cate_name}): {e}")

            browser.close()

        return {
            "success": True,
            "total_items": len(items),
            "total_pages": len(categories),
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _collect_category_playwright(self, page, cate_no: str, cate_name: str) -> list:
        items = []
        page_num = 1

        while True:
            url = f"{BASE_URL}/product/list.html?cate_no={cate_no}&page={page_num}"
            page.goto(url, wait_until="domcontentloaded")
            time.sleep(0.5)

            products = page.evaluate(r"""
                () => {
                    const seen = new Set();
                    const results = [];
                    document.querySelectorAll('a[href*="/product/"][href*="/category/"]').forEach(link => {
                        const m = link.href.match(/\/product\/[^\/]+\/(\d+)\//);
                        if (!m || seen.has(m[1])) return;
                        seen.add(m[1]);

                        const li = link.closest('li');
                        const nameEl = link.querySelector('strong') || (li && li.querySelector('strong'));
                        const priceEl = li && li.querySelector('.price, .xans-product-price');
                        const imgEl = li && li.querySelector('img');

                        results.push({
                            product_no: m[1],
                            name: nameEl ? nameEl.textContent.trim() : '',
                            price: priceEl ? priceEl.textContent.trim() : '',
                            img: imgEl ? (imgEl.src || '') : '',
                            href: link.href,
                        });
                    });
                    return results;
                }
            """)

            if not products:
                break

            for prod in products:
                img = prod.get("img", "")
                if img.startswith("//"):
                    img = "https:" + img
                items.append({
                    "source_product_code": prod["product_no"],
                    "product_name": prod.get("name", ""),
                    "price": self._parse_price(prod.get("price", "")),
                    "supply_price": None,
                    "status": "active",
                    "image_url": img or None,
                    "detail_url": prod.get("href", ""),
                    "stock_qty": None,
                    "category_name": cate_name,
                })

            page_num += 1

        return items

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
                    print(f"[hitdesign] {resp.status_code} 응답, {delay}초 후 재시도")
                    time.sleep(delay)
                    delay *= 2
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
        raise Exception(f"GET 실패 ({retries}회): {url}")

    def _post_with_retry(self, session: requests.Session, url: str, retries: int = 3, **kwargs) -> requests.Response:
        delay = 2
        for attempt in range(retries):
            try:
                return session.post(url, timeout=15, **kwargs)
            except requests.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
        raise Exception(f"POST 실패 ({retries}회): {url}")

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
