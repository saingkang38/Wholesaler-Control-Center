import logging
logger = logging.getLogger(__name__)
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from app.collectors.base import BaseCollector

BASE_URL = "https://www.ds1008.com"
LOGIN_URL = f"{BASE_URL}/member/login.html"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Referer": BASE_URL,
}

# 테스트 모드: True면 상품 3개만 수집
TEST_MODE = False
TEST_LIMIT = 3


class Ds1008Collector(BaseCollector):
    wholesaler_code = "ds1008"

    def run(self) -> dict:
        login_id = os.getenv("DS1008_LOGIN_ID")
        login_pw = os.getenv("DS1008_LOGIN_PASSWORD")

        if not login_id or not login_pw:
            return self._error("DS1008_LOGIN_ID / DS1008_LOGIN_PASSWORD 미설정")

        if TEST_MODE:
            logger.info(f"[ds1008] 테스트 모드 — 상품 {TEST_LIMIT}개만 수집")

        # Playwright로 로그인 → 쿠키를 requests로 이전 (철물박사 패턴)
        try:
            session = self._login_and_get_session(login_id, login_pw)
            logger.info("[ds1008] 로그인 완료, requests 세션 전환")
        except Exception as e:
            return self._error(f"로그인 실패: {str(e)[:200]}")

        categories = self._get_categories(session)
        logger.info(f"[ds1008] 카테고리 수: {len(categories)}")

        # 목록 수집
        items = []
        for cate_no, cate_name in categories:
            try:
                cat_items = self._collect_category(session, cate_no, cate_name)
                if cat_items:
                    items.extend(cat_items)
                    logger.info(f"[ds1008] [{cate_name}] {len(cat_items)}개")
            except Exception as e:
                logger.warning(f"[ds1008] 카테고리 오류 ({cate_name}/{cate_no}): {e}")
            if TEST_MODE and len(items) >= TEST_LIMIT:
                items = items[:TEST_LIMIT]
                break

        logger.info(f"[ds1008] 목록 수집 완료: {len(items)}개, 상세페이지 수집 시작")

        # 상세페이지 수집
        for i, item in enumerate(items):
            try:
                detail = self._fetch_detail(session, item["detail_url"])
                item.update(detail)
            except Exception as e:
                logger.warning(f"[ds1008] 상세 오류 ({item['source_product_code']}): {e}")
            if (i + 1) % 100 == 0:
                logger.info(f"[ds1008] 상세 수집 진행: {i + 1}/{len(items)}")
            time.sleep(0.3)

        logger.info(f"[ds1008] 전체 수집 완료: {len(items)}건")

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
    # 로그인 → requests 세션 전환
    # ──────────────────────────────────────────────

    def _login_and_get_session(self, login_id: str, login_pw: str) -> requests.Session:
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

            if "로그아웃" not in page.content() and "logout" not in page.content().lower():
                browser.close()
                raise Exception("로그인 실패 - 계정 정보 확인 필요")

            playwright_cookies = context.cookies()
            browser.close()

        session = requests.Session()
        session.headers.update(HEADERS)
        for c in playwright_cookies:
            session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        return session

    # ──────────────────────────────────────────────
    # 카테고리 수집
    # ──────────────────────────────────────────────

    def _get_categories(self, session: requests.Session) -> list:
        resp = self._get_with_retry(session, f"{BASE_URL}/index.html")
        soup = BeautifulSoup(resp.text, "html.parser")

        seen = set()
        categories = []

        for a in soup.find_all("a", attrs={"cate": re.compile(r"cate_no=\d+")}):
            m = re.search(r"cate_no=(\d+)", a.get("cate", ""))
            if m:
                cate_no = m.group(1)
                if cate_no not in seen:
                    seen.add(cate_no)
                    name = a.get_text(strip=True)
                    if name:
                        categories.append((cate_no, name))

        if not categories:
            for a in soup.find_all("a", href=re.compile(r"cate_no=\d+")):
                m = re.search(r"cate_no=(\d+)", a.get("href", ""))
                if m:
                    cate_no = m.group(1)
                    if cate_no not in seen:
                        seen.add(cate_no)
                        name = a.get_text(strip=True)
                        if name:
                            categories.append((cate_no, name))

        return categories

    def _collect_category(self, session: requests.Session, cate_no: str, cate_name: str) -> list:
        items = []
        page_num = 1

        while True:
            url = f"{BASE_URL}/product/list.html?cate_no={cate_no}&page={page_num}"
            resp = self._get_with_retry(session, url)
            soup = BeautifulSoup(resp.text, "html.parser")

            products = soup.select("div.hb_prod_item")
            if not products:
                break

            for prod in products:
                try:
                    # href에서 product_no 추출
                    a_tag = prod.select_one("a[href*='/product/']")
                    if not a_tag:
                        continue
                    href = a_tag.get("href", "")
                    m = re.search(r"product_no=(\d+)", href)
                    if not m:
                        m = re.search(r"/product/[^/]+/(\d+)/", href)
                    if not m:
                        continue
                    product_no = m.group(1)

                    # 상세 URL (카테고리 포함한 원본 href 사용)
                    detail_url = BASE_URL + href if href.startswith("/") else href

                    # 상품명: displaynone span 제외
                    name_el = prod.select_one("p.name a")
                    name = ""
                    if name_el:
                        for hidden in name_el.select("span.displaynone, span.title"):
                            hidden.decompose()
                        name = name_el.get_text(strip=True)
                        name = re.sub(r"^\[\s*\]\s*", "", name).strip()

                    # 이미지
                    img_el = prod.select_one("img")
                    img = img_el.get("src", "") if img_el else ""
                    if img.startswith("//"):
                        img = "https:" + img

                    items.append({
                        "source_product_code": product_no,
                        "product_name": name,
                        "price": None,          # 상세페이지에서 수집
                        "supply_price": None,
                        "status": "active",
                        "image_url": img or None,
                        "detail_url": detail_url,
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
                    logger.warning(f"[ds1008] 상품 파싱 오류: {e}")

                if TEST_MODE and len(items) >= TEST_LIMIT:
                    return items

            time.sleep(0.5)

            next_link = soup.select_one(
                f".xans-product-listpage a[href*='page={page_num + 1}'], "
                f"a[href*='cate_no={cate_no}'][href*='page={page_num + 1}']"
            )
            if not next_link:
                break
            page_num += 1

        return items

    # ──────────────────────────────────────────────
    # 상세페이지 수집
    # ──────────────────────────────────────────────

    def _fetch_detail(self, session: requests.Session, detail_url: str) -> dict:
        resp = self._get_with_retry(session, detail_url)
        soup = BeautifulSoup(resp.text, "html.parser")
        return self._parse_detail(soup)

    def _parse_detail(self, soup) -> dict:
        # li.xans-record- 에서 라벨 → 값 맵 생성
        info = {}
        for li in soup.select(".xans-product-detail li.xans-record-"):
            title_el = li.select_one(".info_title, [class*=info_title]")
            cont_el = li.select_one(".info_cont, [class*=info_cont]")
            if title_el and cont_el:
                label = title_el.get_text(strip=True)
                value = cont_el.get_text(strip=True)
                if label:
                    info[label] = value

        # 가격
        price = None
        if "판매가" in info:
            price = self._parse_price(info["판매가"])

        # 자체상품코드
        own_code = info.get("자체상품코드") or None

        # 원산지
        origin = info.get("원산지") or None

        # 배송비 + 무료배송 조건: "3,300원(330,000원 이상 구매 시 무료)"
        shipping_fee = None
        shipping_condition = None
        if "배송비" in info:
            ship_text = info["배송비"]
            fee_match = re.match(r"([\d,]+)원", ship_text)
            if fee_match:
                shipping_fee = int(fee_match.group(1).replace(",", ""))
            cond_match = re.search(r"\(([^)]+)\)", ship_text)
            if cond_match:
                shipping_condition = cond_match.group(1)

        # 옵션
        options_text = None
        option_prices_text = None
        all_option_values = []
        all_option_prices = []

        for sel_el in soup.select(".xans-product-option select, select[name*=option]"):
            for opt in sel_el.find_all("option"):
                text = opt.get_text(strip=True)
                if not text or "선택해 주세요" in text:
                    continue
                all_option_values.append(text)
                price_match = re.search(r"([+-])\s*([\d,]+)\s*원", text)
                if price_match:
                    sign = 1 if price_match.group(1) == "+" else -1
                    opt_diff = int(price_match.group(2).replace(",", "")) * sign
                    all_option_prices.append(str(opt_diff))
                else:
                    all_option_prices.append("0")

        if all_option_values:
            options_text = "\n".join(all_option_values)
            option_prices_text = "\n".join(all_option_prices)

        # 상세설명 HTML
        detail_html = ""
        detail_el = soup.select_one("#tab-responsive-1 .cont, #prdDetail .cont, #prdDetail")
        if detail_el:
            for img in detail_el.select("img"):
                # lazy loading: ec-data-src → src
                real_src = img.get("ec-data-src") or img.get("src", "")
                if real_src.startswith("//"):
                    real_src = "https:" + real_src
                elif real_src.startswith("/"):
                    real_src = BASE_URL + real_src
                img["src"] = real_src
            detail_html = detail_el.decode_contents().strip()

        result = {
            "price": price,
            "origin": origin,
            "own_code": own_code,
            "detail_description": detail_html,
            "shipping_fee": shipping_fee,
            "shipping_condition": shipping_condition,
            "extra": {
                "옵션": options_text,
                "옵션가": option_prices_text,
            },
        }

        # 품절 감지 (cafe24 표준 OG 태그)
        avail_meta = soup.select_one("meta[property='product:availability']")
        if avail_meta:
            avail = avail_meta.get("content", "").lower()
            result["status"] = "out_of_stock" if ("out" in avail or "sold" in avail) else "active"

        return result

    # ──────────────────────────────────────────────
    # 테스트 엑셀 저장
    # ──────────────────────────────────────────────

    def _save_test_excel(self, items: list):
        import openpyxl
        from pathlib import Path
        from datetime import datetime

        save_dir = Path(__file__).resolve().parents[2] / "downloads" / "ds1008"
        save_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = save_dir / f"ds1008_test_{timestamp}.xlsx"

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "DS도매_테스트"
        headers = [
            "상품코드", "상품명", "판매가", "공급가", "재고수량", "상태",
            "이미지URL", "상세URL", "카테고리", "원산지", "자체코드",
            "배송비", "배송조건", "옵션", "옵션가", "상세설명",
        ]
        ws.append(headers)

        for item in items:
            extra = item.get("extra") or {}
            ws.append([
                item.get("source_product_code"),
                item.get("product_name"),
                item.get("price"),
                item.get("supply_price"),
                item.get("stock_qty"),
                item.get("status"),
                item.get("image_url"),
                item.get("detail_url"),
                item.get("category_name"),
                item.get("origin"),
                item.get("own_code"),
                item.get("shipping_fee"),
                item.get("shipping_condition"),
                extra.get("옵션"),
                extra.get("옵션가"),
                item.get("detail_description"),
            ])

        wb.save(path)
        logger.info(f"[ds1008] 테스트 엑셀 저장: {path}")

    # ──────────────────────────────────────────────
    # 공통 유틸
    # ──────────────────────────────────────────────

    def _get_with_retry(self, session: requests.Session, url: str, retries: int = 3) -> requests.Response:
        delay = 2
        for attempt in range(retries):
            try:
                resp = session.get(url, timeout=20)
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (403, 429):
                    logger.info(f"[ds1008] {resp.status_code} 응답, {delay}초 대기 후 재시도")
                    time.sleep(delay)
                    delay *= 2
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
        raise Exception(f"GET 요청 실패 ({retries}회 시도): {url}")

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
