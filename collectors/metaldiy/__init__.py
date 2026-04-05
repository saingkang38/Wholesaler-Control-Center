import logging
logger = logging.getLogger(__name__)
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from app.collectors.base import BaseCollector

BASE_URL = "https://www.metaldiy.com"
MAIN_URL = "https://www.metaldiy.com/main/mainView.do"

STATUS_MAP = {
    "2": "discontinued",
    "3": "discontinued",
    "4": "out_of_stock",
    "5": "out_of_stock",
}


class MetaldiyCollector(BaseCollector):
    wholesaler_code = "metaldiy"

    def run(self, test_mode: bool = False) -> dict:
        login_id = os.getenv("METALDIY_LOGIN_ID")
        login_pw = os.getenv("METALDIY_LOGIN_PASSWORD")

        if not login_id or not login_pw:
            return self._error("METALDIY_LOGIN_ID / METALDIY_LOGIN_PASSWORD 미설정")

        items = []
        categories = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                page.set_default_timeout(30000)

                # 1. 로그인
                page.goto(MAIN_URL)
                page.wait_for_load_state("networkidle")
                time.sleep(1)

                with context.expect_page() as popup_info:
                    page.evaluate("fnLoginPopup()")
                popup_page = popup_info.value
                popup_page.wait_for_load_state("networkidle")
                time.sleep(1)
                logger.info("[metaldiy] 로그인 팝업 창 열림")

                popup_page.fill("input[name='loginId']", login_id)
                popup_page.fill("input[name='loginPw']", login_pw)
                popup_page.click("input[type='image']")
                time.sleep(2)

                page.wait_for_load_state("networkidle")
                time.sleep(1)
                login_yn = page.evaluate("() => (typeof loginUser !== 'undefined' ? loginUser.loginYn : 'N')")
                if login_yn != "Y":
                    browser.close()
                    return self._error("로그인 실패 - 계정 정보 확인 필요")

                logger.info("[metaldiy] 로그인 성공 (기업회원)")

                # 2. 카테고리 목록 수집
                categories = self._get_categories(page)
                if not categories:
                    browser.close()
                    return self._error("카테고리 목록 추출 실패")

                logger.info(f"[metaldiy] 카테고리 수: {len(categories)}")

                # 3. 상품 목록 수집
                for cat_id, cat_name in categories:
                    try:
                        cat_items = self._collect_category(page, cat_id, cat_name)
                        items.extend(cat_items)
                        logger.info(f"[metaldiy] [{cat_name}] {len(cat_items)}개 수집")
                    except Exception as e:
                        logger.warning(f"[metaldiy] 카테고리 오류 ({cat_name}/{cat_id}): {e}")

                logger.info(f"[metaldiy] 목록 수집 완료: {len(items)}건, 상세페이지 수집 시작...")

                # 4. Playwright 쿠키 → requests 세션으로 전달
                playwright_cookies = context.cookies()
                session = requests.Session()
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
                })
                for c in playwright_cookies:
                    session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

                browser.close()

            # 5. 상세페이지 수집 (requests + 로그인 쿠키)
            if test_mode:
                logger.info("[metaldiy] 테스트 모드 — 옵션 2개 상품 최대 5개 수집")
                result_items = []
                for item in items:
                    if len(result_items) >= 5:
                        break
                    try:
                        detail = self._fetch_detail(session, item["source_product_code"], item.get("price"))
                        item.update(detail)
                        opt_lines = (item.get("extra") or {}).get("옵션") or ""
                        opt_count = len([l for l in opt_lines.split("\n") if l.strip()]) if opt_lines else 0
                        if opt_count == 2:
                            result_items.append(item)
                            logger.info(f"[metaldiy] 테스트 상품: {item['source_product_code']} (옵션 {opt_count}개)")
                    except Exception as e:
                        logger.warning(f"[metaldiy] 상세 오류 ({item['source_product_code']}): {e}")
                    time.sleep(0.3)
                items = result_items
                logger.info(f"[metaldiy] 테스트 수집 완료: {len(items)}건")
            else:
                for i, item in enumerate(items):
                    try:
                        detail = self._fetch_detail(session, item["source_product_code"], item.get("price"))
                        item.update(detail)
                    except Exception as e:
                        logger.warning(f"[metaldiy] 상세 오류 ({item['source_product_code']}): {e}")
                    if (i + 1) % 100 == 0:
                        logger.info(f"[metaldiy] 상세 수집 진행: {i + 1}/{len(items)}")
                    time.sleep(0.3)
                logger.info(f"[metaldiy] 전체 수집 완료: {len(items)}건")

        except PlaywrightTimeout as e:
            return {
                "success": False,
                "total_items": len(items), "total_pages": 0,
                "success_count": len(items), "fail_count": 1,
                "error_summary": f"타임아웃: {str(e)[:200]}",
                "items": items,
            }
        except Exception as e:
            logger.warning(f"[metaldiy] 오류 발생: {e}")
            return {
                "success": False,
                "total_items": len(items), "total_pages": 0,
                "success_count": len(items), "fail_count": 1,
                "error_summary": str(e)[:500],
                "items": items,
            }

        return {
            "success": True,
            "total_items": len(items),
            "total_pages": len(categories),
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _fetch_detail(self, session: requests.Session, item_id: str, price: int = None) -> dict:
        """로그인 쿠키가 담긴 requests 세션으로 상세페이지 수집"""
        url = f"{BASE_URL}/item/itemView.do?itemId={item_id}"
        resp = session.get(url, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")

        # 원산지
        origin = None
        for dt in soup.select("dt"):
            if dt.get_text(strip=True) == "원산지":
                dd = dt.find_next("dd")
                origin = dd.get_text(strip=True) if dd else None
                break

        # 자체코드
        own_code = None
        for dt in soup.select("dt"):
            if dt.get_text(strip=True) == "상품코드":
                dd = dt.find_next("dd")
                own_code = dd.get_text(strip=True) if dd else None
                break

        # 카테고리
        category = self._parse_category(soup)

        # 배송비
        shipping_fee = None
        for script in soup.find_all("script"):
            text = script.get_text()
            m = re.search(r'deliveryFee\s*:parseFloat\([\'"]?([\d.]+)[\'"]?\)', text)
            if m:
                shipping_fee = int(float(m.group(1)))
                break

        # 무료배송 조건
        shipping_condition = None
        for dt in soup.select("dt"):
            if "추가혜택" in dt.get_text(strip=True):
                dd = dt.find_next("dd")
                if dd:
                    lines = [l.strip() for l in dd.get_text("\n", strip=True).split("\n")]
                    delivery_lines = [l for l in lines if l and ("배송" in l or "무료" in l)]
                    shipping_condition = " / ".join(delivery_lines) if delivery_lines else None
                break

        # 상세설명 HTML
        detail_html = ""
        goods_con = soup.select_one(".goodsCon")
        if goods_con:
            for el in goods_con.select(".goods_related, ul.tabNav, h3, .goods_summary"):
                el.decompose()
            for img in goods_con.select("img"):
                src = img.get("src", "")
                if src.startswith("//"):
                    img["src"] = "https:" + src
                elif src.startswith("/"):
                    img["src"] = BASE_URL + src
            inner = goods_con.decode_contents().strip()
            detail_html = f'<div style="text-align:center;">{inner}</div>'

        # 옵션 (마지막 td = 할인가 기준)
        options_text = None
        option_prices_text = None
        option_rows = soup.select("tbody.optionArea tr.itemOptionTr")
        if option_rows:
            option_names = []
            option_diffs = []
            for tr in option_rows:
                name_el = tr.select_one("td.op_name")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name:
                    continue
                tds = tr.select("td")
                price_td = tds[-1] if tds else None
                opt_price = self._parse_price(price_td.get_text(strip=True)) if price_td else None
                if price is not None and opt_price is not None:
                    diff = opt_price - price
                    diff_str = str(diff) if diff != 0 else "0"
                else:
                    diff_str = ""
                option_names.append(name)
                option_diffs.append(diff_str)
            if option_names:
                options_text = "\n".join(option_names)
                option_prices_text = "\n".join(option_diffs)

        return {
            "origin": origin,
            "own_code": own_code,
            "category_name": category,
            "detail_description": detail_html,
            "shipping_fee": shipping_fee,
            "shipping_condition": shipping_condition,
            "extra": {
                "옵션": options_text,
                "옵션가": option_prices_text,
            },
        }

    def _parse_category(self, soup) -> str:
        path_field = soup.select_one("div.path_field")
        if not path_field:
            return None
        parts = []
        for sel in path_field.select("select"):
            selected = sel.select_one("option[selected]")
            if selected:
                text = selected.get_text(strip=True)
                if text:
                    parts.append(text)
        return " > ".join(parts) if parts else None

    def _get_categories(self, page) -> list:
        return page.evaluate(r"""
            () => {
                const seen = new Set();
                const cats = [];
                document.querySelectorAll('[onclick*="fnGoCate"]').forEach(el => {
                    const raw = el.getAttribute('onclick') || '';
                    const m = raw.match(/fnGoCate\s*\(\s*['"]?(\d+)['"]?\s*,\s*['"]?(\w+)['"]?\s*\)/);
                    if (m && m[1] === '1' && !seen.has(m[2])) {
                        seen.add(m[2]);
                        cats.push([m[2], el.textContent.trim()]);
                    }
                });
                return cats;
            }
        """)

    def _collect_category(self, page, cat_id: str, cat_name: str) -> list:
        items = []
        page_num = 1

        while True:
            url = f"{BASE_URL}/cate/cateItemList.do?cateId={cat_id}&rowLevel=1&currentPageNo={page_num}&pageSize=40"
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(1)

            products = page.evaluate("""
                () => {
                    const results = [];
                    document.querySelectorAll('ul.goods_content').forEach(ul => {
                        const checkbox = ul.querySelector('input[type="checkbox"][name="itemId"]');
                        if (!checkbox) return;

                        const itemId = checkbox.value;
                        const itemSts = checkbox.getAttribute('itemSts') || '1';

                        const nameEl = ul.querySelector('li.goods_md a');
                        const name = nameEl ? nameEl.textContent.trim() : '';

                        const priceEl = ul.querySelector('li.goods_bm span.price strong');
                        const price = priceEl ? priceEl.textContent.trim() : '';

                        const imgEl = ul.querySelector('li.goods_img img');
                        const img = imgEl ? imgEl.src : '';

                        results.push({ itemId, itemSts, name, price, img });
                    });
                    return results;
                }
            """)

            if not products:
                break

            for p in products:
                item_id = p.get("itemId", "")
                if not item_id:
                    continue
                img = p.get("img", "")
                if img.startswith("//"):
                    img = "https:" + img
                elif img.startswith("/"):
                    img = BASE_URL + img

                items.append({
                    "source_product_code": item_id,
                    "product_name": p.get("name", ""),
                    "price": self._parse_price(p.get("price", "")),
                    "status": STATUS_MAP.get(p.get("itemSts", ""), "active"),
                    "image_url": img or None,
                    "detail_url": f"{BASE_URL}/item/itemView.do?itemId={item_id}",
                    "category_name": cat_name,
                    "origin": None,
                    "own_code": None,
                    "detail_description": "",
                    "shipping_fee": None,
                    "shipping_condition": None,
                    "extra": {},
                })

            if len(products) < 40:
                break
            page_num += 1

        return items

    def _parse_price(self, text) -> int:
        if not text:
            return None
        try:
            return int(float(str(text).replace(",", "").strip()))
        except (ValueError, TypeError):
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
