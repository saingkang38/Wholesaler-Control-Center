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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

STATUS_MAP = {
    "2": "discontinued",
    "3": "discontinued",
    "4": "out_of_stock",
    "5": "out_of_stock",
}


class MetaldiyCollector(BaseCollector):
    wholesaler_code = "metaldiy"

    def run(self) -> dict:
        login_id = os.getenv("METALDIY_LOGIN_ID")
        login_pw = os.getenv("METALDIY_LOGIN_PASSWORD")

        if not login_id or not login_pw:
            return self._error("METALDIY_LOGIN_ID / METALDIY_LOGIN_PASSWORD 미설정")

        items = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                page.set_default_timeout(30000)

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

                categories = self._get_categories(page)
                if not categories:
                    browser.close()
                    return self._error("카테고리 목록 추출 실패")

                logger.info(f"[metaldiy] 카테고리 수: {len(categories)}")

                for cat_id, cat_name in categories:
                    try:
                        cat_items = self._collect_category(page, cat_id, cat_name)
                        items.extend(cat_items)
                        logger.info(f"[metaldiy] [{cat_name}] {len(cat_items)}개 수집")
                    except Exception as e:
                        logger.warning(f"[metaldiy] 카테고리 오류 ({cat_name}/{cat_id}): {e}")

                browser.close()

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

        # 상세페이지 수집 (requests, 로그인 불필요)
        logger.info(f"[metaldiy] 목록 수집 완료: {len(items)}건, 상세페이지 수집 시작...")
        for i, item in enumerate(items):
            try:
                detail = self._fetch_detail(item["source_product_code"], item.get("price"))
                item.update(detail)
            except Exception as e:
                logger.warning(f"[metaldiy] 상세 오류 (itemId={item['source_product_code']}): {e}")
            if (i + 1) % 100 == 0:
                logger.info(f"[metaldiy] 상세 수집 진행: {i + 1}/{len(items)}")
            time.sleep(0.3)

        logger.info(f"[metaldiy] 전체 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": len(categories) if 'categories' in dir() else 0,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _fetch_detail(self, item_id: str, price: int = None) -> dict:
        url = f"{BASE_URL}/item/itemView.do?itemId={item_id}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        # 가격은 목록 수집 시 로그인 상태(할인가)로 이미 수집됨 → 여기서 덮어쓰지 않음

        # 원산지: dt[원산지] → dd
        origin = None
        for dt in soup.select("dt"):
            if dt.get_text(strip=True) == "원산지":
                dd = dt.find_next("dd")
                origin = dd.get_text(strip=True) if dd else None
                break

        # 자체코드: dt[상품코드] → dd
        own_code = None
        for dt in soup.select("dt"):
            if dt.get_text(strip=True) == "상품코드":
                dd = dt.find_next("dd")
                own_code = dd.get_text(strip=True) if dd else None
                break

        # 카테고리: path_field select selected option 텍스트
        category = self._parse_category(soup)

        # 배송비: JavaScript deliveryFee 변수
        shipping_fee = None
        shipping_condition = None
        scripts = soup.find_all("script")
        for script in scripts:
            text = script.get_text()
            m = re.search(r'deliveryFee\s*:parseFloat\([\'"]?([\d.]+)[\'"]?\)', text)
            if m:
                shipping_fee = int(float(m.group(1)))
                break

        # 무료배송조건: 추가혜택 dd에서 배송 관련 줄만
        for dt in soup.select("dt"):
            if "추가혜택" in dt.get_text(strip=True):
                dd = dt.find_next("dd")
                if dd:
                    lines = [line.strip() for line in dd.get_text("\n", strip=True).split("\n")]
                    delivery_lines = [l for l in lines if l and ("배송" in l or "무료" in l)]
                    shipping_condition = " / ".join(delivery_lines) if delivery_lines else None
                break

        # 상세설명 HTML: .goodsCon에서 tabNav, goods_related 제거 후 가운데 정렬
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

        # 옵션: tbody.optionArea tr.itemOptionTr
        options_text = None
        option_prices_text = None
        option_rows = soup.select("tbody.optionArea tr.itemOptionTr")
        if option_rows:
            option_names = []
            option_prices = []
            for tr in option_rows:
                name_el = tr.select_one("td.op_name")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name:
                    continue
                opt_price = self._parse_price(tr.get("price", ""))
                option_names.append(name)
                option_prices.append(str(opt_price) if opt_price is not None else "")
            if option_names:
                options_text = "\n".join(option_names)
                option_prices_text = "\n".join(option_prices)

        return {
            "origin": origin,
            "own_code": own_code,
            "category_name": category,
            "detail_description": detail_html,
            "shipping_fee": shipping_fee,
            "shipping_condition": shipping_condition,
            "product_url": f"{BASE_URL}/item/itemView.do?itemId={item_id}",
            "extra": {
                "옵션": options_text,
                "옵션가": option_prices_text,
            },
        }

    def _parse_category(self, soup) -> str:
        """path_field의 select 태그 selected 옵션에서 카테고리 경로 추출"""
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
                    "supply_price": None,
                    "status": STATUS_MAP.get(p.get("itemSts", ""), "active"),
                    "image_url": img or None,
                    "detail_url": f"{BASE_URL}/item/itemView.do?itemId={item_id}",
                    "stock_qty": None,
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
