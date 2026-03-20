import os
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from app.collectors.base import BaseCollector

BASE_URL = "https://www.metaldiy.com"
MAIN_URL = "https://www.metaldiy.com/main/mainView.do"

STATUS_MAP = {
    "2": "discontinued",   # 판매불가
    "3": "discontinued",   # 단종
    "4": "out_of_stock",   # 일시품절
    "5": "out_of_stock",   # 장기품절
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
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                page.set_default_timeout(30000)

                # 1. 메인 페이지 이동
                page.goto(MAIN_URL)
                page.wait_for_load_state("networkidle")
                time.sleep(1)

                # 2. 로그인 팝업 창 열기 (새 창으로 열림)
                with context.expect_page() as popup_info:
                    page.evaluate("fnLoginPopup()")
                popup_page = popup_info.value
                popup_page.wait_for_load_state("networkidle")
                time.sleep(1)
                print("[metaldiy] 로그인 팝업 창 열림")

                # 3. 로그인 폼 입력
                popup_page.fill("input[name='loginId']", login_id)
                popup_page.fill("input[name='loginPw']", login_pw)
                popup_page.click("input[type='image']")
                time.sleep(2)

                # 4. 팝업 닫힘 후 부모 페이지 로그인 상태 확인
                page.wait_for_load_state("networkidle")
                time.sleep(1)
                login_yn = page.evaluate("() => (typeof loginUser !== 'undefined' ? loginUser.loginYn : 'N')")
                if login_yn != "Y":
                    browser.close()
                    return self._error("로그인 실패 - 계정 정보 확인 필요")

                print("[metaldiy] 로그인 성공 (기업회원)")

                # 5. 카테고리 목록 추출
                categories = self._get_categories(page)
                if not categories:
                    browser.close()
                    return self._error("카테고리 목록 추출 실패")

                print(f"[metaldiy] 카테고리 수: {len(categories)}")

                # 6. 카테고리별 상품 수집
                for cat_id, cat_name in categories:
                    try:
                        cat_items = self._collect_category(page, cat_id, cat_name)
                        items.extend(cat_items)
                        print(f"[metaldiy] [{cat_name}] {len(cat_items)}개 수집")
                    except Exception as e:
                        print(f"[metaldiy] 카테고리 오류 ({cat_name}/{cat_id}): {e}")

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
            print(f"[metaldiy] 오류 발생: {e}")
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

    def _get_categories(self, page) -> list:
        """네비게이션에서 fnGoCate(lvl, cateId) 1단계 카테고리만 추출"""
        return page.evaluate(r"""
            () => {
                const seen = new Set();
                const cats = [];
                document.querySelectorAll('[onclick*="fnGoCate"]').forEach(el => {
                    const raw = el.getAttribute('onclick') || '';
                    // fnGoCate('1', '31900001889126') 형태
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
                })

            if len(products) < 40:
                break
            page_num += 1

        return items

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
