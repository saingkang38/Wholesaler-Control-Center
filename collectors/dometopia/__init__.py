import logging
logger = logging.getLogger(__name__)
import os
import io
import time
import ssl
import openpyxl
import requests as req_lib
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from app.collectors.base import BaseCollector


class _WeakSSLAdapter(HTTPAdapter):
    """richases.shopon.biz의 약한 DH 키 + 인증서 검증 우회"""
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

LOGIN_URL = "https://dometopia.com/member/login"
SHOPON_URL = "https://richases.shopon.biz/adm/"
SHOPON_GOODS_URL = "https://richases.shopon.biz/adm/goods_list_new_hosting.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}


class DometopiaCollector(BaseCollector):
    wholesaler_code = "dometopia"

    def run(self) -> dict:
        login_id = os.getenv("DOMETOPIA_LOGIN_ID", "").strip()
        login_pw = os.getenv("DOMETOPIA_LOGIN_PASSWORD", "").strip()
        shopon_id = os.getenv("DOMETOPIA_SHOPON_ID", "").strip()
        shopon_pw = os.getenv("DOMETOPIA_SHOPON_PASSWORD", "").strip()

        if not login_id or not login_pw:
            return self._error("DOMETOPIA_LOGIN_ID / DOMETOPIA_LOGIN_PASSWORD 미설정")
        if not shopon_id or not shopon_pw:
            return self._error("DOMETOPIA_SHOPON_ID / DOMETOPIA_SHOPON_PASSWORD 미설정")

        items = []
        try:
            items = self._collect(login_id, login_pw, shopon_id, shopon_pw)
        except Exception as e:
            logger.warning(f"[dometopia] 오류: {e}")
            return {
                "success": False,
                "total_items": len(items),
                "total_pages": 0,
                "success_count": len(items),
                "fail_count": 1,
                "error_summary": str(e)[:300],
                "items": items,
            }

        logger.info(f"[dometopia] 전체 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": 0,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _collect(self, login_id, login_pw, shopon_id, shopon_pw) -> list:
        all_items = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                accept_downloads=True,
            )
            page = context.new_page()
            page.set_default_timeout(30000)

            # 1. dometopia 로그인
            logger.info("[dometopia] dometopia 로그인 중...")
            page.goto(LOGIN_URL, wait_until="domcontentloaded")
            page.fill("input#userid", login_id)
            page.fill("input#password", login_pw)
            page.click("input[type='submit']")
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            logger.info("[dometopia] dometopia 로그인 완료")

            # 2. 셀러관리자 이동
            logger.info("[dometopia] 셀러관리자 이동...")
            page.goto("https://dometopia.com/selleradmin/main/seller_doto_login", wait_until="domcontentloaded")
            time.sleep(2)
            logger.info(f"[dometopia] 셀러관리자 URL: {page.url}")

            # 3. 샵온 로그인 링크 클릭 → 새 탭으로 SHOP-ON 세션 생성
            logger.info("[dometopia] 샵온 로그인 링크 클릭 (새 탭)...")
            with context.expect_page(timeout=15000) as shopon_page_info:
                page.click("a[href*='shopon_login']")
            shopon_page = shopon_page_info.value
            # 자동 리다이렉트 기다리기
            shopon_page.wait_for_load_state("networkidle")
            time.sleep(3)
            logger.info(f"[dometopia] SHOP-ON 탭 URL: {shopon_page.url}")

            # 4. 아직 로그인 페이지면 수동 로그인
            if "shopon_login" in shopon_page.url or "login" in shopon_page.url.lower() and "shopon.biz" not in shopon_page.url:
                logger.info("[dometopia] SHOP-ON 로그인 페이지 감지, 로그인 시도...")
                try:
                    shopon_page.fill("input[name='id']", shopon_id)
                    shopon_page.fill("input[name='pw']", shopon_pw)
                    shopon_page.click("button[type='submit'], input[type='submit']")
                    shopon_page.wait_for_load_state("networkidle")
                    time.sleep(2)
                    logger.info(f"[dometopia] SHOP-ON 로그인 후 URL: {shopon_page.url}")
                except Exception as e:
                    logger.warning(f"[dometopia] SHOP-ON 로그인 오류: {e}")

            # 5. 상품연동 페이지로 이동
            logger.info("[dometopia] 상품연동 페이지 이동...")
            shopon_page.goto(SHOPON_GOODS_URL, wait_until="networkidle")
            time.sleep(3)
            logger.info(f"[dometopia] 상품연동 URL: {shopon_page.url}")

            # 이후 page 변수를 shopon_page로 교체
            page = shopon_page

            # 5. 페이지당 500개 설정 (최대값)
            try:
                page.select_option("select[name='limit'], select", label="500개(이미지생략)")
                page.wait_for_load_state("networkidle")
                time.sleep(1)
                logger.info("[dometopia] 페이지 크기 500개 설정 완료")
            except Exception:
                logger.info("[dometopia] 페이지 크기 설정 스킵")

            # 엑셀다운로드 버튼 대기
            try:
                page.wait_for_selector("text=엑셀다운로드", timeout=15000)
                logger.info("[dometopia] 엑셀다운로드 버튼 확인")
            except Exception as e:
                logger.info(f"[dometopia] 엑셀다운로드 버튼 없음: {e}")

            # 6. 전체 페이지 순회
            page_num = 1
            while True:
                logger.info(f"[dometopia] 페이지 {page_num} 수집 중...")
                try:
                    # 엑셀다운로드 클릭 → 새 팝업창 오픈
                    with context.expect_page(timeout=15000) as popup_info:
                        page.click("text=엑셀다운로드")
                    popup = popup_info.value
                    popup.wait_for_load_state("networkidle")
                    time.sleep(2)
                    logger.info(f"[dometopia] 팝업 URL: {popup.url}")

                    # 팝업에서 #solution select 로딩 대기 후 이셀러스 선택
                    popup.wait_for_selector("#solution", timeout=10000)
                    popup.select_option("#solution", label="이셀러스")
                    time.sleep(0.5)
                    logger.info("[dometopia] 이셀러스 선택 완료, 다운로드 시도...")

                    # 팝업 내부에서 브라우저 fetch()로 직접 POST → base64 반환
                    logger.info("[dometopia] 브라우저 fetch로 다운로드 시도...")
                    b64 = popup.evaluate("""
                        async () => {
                            const form = document.forms['frm'];
                            const params = new URLSearchParams(new FormData(form));
                            const resp = await fetch('/adm/shop_admin/openexcel_esellers_down.php', {
                                method: 'POST',
                                credentials: 'include',
                                headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                                body: params.toString(),
                            });
                            const ct = resp.headers.get('content-type') || '';
                            console.log('fetch status:', resp.status, 'ct:', ct);
                            const buf = await resp.arrayBuffer();
                            const bytes = new Uint8Array(buf);
                            let bin = '';
                            for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
                            return JSON.stringify({b64: btoa(bin), status: resp.status, ct: ct});
                        }
                    """)
                    import json as _json, base64 as _b64
                    result = _json.loads(b64)
                    logger.info(f"[dometopia] fetch 결과: status={result['status']}, ct={result['ct']}")
                    data = _b64.b64decode(result['b64'])
                    logger.info(f"[dometopia] 다운로드 완료: {len(data)} bytes")
                    popup.close()

                    batch = self._parse_xlsx(data)
                    all_items.extend(batch)
                    logger.info(f"[dometopia] 페이지 {page_num}: {len(batch)}건")

                    if not batch:
                        break

                except Exception as e:
                    logger.warning(f"[dometopia] 페이지 {page_num} 다운로드 오류: {e}")
                    break

                # 다음 페이지
                try:
                    next_btn = page.query_selector(f"a:has-text('{page_num + 1}')")
                    if not next_btn:
                        logger.info("[dometopia] 다음 페이지 없음, 종료")
                        break
                    next_btn.click()
                    page.wait_for_load_state("networkidle")
                    time.sleep(1)
                    page_num += 1
                except Exception:
                    break

            browser.close()

        return all_items

    def _parse_xlsx(self, content: bytes) -> list:
        try:
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            wb.close()
        except Exception as e:
            logger.warning(f"[dometopia] 엑셀 파싱 오류: {e}")
            return []

        if not rows:
            return []

        headers = [str(h).strip() if h is not None else "" for h in rows[0]]
        items = []

        for row in rows[1:]:
            if all(v is None for v in row):
                continue
            data = {h: (str(v).strip() if v is not None else "") for h, v in zip(headers, row)}

            source_code = data.get("상품코드", "") or data.get("판매자상품코드", "")
            if not source_code:
                continue

            items.append({
                "source_product_code": source_code,
                "product_name": data.get("상품명", ""),
                "price": self._parse_price(data.get("판매가", "")),
                "supply_price": None,
                "status": "active",
                "image_url": data.get("이미지", "") or None,
                "detail_url": "",
                "stock_qty": None,
                "category_name": data.get("카테고리", "") or None,
                "origin": None,
                "own_code": data.get("판매자상품코드", "") or None,
                "detail_description": "",
                "shipping_fee": None,
                "shipping_condition": None,
                "extra": {h: v for h, v in data.items()},
            })

        return items

    def _parse_price(self, text) -> int:
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        return int(cleaned) if cleaned else None

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
