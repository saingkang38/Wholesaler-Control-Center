import logging
logger = logging.getLogger(__name__)
import os
import time
import zipfile
import tempfile
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from app.collectors.base import BaseCollector

LOGIN_URL = "https://www.ownerclan.com/V2/member/loginform.php"
DOWNLOAD_LIST_URL = "https://www.ownerclan.com/V2/service/productDownloadList.php"
DOWNLOAD_FORM_URL = "https://www.ownerclan.com/V2/service/productDownload.php"

STATUS_MAP = {
    "품절": "out_of_stock",
    "일시품절": "out_of_stock",
    "단종": "discontinued",
    "판매중지": "discontinued",
}


IDX_FILE = Path(__file__).resolve().parents[2] / "downloads" / "ownerclan" / ".ownerclan_idx.txt"


class OwnerclanCollector(BaseCollector):
    wholesaler_code = "ownerclan"

    def run(self, supplier_code: str = None, phase: str = None) -> dict:
        """
        phase=None   : 기존 전체 실행 (트리거 + 대기 + 다운로드)
        phase='trigger'  : 로그인 → 다운로드 세트 요청 → idx 저장 후 종료
        phase='download' : 로그인 → 저장된 idx로 다운로드 → 파싱
        """
        login_id = os.getenv("OWNERCLAN_LOGIN_ID")
        login_pw = os.getenv("OWNERCLAN_LOGIN_PASSWORD")

        if not login_id or not login_pw:
            return self._error("OWNERCLAN_LOGIN_ID / OWNERCLAN_LOGIN_PASSWORD 미설정")

        if phase == "trigger":
            return self._run_trigger(login_id, login_pw)
        elif phase == "download":
            return self._run_download(login_id, login_pw)
        else:
            return self._run_full(login_id, login_pw)

    # ──────────────────────────────────────────────
    # 트리거 단계: 로그인 → 다운로드 세트 요청 → idx 저장
    # ──────────────────────────────────────────────

    def _run_trigger(self, login_id: str, login_pw: str) -> dict:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()
                page.set_default_timeout(30000)

                idx = self._login_and_trigger(page, login_id, login_pw)
                browser.close()

            IDX_FILE.parent.mkdir(parents=True, exist_ok=True)
            IDX_FILE.write_text(str(idx))
            logger.info(f"[ownerclan] 트리거 완료 / idx={idx} 저장됨")

            return {
                "success": True,
                "total_items": 0, "total_pages": 0,
                "success_count": 0, "fail_count": 0,
                "error_summary": None,
                "items": [],
                "trigger_idx": idx,
            }
        except Exception as e:
            logger.error(f"[ownerclan] 트리거 실패: {e}")
            return self._error(f"트리거 실패: {str(e)[:300]}")

    # ──────────────────────────────────────────────
    # 다운로드 단계: 저장된 idx로 파일 다운로드 + 파싱
    # ──────────────────────────────────────────────

    def _run_download(self, login_id: str, login_pw: str) -> dict:
        if not IDX_FILE.exists():
            return self._error("트리거 idx 파일 없음 — 먼저 트리거를 실행하세요")

        idx = IDX_FILE.read_text().strip()
        if not idx:
            return self._error("트리거 idx 값이 비어있음")

        items = []
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()
                page.set_default_timeout(30000)

                self._login(page, login_id, login_pw)

                safe_path = self._download_file(page, idx)
                browser.close()

            items, total_rows = self._parse_zip(safe_path)
            IDX_FILE.unlink(missing_ok=True)
            logger.info(f"[ownerclan] 다운로드+파싱 완료: {total_rows}건")

        except PlaywrightTimeout as e:
            return self._error(f"타임아웃: {str(e)[:200]}")
        except Exception as e:
            logger.warning(f"[ownerclan] 다운로드 오류: {e}")
            return self._error(str(e)[:500])

        return {
            "success": True,
            "total_items": total_rows,
            "total_pages": 1,
            "success_count": total_rows,
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    # ──────────────────────────────────────────────
    # 전체 실행 (기존 동작 유지)
    # ──────────────────────────────────────────────

    def _run_full(self, login_id: str, login_pw: str) -> dict:
        items = []
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()
                page.set_default_timeout(30000)

                idx = self._login_and_trigger(page, login_id, login_pw)

                wait_seconds = int(os.getenv("OWNERCLAN_WAIT_SECONDS", "1200"))
                logger.info(f"[ownerclan] {wait_seconds}초 대기 후 다운로드 시작...")
                time.sleep(wait_seconds)

                safe_path = self._download_file(page, idx)
                browser.close()

            items, total_rows = self._parse_zip(safe_path)
            logger.info(f"[ownerclan] 파싱 완료: {total_rows}건")

        except PlaywrightTimeout as e:
            return {
                "success": False,
                "total_items": len(items), "total_pages": 0,
                "success_count": len(items), "fail_count": 1,
                "error_summary": f"타임아웃: {str(e)[:200]}",
                "items": items,
            }
        except Exception as e:
            logger.warning(f"[ownerclan] 오류 발생: {e}")
            return {
                "success": False,
                "total_items": len(items), "total_pages": 0,
                "success_count": len(items), "fail_count": 1,
                "error_summary": str(e)[:500],
                "items": items,
            }

        return {
            "success": True,
            "total_items": total_rows,
            "total_pages": 1,
            "success_count": total_rows,
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    # ──────────────────────────────────────────────
    # 공통: 로그인
    # ──────────────────────────────────────────────

    def _login(self, page, login_id: str, login_pw: str):
        page.goto(LOGIN_URL)
        page.wait_for_load_state("networkidle")
        time.sleep(1)
        page.fill("input[name='id']", login_id)
        page.fill("input[name='passwd']", login_pw)
        page.click("input[type='submit'].img_log")
        page.wait_for_load_state("networkidle")
        time.sleep(2)
        if "loginform" in page.url.lower():
            raise Exception("로그인 실패 - 계정 정보 확인 필요")
        logger.info("[ownerclan] 로그인 성공")

    # ──────────────────────────────────────────────
    # 공통: 로그인 + 트리거 → idx 반환
    # ──────────────────────────────────────────────

    def _login_and_trigger(self, page, login_id: str, login_pw: str) -> str:
        self._login(page, login_id, login_pw)

        page.goto(DOWNLOAD_FORM_URL)
        page.wait_for_load_state("networkidle")
        time.sleep(1)

        try:
            page.fill("input[name='price_free_rate5']", "0")
        except Exception:
            pass

        page.check("input[name='is_search_vender'][value='D']")
        time.sleep(0.5)

        page.on("dialog", lambda d: (logger.info(f"[ownerclan] 팝업: {d.message[:80]}"), d.accept()))
        page.click("button#btn_submit2")
        page.wait_for_load_state("networkidle")
        time.sleep(2)

        page.goto(DOWNLOAD_LIST_URL)
        page.wait_for_load_state("networkidle")
        time.sleep(1)

        idx = page.evaluate(r"""
            () => {
                const links = document.querySelectorAll('a[href*="productDownload.php?idx="]');
                if (links.length > 0) {
                    const m = links[0].href.match(/idx=(\d+)/);
                    if (m) return m[1];
                }
                const spans = document.querySelectorAll('[id^="downloadSpan"]');
                if (spans.length > 0) {
                    const m = spans[0].id.match(/\d+/);
                    if (m) return m[0];
                }
                return null;
            }
        """)

        if not idx:
            raise Exception("다운로드 세트 idx 확인 실패")

        logger.info(f"[ownerclan] 다운로드 세트 생성 / idx={idx}")
        return idx

    # ──────────────────────────────────────────────
    # 공통: idx로 파일 다운로드 → 저장 경로 반환
    # ──────────────────────────────────────────────

    def _download_file(self, page, idx: str) -> str:
        page.goto(DOWNLOAD_LIST_URL)
        page.wait_for_load_state("networkidle")
        time.sleep(1)

        page.evaluate(f"showDownloadList('{idx}')")

        download_selector = f'a[href*="downloadServer.php?idx={idx}"]'
        try:
            page.wait_for_selector(download_selector, timeout=30000)
        except PlaywrightTimeout:
            raise Exception(f"다운로드 링크 미표시 (idx={idx})")

        with page.expect_download(timeout=120000) as dl_info:
            page.evaluate(f"""
                () => {{
                    const link = document.querySelector('a[href*="downloadServer.php?idx={idx}"]');
                    if (link) link.click();
                }}
            """)

        import shutil
        from datetime import datetime
        download = dl_info.value
        tmp_path = download.path()
        downloads_dir = Path(__file__).resolve().parents[2] / "downloads" / "ownerclan"
        downloads_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_path = str(downloads_dir / f"ownerclan_{timestamp}.zip")
        shutil.copy2(tmp_path, safe_path)
        logger.info(f"[ownerclan] 파일 저장: {safe_path}")
        return safe_path

    def _parse_zip(self, zip_path) -> tuple:
        import openpyxl
        items = []
        total_rows = 0

        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmpdir)

            xlsx_files = sorted(Path(tmpdir).glob("*.xlsx"))
            if not xlsx_files:
                raise Exception("ZIP 내 xlsx 파일 없음")

            logger.info(f"[ownerclan] xlsx 파일 수: {len(xlsx_files)}개")

            for xlsx_file in xlsx_files:
                wb = openpyxl.load_workbook(str(xlsx_file), read_only=True, data_only=True)
                ws = wb.active
                rows = list(ws.iter_rows(values_only=True))
                wb.close()

                if not rows:
                    continue

                # 비어있지 않은 셀이 더 많은 행을 실제 헤더로 선택
                header_row_idx = 0
                if len(rows) > 1:
                    count0 = sum(1 for h in rows[0] if h)
                    count1 = sum(1 for h in rows[1] if h)
                    if count1 > count0:
                        header_row_idx = 1

                headers = [str(h).strip() if h is not None else "" for h in rows[header_row_idx]]
                col = self._map_columns(headers)

                data_rows = [r for r in rows[header_row_idx + 1:] if any(r)]
                total_rows += len(data_rows)

                standard_cols = set(v for v in col.values() if v is not None)

                for row in data_rows:
                    code = self._cell(row, col.get("code"))
                    if not code:
                        continue
                    extra = {}
                    for i, h in enumerate(headers):
                        if i not in standard_cols and h:
                            extra[h] = self._cell(row, i)
                    items.append({
                        "source_product_code": str(code),
                        "product_name": self._cell(row, col.get("name")),
                        "price": self._parse_price(self._cell(row, col.get("price"))),
                        "supply_price": self._parse_price(self._cell(row, col.get("supply_price"))),
                        "status": STATUS_MAP.get(self._cell(row, col.get("status")) or "", "active"),
                        "image_url": self._cell(row, col.get("image_url")),
                        "detail_url": self._cell(row, col.get("detail_url")),
                        "stock_qty": self._parse_int(self._cell(row, col.get("stock_qty"))),
                        "category_name": self._cell(row, col.get("category_name")),
                        "detail_description": self._cell(row, col.get("detail_description")),
                        "extra": extra,
                    })

        logger.info(f"[ownerclan] 전체 데이터 행: {total_rows}개 / 파싱 성공: {len(items)}개")
        return items, total_rows

    def _map_columns(self, headers) -> dict:
        mapping = {}
        for i, h in enumerate(headers):
            h_lower = h.lower()
            if any(k in h for k in ["상품코드", "품번", "코드"]) and "code" not in mapping:
                mapping["code"] = i
            elif any(k in h for k in ["상품명", "품명"]) and "name" not in mapping:
                mapping["name"] = i
            elif ("공급가" in h or "원가" in h) and "supply_price" not in mapping:
                mapping["supply_price"] = i
            elif ("판매가" in h or "소비자가" in h) and "price" not in mapping:
                mapping["price"] = i
            elif "재고" in h and "stock_qty" not in mapping:
                mapping["stock_qty"] = i
            elif "상태" in h and "status" not in mapping:
                mapping["status"] = i
            elif "이미지" in h and "image_url" not in mapping:
                mapping["image_url"] = i
            elif ("상세" in h and ("url" in h_lower or "주소" in h or "링크" in h)) and "detail_url" not in mapping:
                mapping["detail_url"] = i
            elif ("카테고리" in h or "분류" in h) and "category_name" not in mapping:
                mapping["category_name"] = i
            elif "본문상세설명" in h and "detail_description" not in mapping:
                mapping["detail_description"] = i
        return mapping

    def _cell(self, row, idx):
        if idx is None or idx >= len(row):
            return None
        val = row[idx]
        return str(val).strip() if val is not None else None

    def _parse_price(self, text) -> int:
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        return int(cleaned) if cleaned else None

    def _parse_int(self, text) -> int:
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
