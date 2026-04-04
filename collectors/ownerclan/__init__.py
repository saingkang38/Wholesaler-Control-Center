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


class OwnerclanCollector(BaseCollector):
    wholesaler_code = "ownerclan"

    def run(self, supplier_code: str = None) -> dict:
        login_id = os.getenv("OWNERCLAN_LOGIN_ID")
        login_pw = os.getenv("OWNERCLAN_LOGIN_PASSWORD")

        if not login_id or not login_pw:
            return self._error("OWNERCLAN_LOGIN_ID / OWNERCLAN_LOGIN_PASSWORD 미설정")

        items = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()
                page.set_default_timeout(30000)

                # 1. 로그인
                page.goto(LOGIN_URL)
                page.wait_for_load_state("networkidle")
                time.sleep(1)
                page.fill("input[name='id']", login_id)
                page.fill("input[name='passwd']", login_pw)
                page.click("input[type='submit'].img_log")
                page.wait_for_load_state("networkidle")
                time.sleep(2)

                if "loginform" in page.url.lower():
                    browser.close()
                    return self._error("로그인 실패 - 계정 정보 확인 필요")

                logger.info("[ownerclan] 로그인 성공")

                # 2. DB 다운로드 폼 페이지 이동
                page.goto(DOWNLOAD_FORM_URL)
                page.wait_for_load_state("networkidle")
                time.sleep(1)

                # 3. 마켓수수료 0% 설정
                try:
                    page.fill("input[name='price_free_rate5']", "0")
                except Exception:
                    pass

                # 4. "찜한 공급사 상품만 다운로드" 라디오 선택 (value="D")
                page.check("input[name='is_search_vender'][value='D']")
                time.sleep(0.5)

                # 5. 다운로드 세트 만들기 클릭 (팝업 2회 자동 수락)
                page.on("dialog", lambda d: (logger.info(f"[ownerclan] 팝업: {d.message[:80]}"), d.accept()))
                page.click("button#btn_submit2")
                page.wait_for_load_state("networkidle")
                time.sleep(2)

                # 6. 목록 페이지로 이동 후 idx 확인
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
                    browser.close()
                    return self._error("다운로드 세트 idx 확인 실패")

                logger.info(f"[ownerclan] 다운로드 세트 생성 / idx={idx}")

                # 7. 1시간 대기 후 다운로드 (작업 완료에 시간이 많이 걸림)
                wait_seconds = int(os.getenv("OWNERCLAN_WAIT_SECONDS", "1200"))
                logger.info(f"[ownerclan] {wait_seconds}초 대기 후 다운로드 시작...")
                time.sleep(wait_seconds)

                # 8. showDownloadList 호출 → 하위 행(tr#downloadTr) 표시
                page.evaluate(f"showDownloadList('{idx}')")

                # AJAX 응답으로 DOM에 다운로드 링크가 생길 때까지 대기
                download_selector = f'a[href*="downloadServer.php?idx={idx}"]'
                try:
                    page.wait_for_selector(download_selector, timeout=30000)
                except PlaywrightTimeout:
                    browser.close()
                    return self._error(f"다운로드 링크 미표시 (idx={idx}) - showDownloadList 응답 없음")

                # 9. 전체 다운로드 클릭 → 파일 다운로드
                with page.expect_download(timeout=120000) as dl_info:
                    page.evaluate(f"""
                        () => {{
                            const link = document.querySelector('a[href*="downloadServer.php?idx={idx}"]');
                            if (link) link.click();
                        }}
                    """)

                download = dl_info.value
                tmp_path = download.path()
                logger.info(f"[ownerclan] ZIP 다운로드 완료: {tmp_path}")

                # 브라우저 닫기 전에 프로젝트 downloads 폴더로 복사
                import shutil
                from datetime import datetime
                downloads_dir = Path(__file__).resolve().parents[2] / "downloads" / "ownerclan"
                downloads_dir.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_path = str(downloads_dir / f"ownerclan_{timestamp}.zip")
                shutil.copy2(tmp_path, safe_path)
                logger.info(f"[ownerclan] 파일 저장 위치: {safe_path}")

                browser.close()

                # 10. ZIP 해제 + xlsx 파싱
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
