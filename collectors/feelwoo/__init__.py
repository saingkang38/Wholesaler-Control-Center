import io
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests

from app.collectors.base import BaseCollector

BASE_URL = "https://www.feelwoo.com"
OFFICE_URL = "http://feelwoo-office.co.kr"
LOGIN_URL = BASE_URL + "/member/login_ps.php"
DOWNLOAD_URL = OFFICE_URL + "/FWC/FWCSmartStoreExcelDown.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": BASE_URL,
}

# 모든 카테고리 코드 (feelwoo-office 다운로드 파라미터)
ALL_CATEGORIES = "006,007,010,011,012,013,014,015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,030,031"


class FeelwooCollector(BaseCollector):
    wholesaler_code = "feelwoo"

    def run(self) -> dict:
        session = requests.Session()

        login_id = os.getenv("FEELWOO_LOGIN_ID")
        login_pw = os.getenv("FEELWOO_LOGIN_PASSWORD")
        if not login_id or not login_pw:
            return {
                "success": False,
                "total_items": 0,
                "total_pages": 0,
                "success_count": 0,
                "fail_count": 0,
                "error_summary": "FEELWOO_LOGIN_ID / FEELWOO_LOGIN_PASSWORD 환경변수 없음",
                "items": [],
            }

        # 1. feelwoo.com 로그인
        try:
            login_page_url = BASE_URL + "/member/login.php"
            session.get(login_page_url, headers=HEADERS, timeout=15)

            resp = session.post(
                LOGIN_URL,
                data={
                    "mode": "login",
                    "loginId": login_id.strip(),
                    "loginPwd": login_pw.strip(),
                    "returnUrl": BASE_URL + "/",
                },
                headers={
                    **HEADERS,
                    "Referer": login_page_url,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": BASE_URL,
                },
                timeout=15,
                allow_redirects=True,
            )
            if "parent.location.href" not in resp.text:
                return {
                    "success": False,
                    "total_items": 0,
                    "total_pages": 0,
                    "success_count": 0,
                    "fail_count": 1,
                    "error_summary": f"로그인 실패 (status={resp.status_code})",
                    "items": [],
                }
            print("[feelwoo] 로그인 완료")
        except Exception as e:
            return {
                "success": False,
                "total_items": 0,
                "total_pages": 0,
                "success_count": 0,
                "fail_count": 1,
                "error_summary": f"로그인 오류: {e}",
                "items": [],
            }

        # 2. feelwoo-office.co.kr 접근 확인
        try:
            session.get(OFFICE_URL + "/FWC/FWCProductDown.php", headers=HEADERS, timeout=15)
        except Exception:
            pass

        # 3. Excel 다운로드
        today = datetime.now().strftime("%Y-%m-%d")
        params = {
            "fromDate": "2019-01-01",
            "toDate": today,
            "category": ALL_CATEGORIES,
            "stock": "",
            "costRate": "1",
            "sumnailIdx": "1",
        }

        try:
            print("[feelwoo] Excel 다운로드 시작...")
            dl_resp = session.get(
                DOWNLOAD_URL,
                params=params,
                headers={**HEADERS, "Referer": OFFICE_URL + "/FWC/FWCProductDown.php"},
                timeout=60,
                stream=True,
            )
            if not dl_resp.ok:
                return {
                    "success": False,
                    "total_items": 0,
                    "total_pages": 0,
                    "success_count": 0,
                    "fail_count": 1,
                    "error_summary": f"Excel 다운로드 실패 (status={dl_resp.status_code})",
                    "items": [],
                }

            content = dl_resp.content
            content_type = dl_resp.headers.get("Content-Type", "")
            print(f"[feelwoo] 다운로드 완료: {len(content)} bytes, Content-Type: {content_type}")

            # 원본 파일 저장
            try:
                save_dir = Path.home() / "OneDrive" / "supplier_sync" / "feelwoo"
                save_dir.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                raw_path = save_dir / f"feelwoo_{timestamp}.xlsx"
                raw_path.write_bytes(content)
                print(f"[feelwoo] 원본 저장: {raw_path}")
            except Exception as e:
                print(f"[feelwoo] 원본 저장 실패 (무시): {e}")

            if len(content) < 100:
                return {
                    "success": False,
                    "total_items": 0,
                    "total_pages": 0,
                    "success_count": 0,
                    "fail_count": 1,
                    "error_summary": f"응답이 너무 짧음 ({len(content)} bytes): {content[:200]}",
                    "items": [],
                }

        except Exception as e:
            return {
                "success": False,
                "total_items": 0,
                "total_pages": 0,
                "success_count": 0,
                "fail_count": 1,
                "error_summary": f"Excel 다운로드 오류: {e}",
                "items": [],
            }

        # 4. Excel 파싱
        try:
            items = self._parse_excel(content)
        except Exception as e:
            return {
                "success": False,
                "total_items": 0,
                "total_pages": 0,
                "success_count": 0,
                "fail_count": 1,
                "error_summary": f"Excel 파싱 오류: {e}",
                "items": [],
            }

        print(f"[feelwoo] 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": 1,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _parse_excel(self, content: bytes) -> list:
        from bs4 import BeautifulSoup

        # UTF-8 BOM 제거 후 디코드
        text = content.decode("utf-8-sig", errors="replace")

        soup = BeautifulSoup(text, "lxml-xml")

        # Microsoft SpreadsheetML: <Row><Cell><Data>...</Data></Cell></Row>
        rows = soup.find_all("Row")
        if not rows:
            # HTML 테이블 형식 fallback
            soup2 = BeautifulSoup(text, "html.parser")
            rows_html = soup2.find_all("tr")
            if not rows_html:
                raise ValueError(f"테이블 구조를 찾을 수 없음. 내용 앞부분: {text[:300]}")
            return self._parse_html_rows(rows_html)

        # SpreadsheetML 파싱
        all_rows = []
        for row in rows:
            cells = []
            for cell in row.find_all("Cell"):
                data = cell.find("Data")
                cells.append(data.get_text(strip=True) if data else "")
            all_rows.append(cells)

        return self._extract_items(all_rows)

    def _parse_html_rows(self, tr_list) -> list:
        all_rows = []
        for tr in tr_list:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            all_rows.append(cells)
        return self._extract_items(all_rows)

    def _extract_items(self, all_rows: list) -> list:
        if not all_rows:
            return []

        # 헤더 행 찾기
        header_row_idx = 0
        headers = []
        for i, row in enumerate(all_rows[:10]):
            row_text = " ".join(row)
            if any(kw in row_text for kw in ["상품코드", "상품명", "판매가", "공급가"]):
                header_row_idx = i
                headers = row
                break
        if not headers:
            headers = all_rows[0]

        print(f"[feelwoo] 헤더: {headers}")

        col = self._find_col
        idx_code = col(headers, ["상품코드", "품번", "코드", "code"])
        idx_name = col(headers, ["상품명", "품명", "제품명", "name"])
        idx_price = col(headers, ["판매가", "공급가", "가격", "price", "금액"])
        idx_stock = col(headers, ["재고", "수량", "stock"])
        idx_status = col(headers, ["상태", "판매여부", "사용여부"])
        idx_image = col(headers, ["이미지", "썸네일", "image", "img"])
        idx_category = col(headers, ["카테고리", "분류"])

        if idx_code is None and idx_name is None:
            raise ValueError(f"상품코드/상품명 컬럼 없음. 헤더: {headers}")

        mapped_indices = {i for i in [idx_code, idx_name, idx_price, idx_stock, idx_status, idx_image, idx_category] if i is not None}

        items = []
        seen = set()

        for row in all_rows[header_row_idx + 1:]:
            def cell(idx, r=row):
                if idx is None or idx >= len(r):
                    return None
                return r[idx].strip() or None

            source_code = cell(idx_code) or ""
            product_name = cell(idx_name) or ""

            if not product_name:
                continue
            if not source_code:
                source_code = re.sub(r'\s+', '_', product_name)[:50]

            if source_code in seen:
                continue
            seen.add(source_code)

            price = self._parse_price(cell(idx_price))

            status = "active"
            if idx_status is not None:
                st = cell(idx_status) or ""
                if st in ("N", "0", "품절", "판매중지", "미사용"):
                    status = "out_of_stock"
            if idx_stock is not None:
                qty_str = cell(idx_stock) or "0"
                try:
                    qty = int(float(qty_str))
                    if qty <= 0:
                        status = "out_of_stock"
                except (ValueError, TypeError):
                    pass

            image_url = cell(idx_image) or ""
            category = cell(idx_category) or ""

            extra = {}
            for i, h in enumerate(headers):
                if i not in mapped_indices and h:
                    extra[h] = cell(i)

            items.append({
                "source_product_code": source_code,
                "product_name": product_name,
                "price": price,
                "supply_price": None,
                "status": status,
                "image_url": image_url,
                "detail_url": "",
                "stock_qty": None,
                "category_name": category or None,
                "extra": extra,
            })

        return items

    def _find_col(self, headers: list, candidates: list):
        for candidate in candidates:
            for i, h in enumerate(headers):
                if candidate.lower() in h.lower():
                    return i
        return None

    def _parse_price(self, text):
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        return int(cleaned) if cleaned else None
