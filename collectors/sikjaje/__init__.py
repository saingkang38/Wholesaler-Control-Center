import io
import os
import time

import openpyxl
import requests

from app.collectors.base import BaseCollector

BASE_URL = "https://www.sikjajekr.com"
LOGIN_URL = BASE_URL + "/store/member/login.php"
DOWNLOAD_URL = BASE_URL + "/product/excel_download.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": BASE_URL + "/product/product_list.php",
}

PRDT_QUERY = """select p.*, c.categorycode as catecode, c.*, pc.*  from tblproduct p left join tblcategory c on p.categorycode=c.categorycode
			left join tblpurchase pc on pc.p_code=p.purchasecode
			 where (p.use_yn = 'Y' OR p.use_yn = '' OR p.use_yn IS NULL) and p.confirm_yn='Y' and p.display_yn='Y' and  p.confirm_yn = 'Y' and p.display_yn = 'Y'   order by pidx DESC """


class SikjajeCollector(BaseCollector):
    wholesaler_code = "sikjaje"

    def run(self) -> dict:
        login_id = os.getenv("SIKJAJE_LOGIN_ID", "").strip()
        login_pw = os.getenv("SIKJAJE_LOGIN_PASSWORD", "").strip()
        if not login_id or not login_pw:
            return self._err("SIKJAJE_LOGIN_ID / SIKJAJE_LOGIN_PASSWORD 환경변수 없음")

        session = requests.Session()

        # 1. 로그인
        try:
            session.get(BASE_URL, headers=HEADERS, timeout=15)
            resp = session.post(
                LOGIN_URL,
                data={"return_url": "/", "type": "login", "id": login_id, "passwd": login_pw},
                headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded", "Origin": BASE_URL},
                timeout=15,
                allow_redirects=True,
            )
            if "회원정보" not in resp.text and "로그아웃" not in resp.text:
                return self._err(f"로그인 실패 (status={resp.status_code})")
            print("[sikjaje] 로그인 완료")
        except Exception as e:
            return self._err(f"로그인 오류: {e}")

        # 2. 엑셀 다운로드
        try:
            print("[sikjaje] 상세 정보 엑셀 다운로드 중...")
            resp = session.post(
                DOWNLOAD_URL,
                data={"mode": "DownloadFullInfo", "prdtQuery": PRDT_QUERY, "pgPrdt": ""},
                headers=HEADERS,
                timeout=120,
            )
            if not resp.ok or len(resp.content) < 1000:
                return self._err(f"엑셀 다운로드 실패 (status={resp.status_code})")
            print(f"[sikjaje] 다운로드 완료: {len(resp.content):,} bytes")
        except Exception as e:
            return self._err(f"엑셀 다운로드 오류: {e}")

        # 3. 파싱
        try:
            items = self._parse_xlsx(resp.content)
            print(f"[sikjaje] 파싱 완료: {len(items)}건")
        except Exception as e:
            return self._err(f"엑셀 파싱 오류: {e}")

        return {
            "success": True,
            "total_items": len(items),
            "total_pages": 1,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _parse_xlsx(self, content: bytes) -> list:
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True))
        wb.close()

        if not rows:
            return []

        headers = [str(h).strip() if h is not None else "" for h in rows[0]]
        items = []

        for row in rows[1:]:
            if all(v is None for v in row):
                continue

            data = {}
            for h, v in zip(headers, row):
                data[h] = str(v).strip() if v is not None else ""

            source_code = data.get("상품관리코드", "").strip()
            if not source_code:
                continue

            product_name = data.get("상품명(25자 이하)", "").strip()
            price = self._parse_price(data.get("판매점가(VAT포함)", ""))
            image_url = data.get("이미지1", "").strip() or None
            stock_qty = self._parse_int(data.get("재고수량(99999)", ""))

            sale_yn = data.get("판매여부(Y/N)", "Y").strip().upper()
            status = "active" if sale_yn == "Y" else "discontinued"

            category = data.get("본사_소분류", "") or data.get("본사_중분류", "") or None

            items.append({
                "source_product_code": source_code,
                "product_name": product_name,
                "price": price,
                "supply_price": None,
                "status": status,
                "image_url": image_url,
                "detail_url": "",
                "stock_qty": stock_qty,
                "category_name": category,
                "origin": None,
                "own_code": None,
                "detail_description": data.get("상세설명(HTML)", ""),
                "shipping_fee": self._parse_price(data.get("배송비(기본)", "")),
                "shipping_condition": None,
                "extra": {h: v for h, v in data.items()},
            })

        return items

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

    def _err(self, msg: str) -> dict:
        return {
            "success": False,
            "total_items": 0,
            "total_pages": 0,
            "success_count": 0,
            "fail_count": 1,
            "error_summary": msg,
            "items": [],
        }
