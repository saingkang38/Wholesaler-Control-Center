import logging
logger = logging.getLogger(__name__)
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
			 where (p.use_yn = 'Y' OR p.use_yn = '' OR p.use_yn IS NULL) and p.confirm_yn='Y'   order by pidx DESC """

# 비노출(품절·판매보류) 상품 전용 쿼리 — 엑셀 헤더에 display_yn 컬럼이 없어서 후처리로 status 강제
# 도매처가 사이트 전시 중지한 상품을 "out_of_stock 후보" 로 분류하기 위한 보조 다운로드
PRDT_QUERY_HIDDEN = """select p.*, c.categorycode as catecode, c.*, pc.*  from tblproduct p left join tblcategory c on p.categorycode=c.categorycode
			left join tblpurchase pc on pc.p_code=p.purchasecode
			 where p.display_yn='N' and p.confirm_yn='Y'   order by pidx DESC """


class SikjajeCollector(BaseCollector):
    wholesaler_code = "sikjaje"

    def run(self) -> dict:
        login_id = os.getenv("SIKJAJE_LOGIN_ID", "").strip()
        login_pw = os.getenv("SIKJAJE_LOGIN_PASSWORD", "").strip()
        if not login_id or not login_pw:
            return self._error("SIKJAJE_LOGIN_ID / SIKJAJE_LOGIN_PASSWORD 환경변수 없음")

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
                return self._error(f"로그인 실패 (status={resp.status_code})")
            logger.info("[sikjaje] 로그인 완료")
        except Exception as e:
            return self._error(f"로그인 오류: {e}")

        # 2. 엑셀 다운로드 (메인: 전시 무관 전체)
        try:
            logger.info("[sikjaje] 상세 정보 엑셀 다운로드 중 (메인)...")
            resp = session.post(
                DOWNLOAD_URL,
                data={"mode": "DownloadFullInfo", "prdtQuery": PRDT_QUERY, "pgPrdt": ""},
                headers=HEADERS,
                timeout=120,
            )
            if not resp.ok or len(resp.content) < 1000:
                return self._error(f"엑셀 다운로드 실패 (status={resp.status_code})")
            logger.info(f"[sikjaje] 다운로드 완료(메인): {len(resp.content):,} bytes")
        except Exception as e:
            return self._error(f"엑셀 다운로드 오류: {e}")

        # 2-B. 보조 다운로드: display_yn='N' (비노출/품절 추정) 전용
        # 엑셀 헤더에 display_yn 컬럼이 없어서, 별도 쿼리로 코드 셋만 확보 후 메인 결과에 status 적용
        hidden_codes = set()
        try:
            logger.info("[sikjaje] 비노출 상품 보조 다운로드 중...")
            resp_h = session.post(
                DOWNLOAD_URL,
                data={"mode": "DownloadFullInfo", "prdtQuery": PRDT_QUERY_HIDDEN, "pgPrdt": ""},
                headers=HEADERS,
                timeout=120,
            )
            if resp_h.ok and len(resp_h.content) > 1000:
                hidden_items = self._parse_xlsx(resp_h.content)
                hidden_codes = {it["source_product_code"] for it in hidden_items if it.get("source_product_code")}
                logger.info(f"[sikjaje] 비노출 상품 코드 수집: {len(hidden_codes)}건")
            else:
                logger.warning(f"[sikjaje] 비노출 보조 다운로드 응답 이상 (status={resp_h.status_code}, size={len(resp_h.content)}) — out_of_stock 분류 생략")
        except Exception as e:
            logger.warning(f"[sikjaje] 비노출 보조 다운로드 실패 (무시, 기존 흐름 유지): {e}")

        # 3. 메인 파싱
        try:
            items = self._parse_xlsx(resp.content)
            logger.info(f"[sikjaje] 파싱 완료: {len(items)}건")
        except Exception as e:
            return self._error(f"엑셀 파싱 오류: {e}")

        # 3-B. 비노출 코드에 해당하는 상품은 status='out_of_stock' 로 강제
        if hidden_codes:
            forced = 0
            main_codes = {it["source_product_code"] for it in items}
            for it in items:
                if it["source_product_code"] in hidden_codes and it["status"] == "active":
                    it["status"] = "out_of_stock"
                    forced += 1
            # 비노출에만 존재하고 메인엔 없는 코드 — 별도 추가 (out_of_stock 으로)
            # 메인 파싱은 정상 흐름. hidden-only 는 신규 master 로 들어가 out_of_stock 으로 즉시 표시됨
            try:
                hidden_items_full = self._parse_xlsx(resp_h.content)
                added = 0
                for hit in hidden_items_full:
                    code = hit.get("source_product_code")
                    if code and code not in main_codes:
                        hit["status"] = "out_of_stock"
                        items.append(hit)
                        added += 1
                logger.info(f"[sikjaje] 비노출 강제 표시: 메인내 {forced}건 + 메인외 신규 {added}건 → out_of_stock")
            except Exception as e:
                logger.warning(f"[sikjaje] 비노출 신규 추가 실패 (무시): {e}")

        # status 분포 진단 로그 (다른 도매처 점검 시 비교 기준)
        status_counts = {}
        for it in items:
            s = it.get("status", "active")
            status_counts[s] = status_counts.get(s, 0) + 1
        logger.info(f"[sikjaje] status 분포: {status_counts}")

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
            # display_yn 컬럼 후보 — 도매처 엑셀 헤더 확인 후 정확한 키로 좁힐 수 있음
            display_yn_raw = (
                data.get("진열여부(Y/N)")
                or data.get("전시여부(Y/N)")
                or data.get("display_yn")
                or "Y"
            )
            display_yn = display_yn_raw.strip().upper() if display_yn_raw else "Y"

            # 상태 판정 우선순위:
            # 1) sale_yn != Y         → discontinued (단종)
            # 2) display_yn = N       → out_of_stock (도매처 비노출 = 품절 추정)
            # 3) stock_qty <= 0       → out_of_stock (재고 0)
            # 4) 그 외                → active
            if sale_yn != "Y":
                status = "discontinued"
            elif display_yn == "N":
                status = "out_of_stock"
            elif stock_qty is not None and stock_qty <= 0:
                status = "out_of_stock"
            else:
                status = "active"

            category = data.get("본사_소분류", "") or data.get("본사_중분류", "") or None

            extra = {h: v for h, v in data.items()}
            opt_name = data.get("옵션명", "") or data.get("옵션", "")
            opt_price = data.get("옵션가", "")
            if opt_name:
                extra["옵션"] = opt_name
            if opt_price:
                extra["옵션가"] = opt_price

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
                "extra": extra,
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
