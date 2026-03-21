import os
import re
import time

import requests
from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector

BASE_URL = "https://www.sikjajekr.com"
LOGIN_URL = BASE_URL + "/store/member/login.php"
LIST_URL = BASE_URL + "/product/product_list.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": BASE_URL,
}

PAGE_SIZE = 1000

# 상품관리코드 패턴: s335518_001, C11685_260320_21, C11278_HB005_008 등
CODE_PATTERN = re.compile(r'^[A-Za-z][A-Za-z0-9]*_[A-Za-z0-9_]+$')
# 공급사코드(c11685) 및 AA코드(AA13002) 제외 패턴
SUPPLIER_CODE = re.compile(r'^[cC]\d{3,6}$')
AA_CODE = re.compile(r'^AA\d+$')


class SikjajeCollector(BaseCollector):
    wholesaler_code = "sikjaje"

    def run(self) -> dict:
        session = requests.Session()

        login_id = os.getenv("SIKJAJE_LOGIN_ID")
        login_pw = os.getenv("SIKJAJE_LOGIN_PASSWORD")
        if not login_id or not login_pw:
            return self._err("SIKJAJE_LOGIN_ID / SIKJAJE_LOGIN_PASSWORD 환경변수 없음")

        # 1. 로그인
        try:
            session.get(BASE_URL, headers=HEADERS, timeout=15)
            resp = session.post(
                LOGIN_URL,
                data={
                    "return_url": "/",
                    "type": "login",
                    "id": login_id.strip(),
                    "passwd": login_pw.strip(),
                },
                headers={
                    **HEADERS,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": BASE_URL,
                },
                timeout=15,
                allow_redirects=True,
            )
            # 로그인 성공 시 회원 메뉴 노출
            if "회원정보" not in resp.text and "로그아웃" not in resp.text:
                return self._err(f"로그인 실패 (status={resp.status_code})")
            print("[sikjaje] 로그인 완료")
        except Exception as e:
            return self._err(f"로그인 오류: {e}")

        # 2. 페이지 순회
        items = []
        seen = set()
        page = 1

        try:
            while True:
                print(f"[sikjaje] 페이지 {page} 수집 중...")
                resp = session.get(
                    LIST_URL,
                    params={"page_num": PAGE_SIZE, "page": page},
                    headers=HEADERS,
                    timeout=60,
                )
                if not resp.ok:
                    print(f"[sikjaje] 요청 실패: {resp.status_code}")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                batch = self._parse_page(soup, seen)

                if not batch:
                    print(f"[sikjaje] 페이지 {page}: 상품 없음, 종료")
                    break

                items.extend(batch)
                print(f"[sikjaje] 페이지 {page}: {len(batch)}건 (누계 {len(items)}건)")

                if not self._has_next_page(soup, page):
                    break

                page += 1
                time.sleep(1)

        except Exception as e:
            print(f"[sikjaje] 수집 오류: {e}")
            return {
                "success": False,
                "total_items": len(items),
                "total_pages": page,
                "success_count": len(items),
                "fail_count": 1,
                "error_summary": str(e)[:500],
                "items": items,
            }

        print(f"[sikjaje] 수집 완료: {len(items)}건")
        return {
            "success": True,
            "total_items": len(items),
            "total_pages": page,
            "success_count": len(items),
            "fail_count": 0,
            "error_summary": None,
            "items": items,
        }

    def _parse_page(self, soup: BeautifulSoup, seen: set) -> list:
        items = []

        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            texts = [td.get_text(strip=True) for td in tds]

            if len(tds) < 6:
                continue

            # 상품관리코드 위치 찾기 (공급사코드/AA코드 제외)
            code_idx = None
            for i, text in enumerate(texts):
                if CODE_PATTERN.match(text) and not SUPPLIER_CODE.match(text) and not AA_CODE.match(text):
                    code_idx = i
                    break

            if code_idx is None:
                continue

            source_code = texts[code_idx]
            if source_code in seen:
                continue
            seen.add(source_code)

            # 상품명: 코드 다음 비어있지 않은 셀 (index+1은 이미지 셀로 비어있음)
            product_name = ""
            for i in range(code_idx + 1, min(code_idx + 4, len(texts))):
                t = texts[i]
                if t and len(t) >= 2 and not re.match(r'^[\d,]+$', t) and t not in ("상품주문하기",):
                    product_name = t
                    break

            if not product_name:
                continue

            # 가격: 상품명 이후 순수 숫자(쉼표포함) 셀
            price = None
            name_idx = next((i for i in range(code_idx + 1, len(texts)) if texts[i] == product_name), code_idx + 1)
            for i in range(name_idx + 1, min(name_idx + 5, len(texts))):
                t = texts[i]
                if re.match(r'^\d[\d,]*$', t):
                    p = self._parse_price(t)
                    if p and 100 <= p <= 50_000_000:
                        price = p
                        break

            # 카테고리: texts[0]에서 AA코드 이후 부분 제거
            category = ""
            raw_cat = texts[0] if texts else ""
            if ">" in raw_cat:
                # AA코드(AAxxxxx) 제거: 마지막 AA 패턴 앞까지만 사용
                cat_clean = re.sub(r'AA\d+.*$', '', raw_cat).strip()
                if cat_clean:
                    category = cat_clean

            # 이미지
            img_url = ""
            for td in tds:
                img = td.find("img")
                if img:
                    src = img.get("src") or img.get("data-src") or ""
                    if src:
                        img_url = BASE_URL + src if src.startswith("/") else src
                        break

            items.append({
                "source_product_code": source_code,
                "product_name": product_name,
                "price": price,
                "supply_price": None,
                "status": "active",
                "image_url": img_url,
                "detail_url": "",
                "stock_qty": None,
                "category_name": category or None,
            })

        return items

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        # pageNavi 클래스에서 다음 페이지 링크 찾기
        for a in soup.find_all("a", class_="navi"):
            href = a.get("href", "")
            if f"page={current_page + 1}" in href:
                return True
        # 텍스트로 다음 페이지 숫자 링크 찾기
        return bool(soup.find("a", string=str(current_page + 1)))

    def _parse_price(self, text):
        if not text:
            return None
        cleaned = "".join(c for c in str(text) if c.isdigit())
        if not cleaned:
            return None
        val = int(cleaned)
        if val > 100_000_000:  # 1억 초과는 파싱 오류로 간주
            return None
        return val

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
