"""
기존에 저장된 오너클랜 ZIP 파일에서 본문상세설명을 읽어 MasterProduct DB를 업데이트한다.
재수집 없이 OneDrive에 있는 최신 ZIP을 사용한다.
"""
import io
import sys
import zipfile
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from dotenv import load_dotenv
load_dotenv(BASE / ".env")
load_dotenv(BASE / ".env.local", override=True)

import openpyxl
from app import create_app
from app.master.models import MasterProduct
from app.infrastructure import db

app = create_app()

with app.app_context():
    # 컬럼 없으면 추가
    from sqlalchemy import text
    try:
        db.session.execute(text("ALTER TABLE master_products ADD COLUMN detail_description TEXT"))
        db.session.commit()
        print("detail_description 컬럼 추가됨")
    except Exception:
        pass  # 이미 존재

    zip_dir = Path.home() / "OneDrive" / "supplier_sync" / "ownerclan"
    zips = sorted(zip_dir.glob("*.zip"))
    if not zips:
        print(f"ZIP 파일 없음: {zip_dir}")
        sys.exit(1)

    latest = zips[-1]
    print(f"사용 파일: {latest}")

    updated = 0
    skipped = 0

    with zipfile.ZipFile(latest) as zf:
        xlsx_names = [n for n in zf.namelist() if n.endswith(".xlsx")]
        print(f"xlsx 파일 수: {len(xlsx_names)}")

        for xlsx_name in xlsx_names:
            print(f"  읽는 중: {xlsx_name}")
            wb = openpyxl.load_workbook(
                io.BytesIO(zf.read(xlsx_name)), read_only=True, data_only=True
            )
            for ws in wb.worksheets:
                rows = list(ws.iter_rows(values_only=True))
                if not rows:
                    continue

                # 헤더 행 찾기 (최대 3행 탐색)
                header_idx = 0
                for i, row in enumerate(rows[:3]):
                    if any(str(c or "").strip() == "본문상세설명" for c in row):
                        header_idx = i
                        break

                headers = [str(h or "").strip() for h in rows[header_idx]]

                # 상품명 컬럼 찾기
                col_name = None
                col_desc = None
                for i, h in enumerate(headers):
                    if col_name is None and ("상품명" in h or "품명" in h):
                        col_name = i
                    if col_desc is None and h == "본문상세설명":
                        col_desc = i

                if col_name is None or col_desc is None:
                    print(f"    컬럼 없음 (상품명={col_name}, 본문상세설명={col_desc}) — 건너뜀")
                    continue

                print(f"    헤더 행={header_idx}, 상품명 col={col_name}, 본문상세설명 col={col_desc}")

                batch = 0
                for row in rows[header_idx + 1:]:
                    name_val = str(row[col_name] or "").strip() if len(row) > col_name else ""
                    desc_val = str(row[col_desc] or "").strip() if len(row) > col_desc else ""

                    if not name_val or not desc_val:
                        skipped += 1
                        continue

                    master = MasterProduct.query.filter_by(product_name=name_val).first()
                    if master and not master.detail_description:
                        master.detail_description = desc_val
                        updated += 1
                        batch += 1
                        if batch % 500 == 0:
                            db.session.commit()
                            print(f"    중간 커밋: {updated}건")

                db.session.commit()
            wb.close()

    print(f"\n완료 - 업데이트: {updated}건 / 건너뜀: {skipped}건")
