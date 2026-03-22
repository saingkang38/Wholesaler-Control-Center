import io
from flask import Blueprint, render_template, request, jsonify, Response
from flask_login import login_required
from app.master.models import MasterProduct
from app.infrastructure import db

product_prep_bp = Blueprint("product_prep", __name__)


@product_prep_bp.route("/product-prep")
@login_required
def product_prep_page():
    from app.wholesalers.models import Wholesaler
    wholesalers = Wholesaler.query.order_by(Wholesaler.name).all()
    wholesaler_id = request.args.get("wholesaler_id", type=int)
    limit = request.args.get("limit", 1000, type=int)
    if limit not in (20, 50, 100, 300, 500, 1000, 2000):
        limit = 1000
    page = request.args.get("page", 1, type=int)
    if page < 1:
        page = 1

    q = MasterProduct.query.filter(
        MasterProduct.is_prep_ready == False,
        MasterProduct.current_status == "active",
    )
    if wholesaler_id:
        q = q.filter(MasterProduct.wholesaler_id == wholesaler_id)

    total = q.count()
    import math
    total_pages = max(1, math.ceil(total / limit))
    if page > total_pages:
        page = total_pages

    products = q.order_by(MasterProduct.id.desc()).offset((page - 1) * limit).limit(limit).all()
    return render_template(
        "product_prep.html",
        products=products,
        wholesalers=wholesalers,
        selected_wholesaler_id=wholesaler_id,
        selected_limit=limit,
        page=page,
        total_pages=total_pages,
        total=total,
    )


@product_prep_bp.route("/product-prep/download")
@login_required
def download_xlsx():
    import os
    import subprocess
    import openpyxl
    from pathlib import Path
    from datetime import datetime
    from openpyxl.styles import Font, PatternFill, Alignment

    wholesaler_id = request.args.get("wholesaler_id", type=int)
    q = MasterProduct.query.filter(
        MasterProduct.is_prep_ready == False,
        MasterProduct.current_status == "active",
    )
    if wholesaler_id:
        q = q.filter(MasterProduct.wholesaler_id == wholesaler_id)
    products = q.order_by(MasterProduct.id.desc()).all()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "상품가공"

    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")

    h1 = ws.cell(row=1, column=1, value="원본 상품명*")
    h1.font = Font(bold=True, color="FFFFFF")
    h1.fill = header_fill
    h1.alignment = Alignment(horizontal="center")

    h2 = ws.cell(row=1, column=2, value="상세 설명*\nhtml 코드 그대로 삽입")
    h2.font = Font(bold=True, color="FFFFFF")
    h2.fill = header_fill
    h2.alignment = Alignment(horizontal="center", wrap_text=True)
    ws.row_dimensions[1].height = 30

    for row, m in enumerate(products, 2):
        ws.cell(row=row, column=1, value=m.product_name or "")
        ws.cell(row=row, column=2, value=m.detail_description or "")

    ws.column_dimensions["A"].width = 50
    ws.column_dimensions["B"].width = 80

    # 저장 폴더 결정 (env 설정 없으면 바탕화면\상품가공)
    raw_dir = os.getenv("PRODUCT_PREP_DOWNLOAD_DIR")
    if raw_dir:
        save_dir = Path(raw_dir)
    else:
        save_dir = Path.home() / "Desktop" / "상품가공"
    save_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"product_prep_{timestamp}.xlsx"
    save_path = save_dir / filename
    wb.save(str(save_path))

    # 저장 완료 후 탐색기로 폴더 열기
    try:
        subprocess.Popen(["explorer", str(save_dir)])
    except Exception:
        pass

    return jsonify({"success": True, "path": str(save_path), "filename": filename})


@product_prep_bp.route("/product-prep/upload", methods=["POST"])
@login_required
def upload_xlsx():
    import openpyxl

    f = request.files.get("file")
    if not f:
        return jsonify({"error": "파일 없음"}), 400

    try:
        wb = openpyxl.load_workbook(io.BytesIO(f.read()), data_only=True)
        ws = wb.active
    except Exception as e:
        return jsonify({"error": f"xlsx 파일 읽기 실패: {e}"}), 400

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return jsonify({"error": "빈 파일"}), 400

    # 컬럼 순서: 상품명 / 이미지url / 가공된상품명 / 네이버카테고리번호
    header = [str(h).strip() if h else "" for h in rows[0]]
    try:
        col_name     = header.index("상품명")
        col_edited   = header.index("가공된상품명")
        col_category = header.index("네이버카테고리번호")
    except ValueError as e:
        return jsonify({"error": f"필수 컬럼 없음: {e} — 헤더: {header}"}), 400

    updated = 0
    skipped = 0
    errors = []
    incomplete = []

    for row in rows[1:]:
        def cell(idx, r=row):
            v = r[idx] if idx < len(r) else None
            return str(v).strip() if v is not None else ""

        product_name = cell(col_name)
        edited_name  = cell(col_edited)
        category_id  = cell(col_category)

        if not product_name:
            skipped += 1
            continue

        master = MasterProduct.query.filter_by(product_name=product_name).first()
        if not master:
            errors.append(f"DB에 없음: {product_name[:40]}")
            continue

        if edited_name and category_id:
            # 조건 충족 → 저장
            master.edited_name = edited_name
            master.category_id = category_id
            master.is_prep_ready = True
            updated += 1
        elif edited_name and not category_id:
            # 카테고리 없음 → 저장 안 함
            incomplete.append(f"카테고리 없음: {product_name[:40]}")
        elif category_id and not edited_name:
            # 상품명 없음 → 저장 안 함
            incomplete.append(f"가공된상품명 없음: {product_name[:40]}")
        else:
            skipped += 1

    db.session.commit()
    return jsonify({
        "updated": updated,
        "skipped": skipped,
        "incomplete": incomplete[:30],
        "errors": errors[:20],
    })
