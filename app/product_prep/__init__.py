import csv
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
def download_csv():
    wholesaler_id = request.args.get("wholesaler_id", type=int)
    q = MasterProduct.query.filter(
        MasterProduct.is_prep_ready == False,
        MasterProduct.current_status == "active",
    )
    if wholesaler_id:
        q = q.filter(MasterProduct.wholesaler_id == wholesaler_id)
    products = q.order_by(MasterProduct.id.desc()).all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "supplier_product_code",
        "wholesaler_name",
        "product_name",
        "supply_price",
        "product_url",
        "edited_name",
        "category_id",
    ])
    for m in products:
        writer.writerow([
            m.supplier_product_code or "",
            m.wholesaler.name if m.wholesaler else "",
            m.product_name or "",
            m.supply_price or "",
            m.product_url or "",
            m.edited_name or "",
            m.category_id or "",
        ])

    csv_bytes = output.getvalue().encode("utf-8-sig")
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=product_prep.csv"},
    )


@product_prep_bp.route("/product-prep/upload", methods=["POST"])
@login_required
def upload_csv():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "파일 없음"}), 400

    content = f.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))

    updated = 0
    skipped = 0
    errors = []

    for row in reader:
        code = (row.get("supplier_product_code") or "").strip()
        wholesaler_name = (row.get("wholesaler_name") or "").strip()
        edited_name = (row.get("edited_name") or "").strip()
        category_id = (row.get("category_id") or "").strip()

        if not code:
            skipped += 1
            continue

        # wholesaler_name 이 있으면 조합으로 찾기, 없으면 코드만
        if wholesaler_name:
            from app.wholesalers.models import Wholesaler
            ws = Wholesaler.query.filter_by(name=wholesaler_name).first()
            if ws:
                master = MasterProduct.query.filter_by(
                    supplier_product_code=code,
                    wholesaler_id=ws.id,
                ).first()
            else:
                master = MasterProduct.query.filter_by(supplier_product_code=code).first()
        else:
            master = MasterProduct.query.filter_by(supplier_product_code=code).first()

        if not master:
            errors.append(f"코드 없음: {code}")
            skipped += 1
            continue

        if edited_name and category_id:
            master.edited_name = edited_name
            master.category_id = category_id
            master.is_prep_ready = True
            updated += 1
        else:
            # 부분 저장 (is_prep_ready는 False 유지)
            if edited_name:
                master.edited_name = edited_name
            if category_id:
                master.category_id = category_id
            skipped += 1

    db.session.commit()
    return jsonify({"updated": updated, "skipped": skipped, "errors": errors[:20]})
