from flask import Blueprint, render_template, request
from flask_login import login_required
from app.normalization.models import NormalizedProduct
from app.wholesalers.models import Wholesaler

products_bp = Blueprint("products", __name__)

@products_bp.route("/products")
@login_required
def index():
    page = request.args.get("page", 1, type=int)
    keyword = request.args.get("q", "").strip()
    status = request.args.get("status", "")

    query = NormalizedProduct.query

    if keyword:
        query = query.filter(
            NormalizedProduct.product_name.contains(keyword) |
            NormalizedProduct.source_product_code.contains(keyword)
        )
    if status:
        query = query.filter(NormalizedProduct.status == status)

    pagination = query.order_by(NormalizedProduct.updated_at.desc()).paginate(page=page, per_page=50, error_out=False)

    return render_template("products.html", pagination=pagination, keyword=keyword, status=status)
