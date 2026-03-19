from flask import render_template, request
from flask_login import login_required
from app.store import store_bp
from app.store.models import StoreProduct, NaverStore


STATUS_LABELS = {
    "WAIT":        "판매대기",
    "SALE":        "판매중",
    "SOLDOUT":     "품절",
    "SUSPENSION":  "판매중지",
    "CLOSE":       "판매종료",
    "PROHIBITION": "판매금지",
}


@store_bp.route("/store-products")
@login_required
def store_products_page():
    naver_store_id = request.args.get("naver_store_id", type=int)
    status_filter = request.args.get("status", "")
    page = request.args.get("page", 1, type=int)
    per_page = 100

    stores = NaverStore.query.order_by(NaverStore.store_name).all()

    query = StoreProduct.query
    if naver_store_id:
        query = query.filter_by(naver_store_id=naver_store_id)
    if status_filter:
        query = query.filter_by(store_status=status_filter)

    # 상태별 카운트
    base_query = StoreProduct.query
    if naver_store_id:
        base_query = base_query.filter_by(naver_store_id=naver_store_id)

    total = base_query.count()
    counts = {}
    for code in STATUS_LABELS:
        counts[code] = base_query.filter_by(store_status=code).count()

    pagination = query.order_by(StoreProduct.id.desc()).paginate(page=page, per_page=per_page, error_out=False)

    return render_template(
        "store_products.html",
        stores=stores,
        selected_store_id=naver_store_id,
        status_filter=status_filter,
        status_labels=STATUS_LABELS,
        total=total,
        counts=counts,
        pagination=pagination,
        products=pagination.items,
    )
