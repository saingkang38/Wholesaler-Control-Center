from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required
from app.store.models import NaverStore

group_products_bp = Blueprint("group_products", __name__)


@group_products_bp.route("/group-products")
@login_required
def group_products_page():
    stores = NaverStore.query.order_by(NaverStore.store_name).all()
    store_id = request.args.get("store_id", type=int)
    group_no = request.args.get("group_no", type=int)

    if not store_id and stores:
        store_id = stores[0].id

    selected_store = NaverStore.query.get(store_id) if store_id else None
    group = None
    error = None

    if selected_store and group_no:
        try:
            from store.naver.group_products import get_group_product
            group = get_group_product(group_no, selected_store.client_id, selected_store.client_secret)
        except Exception as e:
            error = str(e)

    return render_template(
        "group_products.html",
        stores=stores,
        selected_store=selected_store,
        group=group,
        group_no=group_no,
        error=error,
    )


@group_products_bp.route("/group-products/create", methods=["POST"])
@login_required
def create_group_product():
    store_id = request.form.get("store_id", type=int)
    name = request.form.get("name", "").strip()
    product_nos_raw = request.form.get("product_nos", "").strip()

    if not name or not product_nos_raw:
        flash("그룹명과 상품번호를 입력하세요.", "error")
        return redirect(url_for("group_products.group_products_page", store_id=store_id))

    try:
        product_nos = [int(x.strip()) for x in product_nos_raw.splitlines() if x.strip()]
    except ValueError:
        flash("상품번호는 숫자만 입력 가능합니다.", "error")
        return redirect(url_for("group_products.group_products_page", store_id=store_id))

    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.group_products import create_group_product as api_create
        result = api_create(
            {"name": name, "originProductNos": product_nos},
            client_id=store.client_id,
            client_secret=store.client_secret,
        )
        flash(f"그룹상품 등록 완료 (requestId: {result.get('requestId', '-')})", "success")
    except Exception as e:
        flash(f"등록 실패: {e}", "error")
    return redirect(url_for("group_products.group_products_page", store_id=store_id))


@group_products_bp.route("/group-products/<int:group_no>/delete", methods=["POST"])
@login_required
def delete_group_product(group_no):
    store_id = request.form.get("store_id", type=int)
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.group_products import delete_group_product as api_delete
        api_delete(group_no, client_id=store.client_id, client_secret=store.client_secret)
        flash("그룹상품 삭제 완료", "success")
    except Exception as e:
        flash(f"삭제 실패: {e}", "error")
    return redirect(url_for("group_products.group_products_page", store_id=store_id))


@group_products_bp.route("/group-products/<int:group_no>/edit", methods=["POST"])
@login_required
def edit_group_product(group_no):
    store_id = request.form.get("store_id", type=int)
    name = request.form.get("name", "").strip()
    product_nos_raw = request.form.get("product_nos", "").strip()

    store = NaverStore.query.get_or_404(store_id)
    try:
        product_nos = [int(x.strip()) for x in product_nos_raw.splitlines() if x.strip()]
        from store.naver.group_products import update_group_product as api_update
        api_update(group_no, {"name": name, "originProductNos": product_nos},
                   client_id=store.client_id, client_secret=store.client_secret)
        flash("그룹상품 수정 완료", "success")
    except Exception as e:
        flash(f"수정 실패: {e}", "error")
    return redirect(url_for("group_products.group_products_page", store_id=store_id, group_no=group_no))
