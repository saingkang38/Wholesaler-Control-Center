from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from datetime import datetime
from app.infrastructure import db
from app.store.models import StoreProduct, NaverStore
from app.master.models import MasterProduct

store_bp = Blueprint("store", __name__)


def sync_store_products(wholesaler_id: int) -> dict:
    """모든 활성 NaverStore를 순회하며 상품 동기화"""
    from store.naver import get_all_products

    stores = NaverStore.query.filter_by(is_active=True).all()
    total_stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    for naver_store in stores:
        stats = _sync_single_store(naver_store, wholesaler_id)
        for k in total_stats:
            total_stats[k] += stats[k]

    print(f"[store] 전체 동기화 완료: {total_stats}")
    return total_stats


def _sync_single_store(naver_store: NaverStore, wholesaler_id: int) -> dict:
    from store.naver import get_all_products

    raw_items = get_all_products(
        client_id=naver_store.client_id,
        client_secret=naver_store.client_secret,
    )
    stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    for item in raw_items:
        origin_no = item.get("originProductNo")
        channel_no = item.get("channelProductNo")
        seller_code = item.get("sellerManagementCode", "").strip()
        name = item.get("name", "")
        status = item.get("statusType", "")
        price = item.get("salePrice")

        if not origin_no:
            continue

        store = StoreProduct.query.filter_by(
            naver_store_id=naver_store.id,
            origin_product_no=origin_no,
        ).first()

        if store:
            store.store_status = status
            store.sale_price = price
            store.product_name = name
            store.seller_management_code = seller_code
            store.last_synced_at = datetime.utcnow()
            stats["updated"] += 1
        else:
            store = StoreProduct(
                naver_store_id=naver_store.id,
                origin_product_no=origin_no,
                channel_product_no=channel_no,
                seller_management_code=seller_code,
                product_name=name,
                store_status=status,
                sale_price=price,
                last_synced_at=datetime.utcnow(),
            )
            db.session.add(store)
            db.session.flush()
            stats["created"] += 1

        # 마스터 매칭 (판매자관리코드 = 도매처 상품코드)
        if seller_code:
            master = MasterProduct.query.filter_by(
                wholesaler_id=wholesaler_id,
                supplier_product_code=seller_code
            ).first()
            if master:
                store.master_product_id = master.id
                stats["matched"] += 1
            else:
                stats["unmatched"] += 1

    db.session.commit()
    print(f"[store] {naver_store.store_name} 동기화: {stats}")
    return stats


# ── 관리 페이지 ──────────────────────────────────────────

@store_bp.route("/stores")
@login_required
def stores_page():
    stores = NaverStore.query.order_by(NaverStore.created_at).all()
    return render_template("stores.html", stores=stores)


@store_bp.route("/stores/add", methods=["POST"])
@login_required
def add_store():
    store_name = request.form.get("store_name", "").strip()
    client_id = request.form.get("client_id", "").strip()
    client_secret = request.form.get("client_secret", "").strip()

    if not store_name or not client_id or not client_secret:
        flash("모든 항목을 입력해주세요.", "error")
        return redirect(url_for("store.stores_page"))

    db.session.add(NaverStore(
        store_name=store_name,
        client_id=client_id,
        client_secret=client_secret,
    ))
    db.session.commit()
    flash(f"'{store_name}' 스토어가 추가됐습니다.", "success")
    return redirect(url_for("store.stores_page"))


@store_bp.route("/stores/<int:store_id>/toggle", methods=["POST"])
@login_required
def toggle_store(store_id):
    store = NaverStore.query.get_or_404(store_id)
    store.is_active = not store.is_active
    db.session.commit()
    return redirect(url_for("store.stores_page"))


@store_bp.route("/stores/<int:store_id>/delete", methods=["POST"])
@login_required
def delete_store(store_id):
    store = NaverStore.query.get_or_404(store_id)
    db.session.delete(store)
    db.session.commit()
    flash(f"'{store.store_name}' 스토어가 삭제됐습니다.", "success")
    return redirect(url_for("store.stores_page"))
