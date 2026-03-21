from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from datetime import datetime
from app.infrastructure import db
from app.store.models import StoreProduct, NaverStore
from app.master.models import MasterProduct
from app.wholesalers.models import Wholesaler

store_bp = Blueprint("store", __name__)

from app.store import routes  # noqa: F401, E402


def sync_store_products() -> dict:
    """모든 활성 NaverStore를 순회하며 상품 동기화"""
    stores = NaverStore.query.filter_by(is_active=True).all()
    total_stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    for naver_store in stores:
        stats = _sync_single_store(naver_store)
        for k in total_stats:
            total_stats[k] += stats[k]

    print(f"[store] 전체 동기화 완료: {total_stats}")
    return total_stats


def _sync_single_store(naver_store: NaverStore) -> dict:
    from store.naver import get_all_products

    raw_items = get_all_products(
        client_id=naver_store.client_id,
        client_secret=naver_store.client_secret,
    )
    stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    for item in raw_items:
        origin_no = item.get("originProductNo")
        channel_products = item.get("channelProducts", [])
        channel = channel_products[0] if channel_products else {}
        channel_no = channel.get("channelProductNo")
        seller_code = (channel.get("sellerManagementCode") or "").strip()
        name = channel.get("name", "")
        status = channel.get("statusType", "")
        price = channel.get("salePrice")

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

        # 마스터 매칭 (prefix 포함/미포함 코드 모두 대응)
        if seller_code:
            from app.wholesalers.models import Wholesaler
            from sqlalchemy import or_
            prefixes = [w.prefix for w in Wholesaler.query.filter(
                Wholesaler.prefix.isnot(None)
            ).all()]
            candidates = list({seller_code} | {f"{p}{seller_code}" for p in prefixes if p})
            master = MasterProduct.query.filter(
                MasterProduct.supplier_product_code.in_(candidates)
            ).first()
            if master:
                store.master_product_id = master.id
                stats["matched"] += 1
            else:
                stats["unmatched"] += 1

    db.session.commit()
    print(f"[store] {naver_store.store_name} 동기화: {stats}")
    return stats


def _rematch_by_codes(naver_store_id: int, codes: list) -> dict:
    """네이버 API 호출 없이 입력한 판매자관리코드만 재매칭"""
    from app.wholesalers.models import Wholesaler
    prefixes = [w.prefix for w in Wholesaler.query.filter(
        Wholesaler.prefix.isnot(None)
    ).all()]
    stats = {"matched": 0, "unmatched": 0, "not_found": 0}

    for code in codes:
        code = code.strip()
        if not code:
            continue
        store = StoreProduct.query.filter_by(
            naver_store_id=naver_store_id,
            seller_management_code=code,
        ).first()
        if not store:
            stats["not_found"] += 1
            continue
        candidates = list({code} | {f"{p}{code}" for p in prefixes if p})
        master = MasterProduct.query.filter(
            MasterProduct.supplier_product_code.in_(candidates)
        ).first()
        if master:
            store.master_product_id = master.id
            stats["matched"] += 1
        else:
            stats["unmatched"] += 1

    db.session.commit()
    return stats


def _name_similarity(a: str, b: str) -> float:
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def propose_code_matches(naver_store_id: int, wholesaler_id: int = None, limit: int = 500) -> list:
    """미매칭 StoreProduct 중 seller_management_code가 MasterProduct와 exact match(prefix 포함/미포함)되는 상품 제안"""
    unmatched = StoreProduct.query.filter(
        StoreProduct.naver_store_id == naver_store_id,
        StoreProduct.master_product_id.is_(None),
        StoreProduct.seller_management_code.isnot(None),
        StoreProduct.seller_management_code != "",
    ).limit(limit).all()

    if not unmatched:
        return []

    prefixes = [w.prefix for w in Wholesaler.query.filter(
        Wholesaler.prefix.isnot(None)
    ).all()]

    results = []
    for sp in unmatched:
        code = sp.seller_management_code.strip()
        candidates = list({code} | {f"{p}{code}" for p in prefixes if p})

        master_q = MasterProduct.query.filter(
            MasterProduct.supplier_product_code.in_(candidates)
        )
        if wholesaler_id:
            master_q = master_q.filter_by(wholesaler_id=wholesaler_id)
        master = master_q.first()

        if master:
            results.append({
                "store_product_id": sp.id,
                "origin_product_no": sp.origin_product_no,
                "store_name": sp.product_name,
                "current_code": code,
                "candidate_master_id": master.id,
                "candidate_code": master.supplier_product_code,
                "candidate_name": master.product_name,
            })

    return results


def push_seller_management_codes(naver_store: object, pairs: list) -> dict:
    """pairs: [{origin_product_no, supplier_product_code}, ...]
    Naver API로 sellerManagementCode 업데이트 후 로컬 DB에도 반영, 재매칭 실행
    """
    from store.naver import update_seller_management_code

    success_count = 0
    fail_count = 0

    for pair in pairs:
        origin_no = pair.get("origin_product_no")
        code = pair.get("supplier_product_code", "").strip()
        if not origin_no or not code:
            fail_count += 1
            continue
        try:
            update_seller_management_code(
                origin_product_no=origin_no,
                seller_management_code=code,
                client_id=naver_store.client_id,
                client_secret=naver_store.client_secret,
            )
            sp = StoreProduct.query.filter_by(
                naver_store_id=naver_store.id,
                origin_product_no=origin_no,
            ).first()
            if sp:
                sp.seller_management_code = code
            success_count += 1
        except Exception as e:
            print(f"[push_codes] origin={origin_no} 실패: {e}")
            fail_count += 1

    db.session.commit()

    # 업데이트된 코드들로 재매칭
    updated_codes = [p["supplier_product_code"] for p in pairs if p.get("supplier_product_code")]
    if updated_codes:
        _rematch_by_codes(naver_store.id, updated_codes)

    return {"success_count": success_count, "fail_count": fail_count}


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


@store_bp.route("/stores/<int:store_id>/edit", methods=["POST"])
@login_required
def edit_store(store_id):
    store = NaverStore.query.get_or_404(store_id)
    store_name = request.form.get("store_name", "").strip()
    client_id = request.form.get("client_id", "").strip()
    client_secret = request.form.get("client_secret", "").strip()

    if not store_name or not client_id:
        flash("스토어 이름과 애플리케이션 ID는 필수입니다.", "error")
        return redirect(url_for("store.stores_page"))

    store.store_name = store_name
    store.client_id = client_id
    if client_secret:
        store.client_secret = client_secret
    db.session.commit()
    flash(f"'{store_name}' 스토어가 수정됐습니다.", "success")
    return redirect(url_for("store.stores_page"))
