import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from datetime import datetime
from app.infrastructure import db
from app.store.models import StoreProduct, NaverStore
from app.master.models import MasterProduct
from app.wholesalers.models import Wholesaler

store_bp = Blueprint("store", __name__)
logger = logging.getLogger(__name__)


def _get_prefixes() -> list:
    """도매처 prefix 목록 — 여러 함수에서 공통 사용"""
    return [w.prefix for w in Wholesaler.query.filter(Wholesaler.prefix.isnot(None)).all()]


def _build_master_map(seller_codes: set, prefixes: list) -> dict:
    """seller_code → MasterProduct 매핑 딕셔너리 — 배치 IN 쿼리로 N+1 + SQLite 변수 제한 방지"""
    if not seller_codes:
        return {}
    all_candidates = set(seller_codes)
    for code in seller_codes:
        for p in prefixes:
            if p:
                all_candidates.add(f"{p}{code}")
    candidates_list = list(all_candidates)
    masters = []
    for i in range(0, len(candidates_list), 500):
        chunk = candidates_list[i:i + 500]
        masters.extend(
            MasterProduct.query.filter(
                MasterProduct.supplier_product_code.in_(chunk)
            ).all()
        )
    return {m.supplier_product_code: m for m in masters}


def _lookup_master(seller_code: str, master_map: dict, prefixes: list):
    """master_map에서 seller_code 또는 prefix+code 로 마스터 조회"""
    if master_map.get(seller_code):
        return master_map[seller_code]
    for p in prefixes:
        if p and master_map.get(f"{p}{seller_code}"):
            return master_map[f"{p}{seller_code}"]
    return None


def sync_store_products() -> dict:
    """모든 활성 NaverStore를 순회하며 상품 동기화"""
    stores = NaverStore.query.filter_by(is_active=True).all()
    total_stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    for naver_store in stores:
        stats = _sync_single_store(naver_store)
        for k in total_stats:
            total_stats[k] += stats[k]

    logger.info(f"[store] 전체 동기화 완료: {total_stats}")
    return total_stats


def _sync_single_store(naver_store: NaverStore) -> dict:
    from store.naver import get_all_products

    raw_items = get_all_products(
        client_id=naver_store.client_id,
        client_secret=naver_store.client_secret,
    )
    stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    prefixes = _get_prefixes()

    # 배치 준비: 전체 원본번호 / seller_code 수집
    parsed = []
    for item in raw_items:
        origin_no = item.get("originProductNo")
        if origin_no is None or origin_no == "":
            continue
        channel_products = item.get("channelProducts") or []
        channel = channel_products[0] if channel_products else None
        parsed.append({
            "origin_no": origin_no,
            "channel_no": channel.get("channelProductNo") if channel else None,
            "seller_code": (channel.get("sellerManagementCode") or "").strip() if channel else "",
            "name": channel.get("name", "") if channel else "",
            "status": channel.get("statusType", "") if channel else "",
            "price": channel.get("salePrice") if channel else None,
        })

    # 기존 StoreProduct 배치 로드 (SQLite 변수 제한 방지)
    all_origin_nos = [p["origin_no"] for p in parsed]
    existing_list = []
    for i in range(0, len(all_origin_nos), 500):
        chunk = all_origin_nos[i:i + 500]
        existing_list.extend(
            StoreProduct.query.filter(
                StoreProduct.naver_store_id == naver_store.id,
                StoreProduct.origin_product_no.in_(chunk),
            ).all()
        )
    existing_stores = {s.origin_product_no: s for s in existing_list}

    # MasterProduct 한 번에 로드 (N+1 방지)
    all_seller_codes = {p["seller_code"] for p in parsed if p["seller_code"]}
    master_map = _build_master_map(all_seller_codes, prefixes)

    for p in parsed:
        origin_no = p["origin_no"]
        seller_code = p["seller_code"]

        store = existing_stores.get(origin_no)
        if store:
            store.store_status = p["status"]
            store.sale_price = p["price"]
            store.product_name = p["name"]
            store.seller_management_code = seller_code
            store.last_synced_at = datetime.utcnow()
            stats["updated"] += 1
        else:
            store = StoreProduct(
                naver_store_id=naver_store.id,
                origin_product_no=origin_no,
                channel_product_no=p["channel_no"],
                seller_management_code=seller_code,
                product_name=p["name"],
                store_status=p["status"],
                sale_price=p["price"],
                last_synced_at=datetime.utcnow(),
            )
            db.session.add(store)
            db.session.flush()
            stats["created"] += 1

        if seller_code:
            master = _lookup_master(seller_code, master_map, prefixes)
            if master:
                store.master_product_id = master.id
                stats["matched"] += 1
            else:
                stats["unmatched"] += 1

    db.session.commit()
    logger.info(f"[store] {naver_store.store_name} 동기화: {stats}")
    return stats


def _rematch_by_codes(naver_store_id: int, codes: list) -> dict:
    """네이버 API 호출 없이 입력한 판매자관리코드만 재매칭"""
    prefixes = _get_prefixes()
    clean_codes = {c.strip() for c in codes if c.strip()}
    master_map = _build_master_map(clean_codes, prefixes)
    stats = {"matched": 0, "unmatched": 0, "not_found": 0}

    try:
        for code in clean_codes:
            store = StoreProduct.query.filter_by(
                naver_store_id=naver_store_id,
                seller_management_code=code,
            ).first()
            if not store:
                stats["not_found"] += 1
                continue
            master = _lookup_master(code, master_map, prefixes)
            if master:
                store.master_product_id = master.id
                stats["matched"] += 1
            else:
                stats["unmatched"] += 1

        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return stats


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

    prefixes = _get_prefixes()
    all_codes = {sp.seller_management_code.strip() for sp in unmatched if sp.seller_management_code}
    master_map = _build_master_map(all_codes, prefixes)

    # wholesaler_id 필터가 있으면 map에서 해당 도매처 상품만 남김
    if wholesaler_id:
        master_map = {k: v for k, v in master_map.items() if v.wholesaler_id == wholesaler_id}

    results = []
    for sp in unmatched:
        code = sp.seller_management_code.strip()
        master = _lookup_master(code, master_map, prefixes)

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
    success_codes = []  # API 호출 성공한 코드만 추적

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
            success_codes.append(code)
            success_count += 1
        except Exception as e:
            logger.warning(f"[push_codes] origin={origin_no} 실패: {e}")
            fail_count += 1

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"[push_codes] 커밋 실패: {e}")
        raise

    # API 호출 성공한 코드만 재매칭
    if success_codes:
        _rematch_by_codes(naver_store.id, success_codes)

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
