import json
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from datetime import datetime
from app.infrastructure import db
from app.actions.models import ActionSignal
from app.master.models import MasterProduct
from app.store.models import StoreProduct

actions_bp = Blueprint("actions", __name__)


SIGNAL_LABELS = {
    "PRICE_UP_NEEDED":    {"label": "가격 인상 필요", "badge": "danger"},
    "PRICE_DOWN_POSSIBLE": {"label": "가격 인하 가능", "badge": "info"},
    "SUSPEND_NEEDED":     {"label": "판매 중지 필요", "badge": "warning"},
    "RESUME_POSSIBLE":    {"label": "판매 재개 가능", "badge": "success"},
    "DISCONTINUE_NEEDED": {"label": "단종 처리 필요", "badge": "dark"},
}


@actions_bp.route("/actions")
@login_required
def actions_page():
    status_filter = request.args.get("status", "pending")
    signals = (
        ActionSignal.query
        .filter_by(status=status_filter)
        .order_by(ActionSignal.detected_at.desc())
        .all()
    )
    rows = []
    for s in signals:
        current = json.loads(s.current_value) if s.current_value else {}
        suggested = json.loads(s.suggested_value) if s.suggested_value else {}
        rows.append({
            "id": s.id,
            "signal_type": s.signal_type,
            "label": SIGNAL_LABELS.get(s.signal_type, {}).get("label", s.signal_type),
            "badge": SIGNAL_LABELS.get(s.signal_type, {}).get("badge", "secondary"),
            "product_name": s.master.product_name if s.master else "-",
            "seller_code": s.store.seller_management_code if s.store else "-",
            "current": current,
            "suggested": suggested,
            "detected_at": s.detected_at.strftime("%Y-%m-%d %H:%M") if s.detected_at else "-",
            "status": s.status,
        })
    pending_count = ActionSignal.query.filter_by(status="pending").count()
    return render_template("actions.html", rows=rows, status_filter=status_filter, pending_count=pending_count)


@actions_bp.route("/actions/<int:signal_id>/resolve", methods=["POST"])
@login_required
def resolve_signal(signal_id):
    action = request.json.get("action")  # approve / reject / skip
    signal = ActionSignal.query.get_or_404(signal_id)

    if action == "approve":
        _execute_signal(signal)
    elif action == "reject":
        signal.status = "rejected"
        signal.resolved_at = datetime.utcnow()
        db.session.commit()
    elif action == "skip":
        signal.status = "skipped"
        signal.resolved_at = datetime.utcnow()
        db.session.commit()

    return jsonify({"ok": True})


def _execute_signal(signal: ActionSignal):
    from store.naver import update_price, change_status

    try:
        store = signal.store
        suggested = json.loads(signal.suggested_value) if signal.suggested_value else {}

        if signal.signal_type in ("PRICE_UP_NEEDED", "PRICE_DOWN_POSSIBLE"):
            new_price = suggested.get("sale_price")
            if new_price and store:
                update_price(store.origin_product_no, new_price)
                store.sale_price = new_price

        elif signal.signal_type in ("SUSPEND_NEEDED", "RESUME_POSSIBLE", "DISCONTINUE_NEEDED"):
            new_status = suggested.get("store_status")
            if new_status and store:
                change_status(store.origin_product_no, new_status)
                store.store_status = new_status

        signal.status = "executed"
        signal.resolved_at = datetime.utcnow()
        db.session.commit()

    except Exception as e:
        signal.status = "pending"
        db.session.commit()
        raise e


def detect_action_signals(wholesaler_id: int) -> dict:
    """
    마스터 상품 vs 스토어 상품 비교 → ActionSignal 생성
    이미 pending 상태인 동일 시그널은 중복 생성하지 않음
    """
    stats = {
        "PRICE_UP_NEEDED": 0,
        "PRICE_DOWN_POSSIBLE": 0,
        "SUSPEND_NEEDED": 0,
        "RESUME_POSSIBLE": 0,
        "DISCONTINUE_NEEDED": 0,
    }

    # 매칭된 스토어 상품만 처리
    stores = (
        StoreProduct.query
        .filter(StoreProduct.master_product_id.isnot(None))
        .all()
    )

    for store in stores:
        master = store.master

        if not master or master.wholesaler_id != wholesaler_id:
            continue

        _check_price_signals(master, store, stats)
        _check_status_signals(master, store, stats)

    db.session.commit()
    print(f"[actions] 시그널 감지 완료: {stats}")
    return stats


def _already_pending(master_id, store_id, signal_type) -> bool:
    return ActionSignal.query.filter_by(
        master_product_id=master_id,
        store_product_id=store_id,
        signal_type=signal_type,
        status="pending",
    ).first() is not None


def _check_price_signals(master: MasterProduct, store: StoreProduct, stats: dict):
    if not master.price or not store.sale_price:
        return

    if master.price > store.sale_price:
        if not _already_pending(master.id, store.id, "PRICE_UP_NEEDED"):
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="PRICE_UP_NEEDED",
                current_value=json.dumps({"sale_price": store.sale_price}),
                suggested_value=json.dumps({"sale_price": master.price}),
            ))
            stats["PRICE_UP_NEEDED"] += 1

    elif master.price < store.sale_price:
        if not _already_pending(master.id, store.id, "PRICE_DOWN_POSSIBLE"):
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="PRICE_DOWN_POSSIBLE",
                current_value=json.dumps({"sale_price": store.sale_price}),
                suggested_value=json.dumps({"sale_price": master.price}),
            ))
            stats["PRICE_DOWN_POSSIBLE"] += 1


def _check_status_signals(master: MasterProduct, store: StoreProduct, stats: dict):
    store_active = store.store_status == "SALE"
    master_status = master.current_status

    if master_status == "discontinued":
        if store_active and not _already_pending(master.id, store.id, "DISCONTINUE_NEEDED"):
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="DISCONTINUE_NEEDED",
                current_value=json.dumps({"store_status": store.store_status}),
                suggested_value=json.dumps({"store_status": "CLOSE"}),
            ))
            stats["DISCONTINUE_NEEDED"] += 1

    elif master_status in ("missing", "discontinued_candidate"):
        if store_active and not _already_pending(master.id, store.id, "SUSPEND_NEEDED"):
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="SUSPEND_NEEDED",
                current_value=json.dumps({"store_status": store.store_status}),
                suggested_value=json.dumps({"store_status": "SUSPENSION"}),
            ))
            stats["SUSPEND_NEEDED"] += 1

    elif master_status == "active":
        if not store_active and not _already_pending(master.id, store.id, "RESUME_POSSIBLE"):
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="RESUME_POSSIBLE",
                current_value=json.dumps({"store_status": store.store_status}),
                suggested_value=json.dumps({"store_status": "SALE"}),
            ))
            stats["RESUME_POSSIBLE"] += 1
