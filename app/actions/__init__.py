import json
import logging
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from app.utils import kst_now
from sqlalchemy.orm import joinedload
from app.infrastructure import db
from app.actions.models import ActionSignal
from app.master.models import MasterProduct
from app.store.models import StoreProduct

actions_bp = Blueprint("actions", __name__)
logger = logging.getLogger(__name__)


SIGNAL_LABELS = {
    "PRICE_UP_NEEDED":     {"label": "가격 인상 필요", "badge": "danger"},
    "PRICE_DOWN_POSSIBLE": {"label": "가격 인하 가능", "badge": "info"},
    "SUSPEND_NEEDED":      {"label": "판매 중지 필요", "badge": "warning"},
    "RESUME_POSSIBLE":     {"label": "판매 재개 가능", "badge": "success"},
    "DISCONTINUE_NEEDED":  {"label": "단종 처리 필요", "badge": "dark"},
    "OPTION_PRICE_CHANGE": {"label": "옵션가 변동", "badge": "warning"},
    "OPTION_STOCK_CHANGE": {"label": "옵션 재고 변동", "badge": "secondary"},
}


@actions_bp.route("/actions")
@login_required
def actions_page():
    status_filter = request.args.get("status", "pending")
    per_page = request.args.get("per_page", 50, type=int)
    page = request.args.get("page", 1, type=int)

    valid_per_page = [30, 50, 100, 300, 500, 1000, 0]
    if per_page not in valid_per_page:
        per_page = 50

    store_filter = request.args.get("store_id", 0, type=int)
    signal_type_filter = request.args.get("signal_type", "")

    from app.store.models import StoreProduct, NaverStore
    query = ActionSignal.query.filter_by(status=status_filter)
    if store_filter:
        sub = db.session.query(StoreProduct.id).filter_by(naver_store_id=store_filter).subquery()
        query = query.filter(ActionSignal.store_product_id.in_(sub))
    if signal_type_filter == "PRICE":
        query = query.filter(ActionSignal.signal_type.in_(["PRICE_UP_NEEDED", "PRICE_DOWN_POSSIBLE"]))
    elif signal_type_filter:
        query = query.filter(ActionSignal.signal_type == signal_type_filter)
    query = query.order_by(ActionSignal.detected_at.desc())

    all_stores = NaverStore.query.order_by(NaverStore.store_name).all()

    if per_page == 0:
        signals = query.all()
        pagination = None
        total = len(signals)
    else:
        pagination = query.paginate(page=page, per_page=per_page, error_out=False)
        signals = pagination.items
        total = pagination.total

    from app.settings import apply_margin

    def _apply_margin_cached(price):
        return apply_margin(price) if price else None

    rows = []
    for s in signals:
        current = json.loads(s.current_value) if s.current_value else {}
        suggested = json.loads(s.suggested_value) if s.suggested_value else {}
        s_price = suggested.get("sale_price")
        wholesale_price = s_price if s_price is not None else current.get("sale_price")
        sale_price = current.get("sale_price")
        margin_price = _apply_margin_cached(wholesale_price) if wholesale_price else None
        rows.append({
            "id": s.id,
            "store_product_id": s.store_product_id,
            "signal_type": s.signal_type,
            "label": SIGNAL_LABELS.get(s.signal_type, {}).get("label", s.signal_type),
            "badge": SIGNAL_LABELS.get(s.signal_type, {}).get("badge", "secondary"),
            "wholesaler_name": s.master.wholesaler.name if s.master and s.master.wholesaler else "-",
            "store_name": s.store.naver_store.store_name if s.store and s.store.naver_store else "-",
            "product_name": s.master.product_name if s.master else "-",
            "seller_code": s.store.seller_management_code if s.store else "-",
            "wholesale_price": wholesale_price,
            "margin_price": margin_price,
            "sale_price": sale_price,
            "detected_at": s.detected_at.strftime("%Y-%m-%d %H:%M") if s.detected_at else "-",
            "status": s.status,
            "error_message": s.error_message,
        })
    pending_count = ActionSignal.query.filter_by(status="pending").count()
    failed_count = ActionSignal.query.filter_by(status="failed").count()
    return render_template("actions.html", rows=rows, status_filter=status_filter,
                           pending_count=pending_count, failed_count=failed_count,
                           pagination=pagination,
                           per_page=per_page, total=total,
                           all_stores=all_stores, store_filter=store_filter,
                           signal_type_filter=signal_type_filter)


@actions_bp.route("/exclusions")
@login_required
def exclusions_page():
    from app.store.models import ProductExclusion
    exclusions = ProductExclusion.query.order_by(ProductExclusion.created_at.desc()).all()
    return render_template("exclusions.html", exclusions=exclusions)


@actions_bp.route("/exclusions/add", methods=["POST"])
@login_required
def add_exclusion():
    from app.store.models import StoreProduct, ProductExclusion
    store_product_id = request.json.get("store_product_id")
    reason = request.json.get("reason", "")
    store = StoreProduct.query.get_or_404(store_product_id)
    if store.exclusion:
        return jsonify({"ok": True})  # 이미 예외 등록됨
    db.session.add(ProductExclusion(store_product_id=store_product_id, reason=reason))
    # 기존 pending 시그널 스킵
    ActionSignal.query.filter_by(store_product_id=store_product_id, status="pending").update({"status": "skipped"})
    db.session.commit()
    return jsonify({"ok": True})


@actions_bp.route("/exclusions/<int:exclusion_id>/delete", methods=["POST"])
@login_required
def delete_exclusion(exclusion_id):
    from app.store.models import ProductExclusion
    exc = ProductExclusion.query.get_or_404(exclusion_id)
    db.session.delete(exc)
    db.session.commit()
    return jsonify({"ok": True})


@actions_bp.route("/actions/bulk-resolve", methods=["POST"])
@login_required
def bulk_resolve():
    import time
    ids = request.json.get("ids", [])
    action = request.json.get("action")  # approve / reject / skip

    ok_count = 0
    fail_count = 0

    for signal_id in ids:
        signal = ActionSignal.query.get(signal_id)
        if not signal or signal.status != "pending":
            continue
        if action == "approve":
            _execute_signal(signal)
            if signal.status == "executed":
                ok_count += 1
            else:
                fail_count += 1
            time.sleep(0.3)  # Naver API rate limit 방지
        elif action == "reject":
            signal.status = "rejected"
            signal.resolved_at = kst_now()
            db.session.commit()
            ok_count += 1
        elif action == "skip":
            signal.status = "skipped"
            signal.resolved_at = kst_now()
            db.session.commit()
            ok_count += 1

    return jsonify({"ok": True, "ok_count": ok_count, "fail_count": fail_count})


@actions_bp.route("/actions/<int:signal_id>/resolve", methods=["POST"])
@login_required
def resolve_signal(signal_id):
    action = request.json.get("action")  # approve / reject / skip
    signal = ActionSignal.query.get_or_404(signal_id)

    try:
        if action == "approve":
            _execute_signal(signal)
        elif action == "reject":
            signal.status = "rejected"
            signal.resolved_at = kst_now()
            db.session.commit()
        elif action == "skip":
            signal.status = "skipped"
            signal.resolved_at = kst_now()
            db.session.commit()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True})


@actions_bp.route("/actions/bulk-retry", methods=["POST"])
@login_required
def bulk_retry():
    ids = request.json.get("ids", [])
    for signal_id in ids:
        signal = ActionSignal.query.get(signal_id)
        if signal and signal.status == "failed":
            signal.status = "pending"
            signal.error_message = None
            signal.resolved_at = None
    db.session.commit()
    return jsonify({"ok": True})


@actions_bp.route("/actions/<int:signal_id>/retry", methods=["POST"])
@login_required
def retry_signal(signal_id):
    signal = ActionSignal.query.get_or_404(signal_id)
    if signal.status != "failed":
        return jsonify({"ok": False, "error": "실패 상태 항목만 재시도할 수 있습니다."}), 400
    signal.status = "pending"
    signal.error_message = None
    signal.resolved_at = None
    db.session.commit()
    return jsonify({"ok": True})


@actions_bp.route("/actions/<int:signal_id>/revert", methods=["POST"])
@login_required
def revert_signal(signal_id):
    signal = ActionSignal.query.get_or_404(signal_id)
    if signal.status != "executed":
        return jsonify({"ok": False, "error": "실행된 항목만 되돌릴 수 있습니다."}), 400
    try:
        _revert_signal(signal)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True})


def _revert_signal(signal: ActionSignal):
    from store.naver import update_price, change_status

    store = signal.store
    current = json.loads(signal.current_value) if signal.current_value else {}

    if not store or not store.naver_store:
        raise ValueError("스토어 정보 없음")

    client_id = store.naver_store.client_id
    client_secret = store.naver_store.client_secret

    if signal.signal_type in ("PRICE_UP_NEEDED", "PRICE_DOWN_POSSIBLE"):
        orig_price = current.get("sale_price")
        if orig_price:
            update_price(store.origin_product_no, int(orig_price), client_id=client_id, client_secret=client_secret)
            store.sale_price = orig_price

    elif signal.signal_type in ("SUSPEND_NEEDED", "RESUME_POSSIBLE", "DISCONTINUE_NEEDED"):
        orig_status = current.get("store_status")
        if orig_status:
            change_status(store.origin_product_no, orig_status, client_id=client_id, client_secret=client_secret)
            store.store_status = orig_status

    elif signal.signal_type in ("OPTION_PRICE_CHANGE", "OPTION_STOCK_CHANGE"):
        raise ValueError("옵션 변동은 되돌리기를 지원하지 않습니다. 직접 수동으로 수정해주세요.")

    signal.status = "reverted"
    signal.resolved_at = kst_now()
    db.session.commit()


def _parse_naver_error(e) -> str:
    """Naver API HTTPError에서 사람이 읽을 수 있는 오류 메시지 추출"""
    try:
        import requests as req_lib
        if isinstance(e, req_lib.HTTPError) and e.response is not None:
            data = e.response.json()
            invalid = data.get("invalidInputs") or []
            if invalid:
                return " / ".join(i.get("message", "") for i in invalid if i.get("message"))
            return data.get("message") or str(e)
    except Exception:
        pass
    return str(e)


def _execute_signal(signal: ActionSignal):
    from store.naver import update_price, change_status

    try:
        store = signal.store
        suggested = json.loads(signal.suggested_value) if signal.suggested_value else {}

        if not store or not store.naver_store:
            raise ValueError("스토어 정보 없음")

        client_id = store.naver_store.client_id
        client_secret = store.naver_store.client_secret

        if signal.signal_type in ("PRICE_UP_NEEDED", "PRICE_DOWN_POSSIBLE"):
            wholesale_price = suggested.get("sale_price")
            if wholesale_price and wholesale_price > 0:
                from app.settings import apply_margin
                new_price = apply_margin(wholesale_price)
                update_price(store.origin_product_no, new_price, client_id=client_id, client_secret=client_secret)
                store.sale_price = new_price

        elif signal.signal_type in ("SUSPEND_NEEDED", "RESUME_POSSIBLE", "DISCONTINUE_NEEDED"):
            new_status = suggested.get("store_status")
            if new_status:
                change_status(store.origin_product_no, new_status, client_id=client_id, client_secret=client_secret)
                store.store_status = new_status

        elif signal.signal_type == "OPTION_STOCK_CHANGE":
            from store.naver.products import get_origin_product, update_origin_product

            options_text = suggested.get("options_text", "")
            option_stocks_text = suggested.get("option_stocks", "")

            option_names = [n.strip() for n in options_text.split("\n") if n.strip()]
            option_stocks = []
            for s in option_stocks_text.split("\n"):
                try:
                    option_stocks.append(int(s.strip()))
                except ValueError:
                    option_stocks.append(999)

            product_data = get_origin_product(store.origin_product_no, client_id, client_secret)
            origin = product_data.get("originProduct", {})
            option_info = origin.get("detailAttribute", {}).get("optionInfo", {})
            combinations = option_info.get("optionCombinations", [])

            if not combinations:
                raise ValueError("스토어 상품에 옵션 없음")

            for i, combo in enumerate(combinations):
                name = combo.get("optionName1") or combo.get("optionName2") or ""
                matched_idx = None
                for j, opt_name in enumerate(option_names):
                    if opt_name == name:
                        matched_idx = j
                        break
                if matched_idx is None and i < len(option_stocks):
                    matched_idx = i
                if matched_idx is not None and matched_idx < len(option_stocks):
                    combo["stockQuantity"] = max(0, option_stocks[matched_idx])

            option_info["optionCombinations"] = combinations
            origin.setdefault("detailAttribute", {})["optionInfo"] = option_info
            update_origin_product(store.origin_product_no, {"originProduct": origin}, client_id, client_secret)

        elif signal.signal_type == "OPTION_PRICE_CHANGE":
            from store.naver.products import get_origin_product, update_origin_product

            list_price    = suggested.get("list_price")
            discount      = suggested.get("discount", 0)
            additions     = suggested.get("additions", [])
            options_text  = suggested.get("options_text", "")

            if not list_price or not additions:
                raise ValueError("옵션 가격 데이터 부족 (시그널 재감지 필요)")

            option_names = [n.strip() for n in options_text.split("\n") if n.strip()]

            # 네이버 상품 전체 조회
            product_data = get_origin_product(store.origin_product_no, client_id, client_secret)
            origin = product_data.get("originProduct", {})
            option_info = origin.get("detailAttribute", {}).get("optionInfo", {})
            combinations = option_info.get("optionCombinations", [])

            if not combinations:
                raise ValueError("스토어 상품에 옵션 없음")

            # 옵션 추가금 매칭 (이름 우선, 없으면 순서)
            for i, combo in enumerate(combinations):
                name = combo.get("optionName1") or combo.get("optionName2") or ""
                matched_idx = None
                for j, opt_name in enumerate(option_names):
                    if opt_name == name:
                        matched_idx = j
                        break
                if matched_idx is None and i < len(additions):
                    matched_idx = i
                if matched_idx is not None and matched_idx < len(additions):
                    combo["price"] = additions[matched_idx]   # 음수 허용 (조건 충족 보장됨)

            option_info["optionCombinations"] = combinations
            origin.setdefault("detailAttribute", {})["optionInfo"] = option_info

            # 정가 설정
            origin["salePrice"] = list_price

            # 즉시할인 설정 (0이면 필드 제거)
            if discount > 0:
                origin["immediateDiscountPolicy"] = {"immediateDiscountAmount": discount}
            else:
                origin.pop("immediateDiscountPolicy", None)

            update_origin_product(store.origin_product_no, {"originProduct": origin}, client_id, client_secret)

            # DB에 적용 가격 기록
            store.option_list_price = list_price
            store.option_discount_amount = discount if discount > 0 else None

        signal.status = "executed"
        signal.error_message = None
        signal.resolved_at = kst_now()
        db.session.commit()

    except Exception as e:
        db.session.rollback()
        # 실패 상태로 저장 (pending에서 사라지고 실패 탭으로 이동)
        signal.status = "failed"
        signal.error_message = _parse_naver_error(e)
        signal.resolved_at = kst_now()
        db.session.commit()


def detect_action_signals(wholesaler_id: int) -> dict:
    """
    마스터 상품 vs 스토어 상품 비교 → ActionSignal 생성
    매 실행마다 기존 pending 시그널을 지우고 현재 상태로 새로 감지 (중복/충돌 방지)
    """
    stats = {
        "PRICE_UP_NEEDED": 0,
        "PRICE_DOWN_POSSIBLE": 0,
        "SUSPEND_NEEDED": 0,
        "RESUME_POSSIBLE": 0,
        "DISCONTINUE_NEEDED": 0,
        "OPTION_PRICE_CHANGE": 0,
        "OPTION_STOCK_CHANGE": 0,
    }

    # 해당 도매처의 매칭된 스토어 상품만 조회 — 전체 로드 방지, 관계 미리 로드
    stores = (
        StoreProduct.query
        .filter(StoreProduct.master_product_id.isnot(None))
        .join(MasterProduct, StoreProduct.master_product_id == MasterProduct.id)
        .filter(MasterProduct.wholesaler_id == wholesaler_id)
        .options(joinedload(StoreProduct.master), joinedload(StoreProduct.exclusion))
        .all()
    )

    store_ids = [s.id for s in stores]

    # 기존 pending 시그널 전부 삭제 → 현재 상태로 새로 감지 (중복/충돌 원천 차단)
    # SQLite 파라미터 한계(999) 대비 500개씩 청크 처리
    CHUNK = 500
    for i in range(0, len(store_ids), CHUNK):
        chunk = store_ids[i:i + CHUNK]
        ActionSignal.query.filter(
            ActionSignal.store_product_id.in_(chunk),
            ActionSignal.status == "pending",
        ).delete(synchronize_session=False)
    if store_ids:
        db.session.flush()

    existing_pending = set()  # 삭제 후이므로 항상 빈 셋

    for store in stores:
        master = store.master

        if not master:
            continue

        if store.exclusion:
            continue

        _check_price_signals(master, store, stats, existing_pending)
        _check_status_signals(master, store, stats, existing_pending)
        _check_option_signals(master, store, stats, existing_pending)
        _check_option_stock_signals(master, store, stats, existing_pending)

    db.session.commit()
    logger.info(f"[actions] 시그널 감지 완료: {stats}")
    return stats


def _check_price_signals(master: MasterProduct, store: StoreProduct, stats: dict, pending: set):
    if not master.price or not store.sale_price:
        return

    from app.settings import apply_margin
    margin_price = apply_margin(master.price)  # 마진 적용 기준가

    if margin_price > store.sale_price:
        if (master.id, store.id, "PRICE_UP_NEEDED") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="PRICE_UP_NEEDED",
                current_value=json.dumps({"sale_price": store.sale_price}),
                suggested_value=json.dumps({"sale_price": master.price}),  # 도매가 저장, 실행 시 마진 재적용
            ))
            pending.add((master.id, store.id, "PRICE_UP_NEEDED"))
            stats["PRICE_UP_NEEDED"] += 1

    elif margin_price < store.sale_price:
        if (master.id, store.id, "PRICE_DOWN_POSSIBLE") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="PRICE_DOWN_POSSIBLE",
                current_value=json.dumps({"sale_price": store.sale_price}),
                suggested_value=json.dumps({"sale_price": master.price}),  # 도매가 저장, 실행 시 마진 재적용
            ))
            pending.add((master.id, store.id, "PRICE_DOWN_POSSIBLE"))
            stats["PRICE_DOWN_POSSIBLE"] += 1


def _check_option_signals(master: MasterProduct, store: StoreProduct, stats: dict, pending: set):
    if not master.option_diffs or not master.options_text:
        return
    if not store.origin_product_no:
        return

    # 마지막 실행된 OPTION_PRICE_CHANGE 시그널의 suggested option_diffs와 비교
    last_executed = (
        ActionSignal.query
        .filter_by(store_product_id=store.id, signal_type="OPTION_PRICE_CHANGE")
        .filter(ActionSignal.status.in_(["executed", "reverted"]))
        .order_by(ActionSignal.resolved_at.desc())
        .first()
    )
    if last_executed:
        last_suggested = json.loads(last_executed.suggested_value or "{}")
        if last_suggested.get("option_diffs") == master.option_diffs:
            return  # 이미 최신 옵션가로 적용됨

    if (master.id, store.id, "OPTION_PRICE_CHANGE") not in pending:
        from app.settings import calculate_option_pricing
        pricing = calculate_option_pricing(master.price, master.option_diffs)
        db.session.add(ActionSignal(
            master_product_id=master.id,
            store_product_id=store.id,
            signal_type="OPTION_PRICE_CHANGE",
            current_value=json.dumps({
                "options_text": master.options_text,
                "store_list_price": store.option_list_price,
                "store_discount": store.option_discount_amount,
            }),
            suggested_value=json.dumps({
                "base_price":   master.price,
                "option_diffs": master.option_diffs,
                "options_text": master.options_text,
                "list_price":   pricing["list_price"],
                "discount":     pricing["discount"],
                "sale_price":   pricing["sale_price"],
                "additions":    pricing["additions"],
            }),
        ))
        pending.add((master.id, store.id, "OPTION_PRICE_CHANGE"))
        stats["OPTION_PRICE_CHANGE"] += 1


def _check_option_stock_signals(master: MasterProduct, store: StoreProduct, stats: dict, pending: set):
    """옵션 재고 변동 감지 — 수집기가 extra["옵션재고"] 제공 시 동작"""
    if not master.options_text or master.option_stocks is None:
        return
    if not store.origin_product_no:
        return

    last_executed = (
        ActionSignal.query
        .filter_by(store_product_id=store.id, signal_type="OPTION_STOCK_CHANGE")
        .filter(ActionSignal.status.in_(["executed", "reverted"]))
        .order_by(ActionSignal.resolved_at.desc())
        .first()
    )
    if last_executed:
        last_suggested = json.loads(last_executed.suggested_value or "{}")
        if last_suggested.get("option_stocks") == master.option_stocks:
            return

    if (master.id, store.id, "OPTION_STOCK_CHANGE") not in pending:
        db.session.add(ActionSignal(
            master_product_id=master.id,
            store_product_id=store.id,
            signal_type="OPTION_STOCK_CHANGE",
            current_value=json.dumps({"option_stocks": master.option_stocks}),
            suggested_value=json.dumps({
                "option_stocks": master.option_stocks,
                "options_text": master.options_text,
            }),
        ))
        pending.add((master.id, store.id, "OPTION_STOCK_CHANGE"))
        stats["OPTION_STOCK_CHANGE"] += 1


def _check_status_signals(master: MasterProduct, store: StoreProduct, stats: dict, pending: set):
    store_active = store.store_status == "SALE"
    master_status = master.current_status

    if master_status == "discontinued":
        if store_active and (master.id, store.id, "DISCONTINUE_NEEDED") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="DISCONTINUE_NEEDED",
                current_value=json.dumps({"store_status": store.store_status}),
                suggested_value=json.dumps({"store_status": "CLOSE"}),
            ))
            pending.add((master.id, store.id, "DISCONTINUE_NEEDED"))
            stats["DISCONTINUE_NEEDED"] += 1

    elif master_status in ("missing", "discontinued_candidate"):
        if store_active and (master.id, store.id, "SUSPEND_NEEDED") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="SUSPEND_NEEDED",
                current_value=json.dumps({"store_status": store.store_status}),
                suggested_value=json.dumps({"store_status": "SUSPENSION"}),
            ))
            pending.add((master.id, store.id, "SUSPEND_NEEDED"))
            stats["SUSPEND_NEEDED"] += 1

    elif master_status == "out_of_stock":
        # 옵션 없는 상품이 명시적으로 품절 → 상품 전체 중지
        if store_active and not master.options_text and (master.id, store.id, "SUSPEND_NEEDED") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="SUSPEND_NEEDED",
                current_value=json.dumps({"store_status": store.store_status}),
                suggested_value=json.dumps({"store_status": "SUSPENSION"}),
            ))
            pending.add((master.id, store.id, "SUSPEND_NEEDED"))
            stats["SUSPEND_NEEDED"] += 1

    elif master_status == "active":
        # CLOSE(판매종료)는 API로 복구 불가 → 제외
        resumable = not store_active and store.store_status != "CLOSE"
        if resumable and (master.id, store.id, "RESUME_POSSIBLE") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="RESUME_POSSIBLE",
                current_value=json.dumps({"store_status": store.store_status}),
                suggested_value=json.dumps({"store_status": "SALE"}),
            ))
            pending.add((master.id, store.id, "RESUME_POSSIBLE"))
            stats["RESUME_POSSIBLE"] += 1
