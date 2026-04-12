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
    "OPTION_ADD":          {"label": "옵션 추가/변경", "badge": "primary"},
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
    option_type_filter = request.args.get("option_type", "no_option")
    search_q = request.args.get("q", "").strip()

    from app.store.models import StoreProduct, NaverStore
    query = ActionSignal.query.filter_by(status=status_filter)
    if store_filter:
        sub = db.session.query(StoreProduct.id).filter_by(naver_store_id=store_filter).subquery()
        query = query.filter(ActionSignal.store_product_id.in_(sub))
    if signal_type_filter == "PRICE":
        query = query.filter(ActionSignal.signal_type.in_(["PRICE_UP_NEEDED", "PRICE_DOWN_POSSIBLE"]))
    elif signal_type_filter == "OPTION":
        query = query.filter(ActionSignal.signal_type.in_(["OPTION_PRICE_CHANGE", "OPTION_ADD", "OPTION_STOCK_CHANGE"]))
    elif signal_type_filter:
        query = query.filter(ActionSignal.signal_type == signal_type_filter)
    if option_type_filter:
        query = query.join(MasterProduct, ActionSignal.master_product_id == MasterProduct.id)
        if option_type_filter == "no_option":
            query = query.filter(
                db.or_(MasterProduct.options_text == None, MasterProduct.options_text == "")
            )
        elif option_type_filter == "option_no_extra":
            query = query.filter(
                MasterProduct.options_text != None,
                MasterProduct.options_text != "",
                db.or_(MasterProduct.option_diffs == None, MasterProduct.option_diffs == ""),
            )
        elif option_type_filter == "option_with_extra":
            query = query.filter(
                MasterProduct.options_text != None,
                MasterProduct.options_text != "",
                MasterProduct.option_diffs != None,
                MasterProduct.option_diffs != "",
            )
    if search_q:
        sp_sub = db.session.query(StoreProduct.id).filter(
            db.or_(
                StoreProduct.product_name.ilike(f"%{search_q}%"),
                StoreProduct.seller_management_code.ilike(f"%{search_q}%"),
            )
        ).subquery()
        query = query.filter(ActionSignal.store_product_id.in_(sp_sub))
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

        if s.signal_type == "OPTION_PRICE_CHANGE":
            # 도매가격=도매기준가 / 마진적용=실판매가 / 판매가격=정가(설정판매가)
            wholesale_price = suggested.get("base_price")
            margin_price    = suggested.get("sale_price")      # apply_margin(base_price) 이미 계산됨
            sale_price      = suggested.get("list_price")      # 조건 충족 정가
            discount        = suggested.get("discount", 0)
            option_count    = len(suggested.get("additions", []))
        elif s.signal_type == "OPTION_STOCK_CHANGE":
            wholesale_price = None
            margin_price    = None
            sale_price      = None
            discount        = 0
            option_count    = None
        else:
            s_price         = suggested.get("sale_price")
            wholesale_price = s_price if s_price is not None else current.get("sale_price")
            sale_price      = current.get("sale_price")
            margin_price    = _apply_margin_cached(wholesale_price) if wholesale_price else None
            discount        = 0
            option_count    = None

        # PRICE 시그널 + 옵션 상품인 경우: 승인 시 적용될 옵션별 가격 계산
        option_details = []
        if s.signal_type in ("PRICE_UP_NEEDED", "PRICE_DOWN_POSSIBLE") and wholesale_price:
            master = s.master
            if master and master.option_diffs and master.options_text:
                try:
                    from app.settings import calculate_option_pricing
                    opt_p = calculate_option_pricing(wholesale_price, master.option_diffs)
                    names = [n.strip() for n in master.options_text.split("\n") if n.strip()]
                    adds  = opt_p["additions"]
                    base  = opt_p["sale_price"]
                    option_details = [
                        (names[i] if i < len(names) else f"옵션{i+1}", base + (adds[i] if i < len(adds) else 0))
                        for i in range(len(names))
                    ]
                except Exception:
                    pass

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
            "discount": discount,
            "option_count": option_count,
            "option_details": option_details,
            "detected_at": s.detected_at.strftime("%Y-%m-%d %H:%M") if s.detected_at else "-",
            "status": s.status,
            "error_message": s.error_message,
        })
    pending_count = ActionSignal.query.filter_by(status="pending").count()
    failed_count = ActionSignal.query.filter_by(status="failed").count()

    def _option_type_count(otype):
        base = ActionSignal.query.filter_by(status="pending").join(
            MasterProduct, ActionSignal.master_product_id == MasterProduct.id
        )
        if otype == "no_option":
            return base.filter(
                db.or_(MasterProduct.options_text == None, MasterProduct.options_text == "")
            ).count()
        elif otype == "option_no_extra":
            return base.filter(
                MasterProduct.options_text != None,
                MasterProduct.options_text != "",
                db.or_(MasterProduct.option_diffs == None, MasterProduct.option_diffs == ""),
            ).count()
        elif otype == "option_with_extra":
            return base.filter(
                MasterProduct.options_text != None,
                MasterProduct.options_text != "",
                MasterProduct.option_diffs != None,
                MasterProduct.option_diffs != "",
            ).count()

    no_option_count = _option_type_count("no_option")
    option_no_extra_count = _option_type_count("option_no_extra")
    option_with_extra_count = _option_type_count("option_with_extra")

    return render_template("actions.html", rows=rows, status_filter=status_filter,
                           pending_count=pending_count, failed_count=failed_count,
                           pagination=pagination,
                           per_page=per_page, total=total,
                           all_stores=all_stores, store_filter=store_filter,
                           signal_type_filter=signal_type_filter,
                           option_type_filter=option_type_filter,
                           no_option_count=no_option_count,
                           option_no_extra_count=option_no_extra_count,
                           option_with_extra_count=option_with_extra_count,
                           search_q=search_q)


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
            # 실행 후 실제 상태 확인 — 내부 오류로 failed가 됐어도 감지
            if signal.status == "failed":
                return jsonify({"ok": False, "error": signal.error_message or "실행 실패"}), 200
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


# ---------------------------------------------------------------------------
# 옵션 유형 판별 헬퍼
# ---------------------------------------------------------------------------

def _has_options(master) -> bool:
    """도매처 마스터에 옵션이 있는 상품인지 판별"""
    return bool(master and master.options_text and master.options_text.strip())


def _has_extra_price(master) -> bool:
    """옵션 추가금(0 이외 차액)이 실제로 존재하는지 판별"""
    if not master or not master.option_diffs or not master.option_diffs.strip():
        return False
    try:
        return any(int(v.strip()) != 0 for v in master.option_diffs.split("\n") if v.strip())
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# [유형 1] 옵션 없는 상품 — 가격 실행
# ---------------------------------------------------------------------------

def _execute_price_no_option(store, suggested: dict, client_id: str, client_secret: str):
    """
    옵션이 없는 상품의 가격 변동 반영.
    sale_price 단일 업데이트만 수행한다.
    """
    from store.naver import update_price
    from app.settings import apply_margin

    wholesale_price = suggested.get("sale_price")
    if not wholesale_price or wholesale_price <= 0:
        raise ValueError("도매가 정보 없음")

    new_price = apply_margin(wholesale_price)
    update_price(store.origin_product_no, new_price, client_id=client_id, client_secret=client_secret)
    store.sale_price = new_price
    logger.info(f"[actions][no_option] 가격 반영: store_id={store.id}, price={new_price}")


# ---------------------------------------------------------------------------
# [유형 2] 옵션 있음·추가금 없음 — 가격 실행
# ---------------------------------------------------------------------------

def _execute_price_option_no_extra(store, master, suggested: dict, client_id: str, client_secret: str):
    """
    옵션은 있지만 옵션간 가격 차이가 없는 상품의 가격 변동 반영.
    모든 옵션 combination.price = 0, salePrice = new_price 로 업데이트한다.
    """
    from store.naver.products import get_origin_product, update_origin_product
    from app.settings import apply_margin

    wholesale_price = suggested.get("sale_price")
    if not wholesale_price or wholesale_price <= 0:
        raise ValueError("도매가 정보 없음")

    new_price = apply_margin(wholesale_price)

    product_data = get_origin_product(store.origin_product_no, client_id, client_secret)
    origin = product_data.get("originProduct", {})
    option_info = origin.get("detailAttribute", {}).get("optionInfo", {})
    combinations = option_info.get("optionCombinations", [])

    if not combinations:
        raise ValueError("스토어 상품에 옵션 없음 (option_no_extra 경로)")

    for combo in combinations:
        combo["price"] = 0  # 추가금 없음 — 전부 0원

    option_info["optionCombinations"] = combinations
    origin.setdefault("detailAttribute", {})["optionInfo"] = option_info
    origin["salePrice"] = new_price
    origin["customerBenefit"] = {}  # 즉시할인 없음

    payload = {
        "originProduct": origin,
        "smartstoreChannelProduct": product_data.get("smartstoreChannelProduct", {}),
    }
    update_origin_product(store.origin_product_no, payload, client_id, client_secret)

    store.sale_price = new_price
    store.option_list_price = new_price
    store.option_discount_amount = None
    logger.info(f"[actions][option_no_extra] 가격 반영: store_id={store.id}, price={new_price}")


# ---------------------------------------------------------------------------
# [유형 3] 옵션 있음·추가금 있음 — 가격 실행
# ---------------------------------------------------------------------------

def _execute_price_option_with_extra(store, master, suggested: dict, client_id: str, client_secret: str, signal: "ActionSignal"):
    """
    옵션도 있고 옵션간 추가금 차이도 있는 상품의 가격 변동 반영.
    정가 + 즉시할인 + 옵션추가금을 세트로 업데이트한다.
    처리 후 같은 상품의 pending OPTION_PRICE_CHANGE 시그널을 자동 스킵한다.
    """
    from store.naver.products import get_origin_product, update_origin_product
    from app.settings import apply_margin, calculate_option_pricing

    wholesale_price = suggested.get("sale_price")
    if not wholesale_price or wholesale_price <= 0:
        raise ValueError("도매가 정보 없음")

    new_price = apply_margin(wholesale_price)
    pricing = calculate_option_pricing(wholesale_price, master.option_diffs)
    option_names = [n.strip() for n in master.options_text.split("\n") if n.strip()]

    product_data = get_origin_product(store.origin_product_no, client_id, client_secret)
    origin = product_data.get("originProduct", {})
    option_info = origin.get("detailAttribute", {}).get("optionInfo", {})
    combinations = option_info.get("optionCombinations", [])

    if not combinations:
        raise ValueError("스토어 상품에 옵션 없음 (option_with_extra 경로)")

    for i, combo in enumerate(combinations):
        name = combo.get("optionName1") or combo.get("optionName2") or ""
        matched_idx = next((j for j, n in enumerate(option_names) if n == name), None)
        if matched_idx is None and i < len(pricing["additions"]):
            logger.warning(
                f"[actions][option_with_extra] 옵션명 매칭 실패 → 순서 폴백 "
                f"(store_product_id={store.id}, combo_idx={i}, name='{name}')"
            )
            matched_idx = i
        if matched_idx is not None and matched_idx < len(pricing["additions"]):
            combo["price"] = pricing["additions"][matched_idx]

    option_info["optionCombinations"] = combinations
    origin.setdefault("detailAttribute", {})["optionInfo"] = option_info
    origin["salePrice"] = pricing["list_price"]
    if pricing["discount"] > 0:
        origin["customerBenefit"] = {
            "immediateDiscountPolicy": {
                "discountMethod": {"value": pricing["discount"], "unitType": "WON"}
            }
        }
    else:
        origin["customerBenefit"] = {}

    payload = {
        "originProduct": origin,
        "smartstoreChannelProduct": product_data.get("smartstoreChannelProduct", {}),
    }
    update_origin_product(store.origin_product_no, payload, client_id, client_secret)

    store.sale_price = new_price
    store.option_list_price = pricing["list_price"]
    store.option_discount_amount = pricing["discount"] or None
    logger.info(
        f"[actions][option_with_extra] 가격 반영: store_id={store.id}, "
        f"list_price={pricing['list_price']}, discount={pricing['discount']}"
    )

    # 옵션가도 함께 처리됐으므로 pending OPTION_PRICE_CHANGE 자동 스킵
    pending_opt = ActionSignal.query.filter_by(
        store_product_id=store.id,
        signal_type="OPTION_PRICE_CHANGE",
        status="pending",
    ).first()
    if pending_opt:
        pending_opt.status = "skipped"
        pending_opt.error_message = "가격변동 시그널 실행 시 옵션가도 함께 처리됨"
        pending_opt.resolved_at = kst_now()


# ---------------------------------------------------------------------------
# 시그널 실행 디스패처
# ---------------------------------------------------------------------------

def _execute_signal(signal: ActionSignal):
    from store.naver import change_status

    try:
        store = signal.store
        suggested = json.loads(signal.suggested_value) if signal.suggested_value else {}

        if not store or not store.naver_store:
            raise ValueError("스토어 정보 없음")

        client_id = store.naver_store.client_id
        client_secret = store.naver_store.client_secret

        # ── 가격 변동: 옵션 유형별 독립 실행 ──────────────────────────────
        if signal.signal_type in ("PRICE_UP_NEEDED", "PRICE_DOWN_POSSIBLE"):
            master = signal.master
            if not _has_options(master):
                _execute_price_no_option(store, suggested, client_id, client_secret)
            elif not _has_extra_price(master):
                _execute_price_option_no_extra(store, master, suggested, client_id, client_secret)
            else:
                _execute_price_option_with_extra(store, master, suggested, client_id, client_secret, signal)

        # ── 상태 변동: 옵션 유형 무관 ─────────────────────────────────────
        elif signal.signal_type in ("SUSPEND_NEEDED", "RESUME_POSSIBLE", "DISCONTINUE_NEEDED"):
            new_status = suggested.get("store_status")
            if new_status:
                change_status(store.origin_product_no, new_status, client_id=client_id, client_secret=client_secret)
                store.store_status = new_status

        # ── 옵션 재고 변동: 옵션 있는 상품 전용 ──────────────────────────
        elif signal.signal_type == "OPTION_STOCK_CHANGE":
            from store.naver.products import get_origin_product, update_origin_product

            option_names = [n.strip() for n in suggested.get("options_text", "").split("\n") if n.strip()]
            option_stocks = []
            for s in suggested.get("option_stocks", "").split("\n"):
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
                matched_idx = next((j for j, n in enumerate(option_names) if n == name), None)
                if matched_idx is None and i < len(option_stocks):
                    logger.warning(
                        f"[actions][option_stock] 옵션명 매칭 실패 → 순서 폴백 "
                        f"(store_product_id={store.id}, combo_idx={i}, name='{name}')"
                    )
                    matched_idx = i
                if matched_idx is not None and matched_idx < len(option_stocks):
                    combo["stockQuantity"] = max(0, option_stocks[matched_idx])

            option_info["optionCombinations"] = combinations
            origin.setdefault("detailAttribute", {})["optionInfo"] = option_info
            update_origin_product(store.origin_product_no, {"originProduct": origin}, client_id, client_secret)

        # ── 옵션 추가금 변동: 추가금 있는 상품 전용 ──────────────────────
        elif signal.signal_type == "OPTION_PRICE_CHANGE":
            from store.naver.products import get_origin_product, update_origin_product

            list_price   = suggested.get("list_price")
            discount     = suggested.get("discount", 0)
            additions    = suggested.get("additions", [])
            option_names = [n.strip() for n in suggested.get("options_text", "").split("\n") if n.strip()]

            # 구형 시그널 폴백
            if not list_price or not additions:
                from app.settings import calculate_option_pricing
                _base  = suggested.get("base_price")
                _diffs = suggested.get("option_diffs", "")
                if not _base or not _diffs:
                    raise ValueError("옵션 가격 데이터 부족 (base_price/option_diffs 없음)")
                _p = calculate_option_pricing(_base, _diffs)
                list_price = _p["list_price"]
                discount   = _p["discount"]
                additions  = _p["additions"]

            product_data = get_origin_product(store.origin_product_no, client_id, client_secret)
            origin = product_data.get("originProduct", {})
            option_info = origin.get("detailAttribute", {}).get("optionInfo", {})
            combinations = option_info.get("optionCombinations", [])

            if not combinations:
                raise ValueError("스토어 상품에 옵션 없음")

            for i, combo in enumerate(combinations):
                name = combo.get("optionName1") or combo.get("optionName2") or ""
                matched_idx = next((j for j, n in enumerate(option_names) if n == name), None)
                if matched_idx is None and i < len(additions):
                    logger.warning(
                        f"[actions][option_price] 옵션명 매칭 실패 → 순서 폴백 "
                        f"(store_product_id={store.id}, combo_idx={i}, name='{name}')"
                    )
                    matched_idx = i
                if matched_idx is not None and matched_idx < len(additions):
                    combo["price"] = additions[matched_idx]

            option_info["optionCombinations"] = combinations
            origin.setdefault("detailAttribute", {})["optionInfo"] = option_info
            origin["salePrice"] = list_price
            if discount > 0:
                origin["customerBenefit"] = {
                    "immediateDiscountPolicy": {
                        "discountMethod": {"value": discount, "unitType": "WON"}
                    }
                }
            else:
                origin["customerBenefit"] = {}

            payload = {
                "originProduct": origin,
                "smartstoreChannelProduct": product_data.get("smartstoreChannelProduct", {}),
            }
            update_origin_product(store.origin_product_no, payload, client_id, client_secret)
            store.sale_price = list_price - discount
            store.option_list_price = list_price
            store.option_discount_amount = discount if discount > 0 else None

        # ── 옵션 구성 전체 교체: 추가금 있는 상품 전용 ───────────────────
        elif signal.signal_type == "OPTION_ADD":
            from store.naver.products import get_origin_product, update_origin_product
            from app.settings import calculate_option_pricing

            base_price   = suggested.get("base_price")
            option_diffs = suggested.get("option_diffs", "")
            options_text = suggested.get("options_text", "")

            if not base_price or not option_diffs or not options_text:
                raise ValueError("OPTION_ADD: 옵션 데이터 부족")

            master_names = [n.strip() for n in options_text.split("\n") if n.strip()]
            pricing = calculate_option_pricing(base_price, option_diffs)
            additions = pricing["additions"]

            product_data = get_origin_product(store.origin_product_no, client_id, client_secret)
            origin = product_data.get("originProduct", {})
            option_info = origin.get("detailAttribute", {}).get("optionInfo", {})

            new_combos = [
                {
                    "optionName1": name,
                    "price": additions[i] if i < len(additions) else 0,
                    "stockQuantity": 999,
                    "usable": True,
                }
                for i, name in enumerate(master_names)
            ]
            logger.info(f"[actions][option_add] 전체교체: store_id={store.id}, {len(new_combos)}개")

            option_info["optionCombinations"] = new_combos
            origin.setdefault("detailAttribute", {})["optionInfo"] = option_info
            origin["salePrice"] = pricing["list_price"]
            if pricing["discount"] > 0:
                origin["customerBenefit"] = {
                    "immediateDiscountPolicy": {
                        "discountMethod": {"value": pricing["discount"], "unitType": "WON"}
                    }
                }
            else:
                origin["customerBenefit"] = {}

            payload = {
                "originProduct": origin,
                "smartstoreChannelProduct": product_data.get("smartstoreChannelProduct", {}),
            }
            update_origin_product(store.origin_product_no, payload, client_id, client_secret)
            store.sale_price = pricing["sale_price"]
            store.option_list_price = pricing["list_price"]
            store.option_discount_amount = pricing["discount"] if pricing["discount"] > 0 else None

        signal.status = "executed"
        signal.error_message = None
        signal.resolved_at = kst_now()
        db.session.commit()

    except Exception as e:
        db.session.rollback()
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
        "OPTION_ADD": 0,
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
        _check_option_add_signals(master, store, stats, existing_pending)

    db.session.commit()
    logger.info(f"[actions] 시그널 감지 완료: {stats}")
    return stats


def _check_price_signals(master: MasterProduct, store: StoreProduct, stats: dict, pending: set):
    if not master.price or not store.sale_price:
        return

    from app.settings import apply_margin
    margin_price = apply_margin(master.price)  # 마진 적용 기준가

    # 옵션 상품(option_list_price 있음): 정가 - 즉시할인 = 실효가 (sale_price 시점과 무관하게 일관됨)
    # 일반 즉시할인 상품(option_list_price 없음): sale_price(정가) - 즉시할인 = 실효가
    if store.option_list_price:
        effective_price = store.option_list_price - (store.option_discount_amount or 0)
    else:
        effective_price = store.sale_price - (store.option_discount_amount or 0)

    if margin_price > effective_price:
        if (master.id, store.id, "PRICE_UP_NEEDED") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="PRICE_UP_NEEDED",
                current_value=json.dumps({"sale_price": effective_price}),
                suggested_value=json.dumps({"sale_price": master.price}),  # 도매가 저장, 실행 시 마진 재적용
            ))
            pending.add((master.id, store.id, "PRICE_UP_NEEDED"))
            stats["PRICE_UP_NEEDED"] += 1

    elif margin_price < effective_price:
        if (master.id, store.id, "PRICE_DOWN_POSSIBLE") not in pending:
            db.session.add(ActionSignal(
                master_product_id=master.id,
                store_product_id=store.id,
                signal_type="PRICE_DOWN_POSSIBLE",
                current_value=json.dumps({"sale_price": effective_price}),
                suggested_value=json.dumps({"sale_price": master.price}),  # 도매가 저장, 실행 시 마진 재적용
            ))
            pending.add((master.id, store.id, "PRICE_DOWN_POSSIBLE"))
            stats["PRICE_DOWN_POSSIBLE"] += 1


def _check_option_signals(master: MasterProduct, store: StoreProduct, stats: dict, pending: set):
    if not master.option_diffs or not master.options_text:
        return
    if not master.price:
        return
    if not store.origin_product_no:
        return

    # PRICE 시그널이 이미 생성된 경우 OPTION_PRICE_CHANGE 생성 안 함
    # (PRICE 실행 시 옵션추가금 함께 갱신되므로 중복/순서 충돌 방지)
    if (master.id, store.id, "PRICE_UP_NEEDED") in pending or (master.id, store.id, "PRICE_DOWN_POSSIBLE") in pending:
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


def _check_option_add_signals(master: MasterProduct, store: StoreProduct, stats: dict, pending: set):
    """도매처에 새 옵션이 추가되었거나 옵션 구성이 바뀐 경우 감지"""
    if not master.options_text or not master.option_diffs:
        return
    if not master.price:
        return
    if not store.origin_product_no:
        return

    # 마지막 실행된 OPTION_ADD의 options_text와 비교
    last_executed = (
        ActionSignal.query
        .filter_by(store_product_id=store.id, signal_type="OPTION_ADD")
        .filter(ActionSignal.status.in_(["executed", "reverted"]))
        .order_by(ActionSignal.resolved_at.desc())
        .first()
    )
    if last_executed:
        last_suggested = json.loads(last_executed.suggested_value or "{}")
        if last_suggested.get("options_text") == master.options_text:
            return

    # OPTION_PRICE_CHANGE가 같은 options_text로 이미 실행된 경우 → 옵션 구성 변동 없음
    last_price_exec = (
        ActionSignal.query
        .filter_by(store_product_id=store.id, signal_type="OPTION_PRICE_CHANGE")
        .filter(ActionSignal.status.in_(["executed", "reverted"]))
        .order_by(ActionSignal.resolved_at.desc())
        .first()
    )
    if last_price_exec:
        last_p = json.loads(last_price_exec.suggested_value or "{}")
        if last_p.get("options_text") == master.options_text:
            return

    if (master.id, store.id, "OPTION_ADD") not in pending:
        db.session.add(ActionSignal(
            master_product_id=master.id,
            store_product_id=store.id,
            signal_type="OPTION_ADD",
            current_value=json.dumps({"options_text": master.options_text}),
            suggested_value=json.dumps({
                "base_price":   master.price,
                "option_diffs": master.option_diffs,
                "options_text": master.options_text,
            }),
        ))
        pending.add((master.id, store.id, "OPTION_ADD"))
        stats["OPTION_ADD"] += 1


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
