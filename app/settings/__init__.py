import math
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app
from flask_login import login_required
from app.infrastructure import db
from app.settings.models import MarginRule

settings_bp = Blueprint("settings", __name__)

_KST = ZoneInfo("Asia/Seoul")

_sync_option_state_status = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "result": None,
    "error": None,
}
_sync_option_state_lock = threading.Lock()

_margin_rules_cache = None


def _invalidate_margin_cache():
    global _margin_rules_cache
    _margin_rules_cache = None


def get_margin_rules():
    return MarginRule.query.order_by(MarginRule.price_from).all()


def _get_margin_tuples():
    global _margin_rules_cache
    if _margin_rules_cache is None:
        rules = MarginRule.query.order_by(MarginRule.price_from).all()
        _margin_rules_cache = [(r.price_from, r.price_to, r.margin_rate) for r in rules]
    return _margin_rules_cache


def apply_margin(wholesale_price: int) -> int:
    """도매가에 마진율 적용 후 10원 단위 반올림"""
    if not wholesale_price or wholesale_price <= 0:
        return 0
    for price_from, price_to, margin_rate in _get_margin_tuples():
        if wholesale_price >= price_from:
            if price_to is None or wholesale_price <= price_to:
                applied = wholesale_price * (1 + margin_rate)
                return round(applied / 10) * 10
    return wholesale_price


def _get_base_margin_rate(base_price: int) -> float:
    """base_price에 해당하는 마진율 반환 (옵션 차액 계산용)"""
    for price_from, price_to, margin_rate in _get_margin_tuples():
        if base_price >= price_from:
            if price_to is None or base_price <= price_to:
                return margin_rate
    return 0.0


def calculate_option_pricing(base_price: int, option_diffs_text: str) -> dict:
    """
    옵션 상품의 정가/즉시할인/옵션추가금 계산.

    스마트스토어 옵션가 제한:
      정가 <  2,000       → 0 ~ +100%  (마이너스 불가)
      정가  2,000~9,999   → -50% ~ +100%
      정가 ≥ 10,000       → ±50%
    0원 옵션은 option_diffs 중 0이 있으면 자동 충족.

    Returns:
        list_price  : 정가 (스마트스토어 설정 판매가)
        discount    : 즉시할인금액  (0이면 할인 없음)
        sale_price  : 실판매가 = apply_margin(base_price)
        additions   : 각 옵션 추가금 리스트 (int, 음수 가능)
    """
    if not base_price or base_price <= 0:
        return {"list_price": 0, "discount": 0, "sale_price": 0, "additions": []}
    sale_price = apply_margin(base_price)

    diffs = []
    for token in option_diffs_text.split("\n"):
        token = token.strip()
        try:
            diffs.append(int(token))
        except (ValueError, AttributeError):
            pass

    if not diffs:
        return {"list_price": sale_price, "discount": 0, "sale_price": sale_price, "additions": []}

    # 기준가(base_price)의 마진율을 옵션 차액에도 동일하게 적용
    # apply_margin(base_price + d) 방식은 구간 경계 근처에서 역전 발생 가능
    base_rate = _get_base_margin_rate(base_price)
    raw_additions = [round(d * (1 + base_rate) / 10) * 10 for d in diffs]

    # 네이버 필수 조건: 옵션가 0원짜리 1개 이상 — 가장 저렴한 옵션을 기준(0원)으로 정규화
    min_add = min(raw_additions)
    if min_add != 0:
        additions = [a - min_add for a in raw_additions]
        sale_price = sale_price + min_add   # 실판매가 = 가장 저렴한 옵션의 마진 적용가
    else:
        additions = raw_additions

    max_add = max(additions)
    abs_min = abs(min(additions))   # 마이너스 최대 절대값 (양수면 0)
    abs_min = abs_min if min(additions) < 0 else 0

    # ── Case 1: 정가 < 2,000 (마이너스 불가, +100%) ─────────────────
    if abs_min == 0:
        L = _ceil10(max(sale_price, max_add))
        if L < 2000:
            return {"list_price": L, "discount": L - sale_price, "sale_price": sale_price, "additions": additions}

    # ── Case 2: 정가 2,000 ~ 9,999 (-50% ~ +100%) ──────────────────
    L = _ceil10(max(sale_price, max_add, abs_min * 2))
    if 2000 <= L < 10000:
        return {"list_price": L, "discount": L - sale_price, "sale_price": sale_price, "additions": additions}

    # ── Case 3: 정가 ≥ 10,000 (±50%) ──────────────────────────────
    L = _ceil10(max(sale_price, max_add * 2, abs_min * 2))
    L = max(L, 10000)
    return {"list_price": L, "discount": L - sale_price, "sale_price": sale_price, "additions": additions}


def _ceil10(v: float) -> int:
    """10원 단위 올림"""
    return math.ceil(v / 10) * 10


@settings_bp.route("/settings/margin")
@login_required
def margin_page():
    rules = get_margin_rules()
    return render_template("margin_settings.html", rules=rules)


@settings_bp.route("/settings/margin/add", methods=["POST"])
@login_required
def add_margin_rule():
    price_from = request.form.get("price_from", type=int)
    price_to = request.form.get("price_to", type=int) or None
    margin_rate = request.form.get("margin_rate", type=float)

    if price_from is None or margin_rate is None:
        flash("가격과 마진율을 입력해주세요.", "error")
        return redirect(url_for("settings.margin_page"))

    db.session.add(MarginRule(
        price_from=price_from,
        price_to=price_to,
        margin_rate=margin_rate / 100,
    ))
    db.session.commit()
    _invalidate_margin_cache()
    flash("마진 규칙이 추가됐습니다.", "success")
    return redirect(url_for("settings.margin_page"))


@settings_bp.route("/settings/margin/<int:rule_id>/delete", methods=["POST"])
@login_required
def delete_margin_rule(rule_id):
    rule = MarginRule.query.get_or_404(rule_id)
    db.session.delete(rule)
    db.session.commit()
    _invalidate_margin_cache()
    flash("삭제됐습니다.", "success")
    return redirect(url_for("settings.margin_page"))


@settings_bp.route("/settings/option-sync")
@login_required
def option_sync_page():
    return render_template("option_sync_settings.html", status=_sync_option_state_status)


@settings_bp.route("/settings/option-sync/run", methods=["POST"])
@login_required
def run_option_sync():
    with _sync_option_state_lock:
        if _sync_option_state_status["running"]:
            return jsonify({"started": False, "reason": "이미 실행 중입니다."}), 409
        _sync_option_state_status.update({
            "running": True,
            "started_at": datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": None,
            "result": None,
            "error": None,
        })

    flask_app = current_app._get_current_object()

    def _runner():
        try:
            from app.store import sync_store_option_state
            result = sync_store_option_state(flask_app=flask_app)
            with _sync_option_state_lock:
                _sync_option_state_status["result"] = result
        except Exception as e:
            with _sync_option_state_lock:
                _sync_option_state_status["error"] = str(e)
        finally:
            with _sync_option_state_lock:
                _sync_option_state_status["running"] = False
                _sync_option_state_status["finished_at"] = datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S")

    threading.Thread(target=_runner, daemon=True).start()
    return jsonify({"started": True}), 202


@settings_bp.route("/settings/option-sync/status")
@login_required
def option_sync_status():
    with _sync_option_state_lock:
        return jsonify(dict(_sync_option_state_status))
