from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from app.infrastructure import db
from app.settings.models import MarginRule

settings_bp = Blueprint("settings", __name__)

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
