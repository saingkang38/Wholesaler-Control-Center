from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from app.infrastructure import db
from app.settings.models import MarginRule
from app.store.models import NaverStore

settings_bp = Blueprint("settings", __name__)


def get_margin_rules():
    return MarginRule.query.order_by(MarginRule.price_from).all()


def apply_margin(wholesale_price: int) -> int:
    """도매가에 마진율 적용 후 10원 단위 반올림"""
    if not wholesale_price:
        return 0
    rules = get_margin_rules()
    for rule in rules:
        if wholesale_price >= rule.price_from:
            if rule.price_to is None or wholesale_price <= rule.price_to:
                applied = wholesale_price * (1 + rule.margin_rate)
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
    flash("마진 규칙이 추가됐습니다.", "success")
    return redirect(url_for("settings.margin_page"))


@settings_bp.route("/settings/margin/<int:rule_id>/delete", methods=["POST"])
@login_required
def delete_margin_rule(rule_id):
    rule = MarginRule.query.get_or_404(rule_id)
    db.session.delete(rule)
    db.session.commit()
    flash("삭제됐습니다.", "success")
    return redirect(url_for("settings.margin_page"))


@settings_bp.route("/settings/seller-account")
@login_required
def seller_account():
    store_id = request.args.get("store_id", type=int)
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.seller import get_seller_account
        data = get_seller_account(client_id=store.client_id, client_secret=store.client_secret)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@settings_bp.route("/settings/categories")
@login_required
def search_categories():
    store_id = request.args.get("store_id", type=int)
    category_id = request.args.get("category_id", "root")
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.seller import get_categories
        data = get_categories(category_id, client_id=store.client_id, client_secret=store.client_secret)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@settings_bp.route("/settings/inspection-status")
@login_required
def inspection_status():
    store_id = request.args.get("store_id", type=int)
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.seller import get_inspection_status
        data = get_inspection_status(client_id=store.client_id, client_secret=store.client_secret)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
