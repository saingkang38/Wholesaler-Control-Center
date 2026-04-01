from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from app.infrastructure import db
from app.settings.models import MarginRule, PrepSetting, SmartStoreSetting
from app.store.models import NaverStore

settings_bp = Blueprint("settings", __name__)

# 세션 분리 오류 방지: ORM 객체 대신 튜플 (price_from, price_to, margin_rate) 캐시
_margin_rules_cache = None  # List[tuple] or None


def _invalidate_margin_cache():
    global _margin_rules_cache
    _margin_rules_cache = None


def get_margin_rules():
    """마진 설정 페이지 표시용 — 항상 현재 세션에서 신선하게 조회"""
    return MarginRule.query.order_by(MarginRule.price_from).all()


def _get_margin_tuples():
    """apply_margin 내부용 — 세션 분리 위험 없는 튜플 캐시"""
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


@settings_bp.route("/settings/prep")
@login_required
def prep_setting_page():
    try:
        setting = PrepSetting.get()
    except Exception as e:
        flash(f"설정을 불러오지 못했습니다 (앱 재시작 필요): {e}", "error")
        from types import SimpleNamespace
        from pathlib import Path
        base = Path.home() / "Desktop" / "상품가공"
        setting = SimpleNamespace(
            excel_dir=str(base), image_dir=str(base / "이미지"),
            processed_image_dir=str(base / "가공이미지"),
            side_panel_url="https://namingfactory.ai.kr",
            img_inner_scale=100, img_rotation=0,
            img_output_size=None, img_quality=100,
        )
    return render_template("prep_settings.html", setting=setting)


@settings_bp.route("/settings/prep/save", methods=["POST"])
@login_required
def save_prep_setting():
    try:
        setting = PrepSetting.get()

        def _val(key, current):
            v = request.form.get(key, "").strip()
            return v if v else current

        setting.excel_dir           = _val("excel_dir", setting.excel_dir)
        setting.image_dir           = _val("image_dir", setting.image_dir)
        setting.processed_image_dir = _val("processed_image_dir", setting.processed_image_dir)
        v = request.form.get("side_panel_url", "").strip()
        if v:
            setting.side_panel_url = v

        try:
            setting.img_inner_scale = int(request.form.get("img_inner_scale", 100))
        except (ValueError, TypeError):
            pass
        try:
            setting.img_rotation = int(request.form.get("img_rotation", 0))
        except (ValueError, TypeError):
            pass
        v = request.form.get("img_output_size", "").strip()
        setting.img_output_size = int(v) if v else None
        try:
            setting.img_quality = max(1, min(100, int(request.form.get("img_quality", 100))))
        except (ValueError, TypeError):
            pass

        db.session.commit()
        flash("저장됐습니다.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"저장 실패 — 앱을 재시작해주세요: {e}", "error")

    return redirect(url_for("settings.prep_setting_page"))


@settings_bp.route("/api/pick-folder", methods=["POST"])
@login_required
def pick_folder():
    """서버 측 tkinter 폴더 선택 다이얼로그"""
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.wm_attributes("-topmost", True)
        initial = request.get_json(silent=True) or {}
        folder = filedialog.askdirectory(
            parent=root,
            initialdir=initial.get("initial_dir", ""),
            title="폴더 선택",
        )
        root.destroy()
        return jsonify({"path": folder or None})
    except Exception as e:
        return jsonify({"error": str(e), "path": None}), 500


@settings_bp.route("/settings/smartstore")
@login_required
def smartstore_setting_page():
    setting = SmartStoreSetting.get()
    stores = NaverStore.query.filter_by(is_active=True).all()
    return render_template("smartstore_settings.html", setting=setting, stores=stores)


@settings_bp.route("/settings/smartstore/save", methods=["POST"])
@login_required
def save_smartstore_setting():
    s = SmartStoreSetting.get()
    def _int(key, default):
        try:
            return int(request.form.get(key, default))
        except (ValueError, TypeError):
            return default

    s.delivery_method        = request.form.get("delivery_method", s.delivery_method)
    s.delivery_fee_type      = request.form.get("delivery_fee_type", s.delivery_fee_type)
    s.delivery_fee           = _int("delivery_fee", s.delivery_fee)
    s.free_condition_amount  = _int("free_condition_amount", s.free_condition_amount)
    s.delivery_fee_pay_type  = request.form.get("delivery_fee_pay_type", s.delivery_fee_pay_type)
    s.return_fee             = _int("return_fee", s.return_fee)
    s.exchange_fee           = _int("exchange_fee", s.exchange_fee)
    s.dispatch_days          = _int("dispatch_days", s.dispatch_days)
    s.return_location_name   = request.form.get("return_location_name", "")
    s.return_zip             = request.form.get("return_zip", "")
    s.return_address         = request.form.get("return_address", "")
    s.return_address_detail  = request.form.get("return_address_detail", "")
    s.departure_location_name  = request.form.get("departure_location_name", "")
    s.departure_zip            = request.form.get("departure_zip", "")
    s.departure_address        = request.form.get("departure_address", "")
    s.departure_address_detail = request.form.get("departure_address_detail", "")
    s.delivery_template_code = request.form.get("delivery_template_code", "") or None
    s.delivery_template_name = request.form.get("delivery_template_name", "") or None
    s.as_phone = request.form.get("as_phone", "")
    s.as_guide = request.form.get("as_guide", "")
    db.session.commit()
    flash("저장됐습니다.", "success")
    return redirect(url_for("settings.smartstore_setting_page"))


@settings_bp.route("/api/naver/return-locations")
@login_required
def api_return_locations():
    store_id = request.args.get("store_id", type=int)
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.seller import get_return_locations
        data = get_return_locations(client_id=store.client_id, client_secret=store.client_secret)
        return jsonify({"ok": True, "locations": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@settings_bp.route("/api/naver/departure-locations")
@login_required
def api_departure_locations():
    store_id = request.args.get("store_id", type=int)
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.seller import get_departure_locations
        data = get_departure_locations(client_id=store.client_id, client_secret=store.client_secret)
        return jsonify({"ok": True, "locations": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@settings_bp.route("/api/naver/delivery-templates")
@login_required
def api_delivery_templates():
    store_id = request.args.get("store_id", type=int)
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.seller import get_delivery_templates
        data = get_delivery_templates(client_id=store.client_id, client_secret=store.client_secret)
        return jsonify({"ok": True, "templates": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
