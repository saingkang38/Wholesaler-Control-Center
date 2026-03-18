from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_required
from app.infrastructure import db
from app.suppliers.models import SupplierCode
from app.wholesalers.models import Wholesaler

suppliers_bp = Blueprint("suppliers", __name__)

@suppliers_bp.route("/suppliers")
@login_required
def index():
    wholesaler = Wholesaler.query.filter_by(code="ownerclan").first()
    suppliers = SupplierCode.query.filter_by(wholesaler_id=wholesaler.id).order_by(SupplierCode.created_at.desc()).all()
    return render_template("suppliers.html", suppliers=suppliers)

@suppliers_bp.route("/suppliers/add", methods=["POST"])
@login_required
def add():
    wholesaler = Wholesaler.query.filter_by(code="ownerclan").first()
    supplier_code = request.form.get("supplier_code", "").strip()
    supplier_name = request.form.get("supplier_name", "").strip()

    if supplier_code:
        existing = SupplierCode.query.filter_by(
            wholesaler_id=wholesaler.id,
            supplier_code=supplier_code
        ).first()
        if not existing:
            s = SupplierCode(
                wholesaler_id=wholesaler.id,
                supplier_code=supplier_code,
                supplier_name=supplier_name or None
            )
            db.session.add(s)
            db.session.commit()

    return redirect(url_for("suppliers.index"))

@suppliers_bp.route("/suppliers/delete/<int:supplier_id>", methods=["POST"])
@login_required
def delete(supplier_id):
    s = SupplierCode.query.get_or_404(supplier_id)
    db.session.delete(s)
    db.session.commit()
    return redirect(url_for("suppliers.index"))

@suppliers_bp.route("/suppliers/toggle/<int:supplier_id>", methods=["POST"])
@login_required
def toggle(supplier_id):
    s = SupplierCode.query.get_or_404(supplier_id)
    s.is_active = not s.is_active
    db.session.commit()
    return redirect(url_for("suppliers.index"))