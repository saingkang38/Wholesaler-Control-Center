from flask import Blueprint, render_template, jsonify
from flask_login import login_required
from datetime import date
from sqlalchemy import func
from app.infrastructure import db
from app.execution_logs.models import CollectionRun
from app.master.models import MasterProduct, ProductEvent
from app.wholesalers.models import Wholesaler

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/")
@dashboard_bp.route("/dashboard")
@login_required
def index():
    today = date.today()

    # 마스터 상품 현황
    total = MasterProduct.query.count()
    missing = MasterProduct.query.filter_by(current_status="missing").count()
    discontinued_candidate = MasterProduct.query.filter_by(current_status="discontinued_candidate").count()
    discontinued = MasterProduct.query.filter_by(current_status="discontinued").count()

    # 오늘 이벤트 수 — GROUP BY 한 번으로 조회
    event_counts = dict(
        db.session.query(ProductEvent.event_type, func.count(ProductEvent.id))
        .filter(ProductEvent.event_date == today)
        .group_by(ProductEvent.event_type)
        .all()
    )
    today_new = event_counts.get("NEW", 0)
    today_restocked = event_counts.get("RESTOCKED", 0)
    today_price_change = event_counts.get("PRICE_CHANGE", 0)
    today_image_change = event_counts.get("IMAGE_CHANGE", 0)
    today_name_change = event_counts.get("NAME_CHANGE", 0)
    today_missing = event_counts.get("MISSING", 0)
    today_discontinued_candidate = event_counts.get("DISCONTINUED_CANDIDATE", 0)

    # 최근 수집 이력
    recent_runs = CollectionRun.query.order_by(CollectionRun.created_at.desc()).limit(10).all()

    return render_template("dashboard.html",
        today=today,
        total=total,
        missing=missing,
        discontinued_candidate=discontinued_candidate,
        discontinued=discontinued,
        today_new=today_new,
        today_restocked=today_restocked,
        today_price_change=today_price_change,
        today_image_change=today_image_change,
        today_name_change=today_name_change,
        today_missing=today_missing,
        today_discontinued_candidate=today_discontinued_candidate,
        recent_runs=recent_runs,
    )


@dashboard_bp.route("/api/dashboard/events/<event_type>")
@login_required
def event_list(event_type):
    today = date.today()
    events = (
        db.session.query(ProductEvent, MasterProduct, Wholesaler)
        .join(MasterProduct, ProductEvent.master_product_id == MasterProduct.id)
        .join(Wholesaler, MasterProduct.wholesaler_id == Wholesaler.id)
        .filter(ProductEvent.event_date == today, ProductEvent.event_type == event_type)
        .order_by(ProductEvent.id.desc())
        .limit(500)
        .all()
    )
    rows = []
    for ev, master, ws in events:
        rows.append({
            "wholesaler": ws.name,
            "code": master.supplier_product_code,
            "name": master.product_name,
            "product_url": master.product_url or "",
            "before": ev.before_value,
            "after": ev.after_value,
        })
    return jsonify({"event_type": event_type, "count": len(rows), "rows": rows})
