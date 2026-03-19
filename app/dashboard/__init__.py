from flask import Blueprint, render_template
from flask_login import login_required
from datetime import datetime, date
from app.execution_logs.models import CollectionRun
from app.master.models import MasterProduct, ProductEvent

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/")
@login_required
def index():
    today = date.today()

    # 마스터 상품 현황
    total = MasterProduct.query.count()
    missing = MasterProduct.query.filter_by(current_status="missing").count()
    discontinued_candidate = MasterProduct.query.filter_by(current_status="discontinued_candidate").count()
    discontinued = MasterProduct.query.filter_by(current_status="discontinued").count()

    # 오늘 이벤트 수
    def count_event(event_type):
        return ProductEvent.query.filter_by(event_type=event_type, event_date=today).count()

    today_new = count_event("NEW")
    today_restocked = count_event("RESTOCKED")
    today_price_change = count_event("PRICE_CHANGE")
    today_image_change = count_event("IMAGE_CHANGE")
    today_name_change = count_event("NAME_CHANGE")
    today_missing = count_event("MISSING")
    today_discontinued_candidate = count_event("DISCONTINUED_CANDIDATE")

    # 오늘 이벤트 목록 (최근 50건)
    recent_events = (
        ProductEvent.query
        .filter_by(event_date=today)
        .order_by(ProductEvent.id.desc())
        .limit(50)
        .all()
    )

    # 최근 수집 이력
    recent_runs = CollectionRun.query.order_by(CollectionRun.created_at.desc()).limit(5).all()

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
        recent_events=recent_events,
        recent_runs=recent_runs,
    )
