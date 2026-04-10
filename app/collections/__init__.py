from flask import Blueprint, jsonify
from flask_login import login_required, current_user
from app.collectors.orchestrator import run_collection

collections_bp = Blueprint("collections", __name__)


@collections_bp.route("/api/collect/<wholesaler_code>", methods=["POST"])
@login_required
def trigger_collection(wholesaler_code):
    from app.execution_logs.models import CollectionRun
    from app.wholesalers.models import Wholesaler

    wholesaler = Wholesaler.query.filter_by(code=wholesaler_code, is_active=True).first()
    if wholesaler:
        already = CollectionRun.query.filter_by(
            wholesaler_id=wholesaler.id,
            status="running",
        ).first()
        if already:
            started = already.started_at.strftime("%H:%M") if already.started_at else "?"
            return jsonify({
                "success": False,
                "already_running": True,
                "error": f"{wholesaler.name} 수집이 이미 진행 중입니다 ({started} 시작)",
            }), 409

    result = run_collection(
        wholesaler_code=wholesaler_code,
        trigger_type="manual",
        user_id=current_user.id,
    )
    return jsonify(result)


@collections_bp.route("/api/collection-status")
@login_required
def collection_status():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from app.execution_logs.models import CollectionRun

    running = CollectionRun.query.filter_by(status="running").all()

    _kst = ZoneInfo("Asia/Seoul")
    today_kst = datetime.now(_kst).date()
    today_start = datetime.combine(today_kst, datetime.min.time())
    recent_runs = (
        CollectionRun.query
        .filter(CollectionRun.started_at >= today_start)
        .order_by(CollectionRun.started_at.desc())
        .all()
    )

    return jsonify({
        "running": [
            {
                "wholesaler_name": r.wholesaler.name,
                "wholesaler_code": r.wholesaler.code,
                "started_at": r.started_at.strftime("%H:%M") if r.started_at else "?",
                "trigger_type": r.trigger_type,
            }
            for r in running
        ],
        "recent_runs": [
            {
                "wholesaler_code": r.wholesaler.code,
                "wholesaler_name": r.wholesaler.name,
                "status": r.status,
                "started_at": r.started_at.strftime("%H:%M") if r.started_at else "?",
                "total_items": r.total_items,
                "error_summary": r.error_summary,
            }
            for r in recent_runs
        ],
    })