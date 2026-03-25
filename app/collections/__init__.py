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
    from app.execution_logs.models import CollectionRun
    running = CollectionRun.query.filter_by(status="running").all()
    return jsonify({
        "running": [
            {
                "wholesaler_name": r.wholesaler.name,
                "wholesaler_code": r.wholesaler.code,
                "started_at": r.started_at.strftime("%H:%M") if r.started_at else "?",
                "trigger_type": r.trigger_type,
            }
            for r in running
        ]
    })