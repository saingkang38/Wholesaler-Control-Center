import os
import threading
from flask import Blueprint, current_app, jsonify
from flask_login import login_required, current_user
from app.collectors.orchestrator import run_collection

collections_bp = Blueprint("collections", __name__)

# 동시 수집 요청 차단 — DB 조회보다 먼저 잠금 획득
_running: set = set()
_running_lock = threading.Lock()


@collections_bp.route("/api/collect/<wholesaler_code>", methods=["POST"])
@login_required
def trigger_collection(wholesaler_code):
    from app.execution_logs.models import CollectionRun
    from app.wholesalers.models import Wholesaler

    with _running_lock:
        if wholesaler_code in _running:
            return jsonify({
                "success": False,
                "already_running": True,
                "error": f"{wholesaler_code} 수집이 이미 진행 중입니다",
            }), 409
        _running.add(wholesaler_code)

    try:
        wholesaler = Wholesaler.query.filter_by(code=wholesaler_code, is_active=True).first()
        if wholesaler:
            already = CollectionRun.query.filter_by(
                wholesaler_id=wholesaler.id,
                status="running",
            ).first()
            if already:
                started = already.started_at.strftime("%H:%M") if already.started_at else "?"
                with _running_lock:
                    _running.discard(wholesaler_code)
                return jsonify({
                    "success": False,
                    "already_running": True,
                    "error": f"{wholesaler.name} 수집이 이미 진행 중입니다 ({started} 시작)",
                }), 409

        # 오너클랜: 트리거만 실행 후 타이머로 다운로드 예약
        if wholesaler_code == "ownerclan":
            from collectors.ownerclan import OwnerclanCollector
            trigger_result = OwnerclanCollector().run(phase="trigger")
            if not trigger_result.get("success"):
                with _running_lock:
                    _running.discard(wholesaler_code)
                return jsonify({
                    "success": False,
                    "error": trigger_result.get("error_summary") or trigger_result.get("error", "트리거 실패"),
                })
            wait = int(os.getenv("OWNERCLAN_WAIT_SECONDS", "1200"))
            app = current_app._get_current_object()
            uid = current_user.id

            def _do_download():
                with app.app_context():
                    run_collection("ownerclan", trigger_type="manual", user_id=uid, phase="download")
                with _running_lock:
                    _running.discard("ownerclan")

            threading.Timer(wait, _do_download).start()
            return jsonify({
                "success": True,
                "pending": True,
                "message": f"{wait // 60}분 후 다운로드 예약됨",
            })

        result = run_collection(
            wholesaler_code=wholesaler_code,
            trigger_type="manual",
            user_id=current_user.id,
        )
        return jsonify(result)
    finally:
        # ownerclan은 타이머에서 직접 discard하므로 여기서는 건너뜀
        if wholesaler_code != "ownerclan":
            with _running_lock:
                _running.discard(wholesaler_code)


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
