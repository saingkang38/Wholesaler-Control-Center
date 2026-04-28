import os
import threading
from flask import Blueprint, current_app, jsonify
from flask_login import login_required, current_user
from app.collectors.orchestrator import run_collection

collections_bp = Blueprint("collections", __name__)

# 동시 수집 요청 차단 — DB 조회보다 먼저 잠금 획득
_running: set = set()
_running_lock = threading.Lock()


def _is_chain_active() -> bool:
    from app.execution_logs.models import CollectionRun
    return (
        CollectionRun.query
        .filter_by(trigger_type="chain", status="running")
        .first()
        is not None
    )


@collections_bp.route("/api/collect/<wholesaler_code>", methods=["POST"])
@login_required
def trigger_collection(wholesaler_code):
    from app.execution_logs.models import CollectionRun
    from app.wholesalers.models import Wholesaler

    if _is_chain_active():
        return jsonify({
            "success": False,
            "chain_active": True,
            "error": "체인 수집 진행 중입니다. 완료될 때까지 수동 수집이 제한됩니다.",
        }), 409

    with _running_lock:
        if wholesaler_code in _running:
            return jsonify({
                "success": False,
                "already_running": True,
                "error": f"{wholesaler_code} 수집이 이미 진행 중입니다",
            }), 409
        _running.add(wholesaler_code)

    timer_started = False
    try:
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

        # 오너클랜: 트리거만 실행 후 타이머로 다운로드 예약
        if wholesaler_code == "ownerclan":
            from collectors.ownerclan import OwnerclanCollector
            trigger_result = OwnerclanCollector().run(phase="trigger")
            if not trigger_result.get("success"):
                return jsonify({
                    "success": False,
                    "error": trigger_result.get("error_summary") or trigger_result.get("error", "트리거 실패"),
                })
            wait = int(os.getenv("OWNERCLAN_WAIT_SECONDS", "1200"))
            app = current_app._get_current_object()
            uid = current_user.id

            def _do_download():
                try:
                    with app.app_context():
                        run_collection("ownerclan", trigger_type="manual", user_id=uid, phase="download")
                finally:
                    with _running_lock:
                        _running.discard("ownerclan")

            threading.Timer(wait, _do_download).start()
            timer_started = True
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
        # ownerclan + Timer 정상 예약된 경우에만 Timer가 discard 책임짐.
        # 그 외 모든 경로(예외/조기 return/비-ownerclan)는 여기서 discard.
        if not (wholesaler_code == "ownerclan" and timer_started):
            with _running_lock:
                _running.discard(wholesaler_code)


@collections_bp.route("/api/wholesaler-status/<code>")
@login_required
def wholesaler_status(code):
    from datetime import datetime
    from app.execution_logs.models import CollectionRun
    from app.wholesalers.models import Wholesaler

    wholesaler = Wholesaler.query.filter_by(code=code).first()
    if not wholesaler:
        return jsonify({"error": "not found"}), 404

    current_run = (
        CollectionRun.query
        .filter_by(wholesaler_id=wholesaler.id, status="running")
        .order_by(CollectionRun.started_at.desc())
        .first()
    )
    history = (
        CollectionRun.query
        .filter_by(wholesaler_id=wholesaler.id)
        .order_by(CollectionRun.started_at.desc())
        .limit(5)
        .all()
    )

    def fmt(r):
        elapsed = None
        if r.started_at and r.status == "running":
            elapsed = int((datetime.utcnow() - r.started_at).total_seconds() // 60)
        return {
            "id": r.id,
            "status": r.status,
            "started_at": r.started_at.strftime("%m-%d %H:%M") if r.started_at else None,
            "finished_at": r.finished_at.strftime("%H:%M") if r.finished_at else None,
            "total_items": r.total_items,
            "error_summary": r.error_summary,
            "elapsed_minutes": elapsed,
        }

    return jsonify({
        "code": wholesaler.code,
        "name": wholesaler.name,
        "current": fmt(current_run) if current_run else None,
        "history": [fmt(r) for r in history],
    })


@collections_bp.route("/api/collect/<wholesaler_code>/cancel", methods=["POST"])
@login_required
def cancel_collection(wholesaler_code):
    from datetime import datetime
    from app.execution_logs.models import CollectionRun
    from app.wholesalers.models import Wholesaler
    from app.infrastructure import db

    wholesaler = Wholesaler.query.filter_by(code=wholesaler_code).first()
    if not wholesaler:
        return jsonify({"error": "not found"}), 404

    run = (
        CollectionRun.query
        .filter_by(wholesaler_id=wholesaler.id, status="running")
        .order_by(CollectionRun.started_at.desc())
        .first()
    )
    if not run:
        return jsonify({"success": False, "error": "진행 중인 수집이 없습니다"}), 404

    run.status = "cancelled"
    run.finished_at = datetime.utcnow()
    run.error_summary = "사용자 수동 중단"
    db.session.commit()

    with _running_lock:
        _running.discard(wholesaler_code)

    return jsonify({"success": True})


@collections_bp.route("/api/collection-status")
def collection_status():
    from flask_login import current_user
    from flask import jsonify as _jsonify
    if not current_user.is_authenticated:
        return _jsonify({"error": "unauthenticated"}), 401
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from app.execution_logs.models import CollectionRun

    running = CollectionRun.query.filter_by(status="running").all()

    _kst = ZoneInfo("Asia/Seoul")
    now_kst = datetime.now(_kst).replace(tzinfo=None)
    # 파이프라인은 19:00 시작 → 19:00 이전이면 전날 19:00, 이후면 오늘 19:00 기준
    from datetime import time as _time, timedelta as _td
    if now_kst.hour < 19:
        today_start = datetime.combine(now_kst.date() - _td(days=1), _time(19, 0))
    else:
        today_start = datetime.combine(now_kst.date(), _time(19, 0))
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
