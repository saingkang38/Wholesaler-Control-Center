import os
import sys
import logging
from datetime import datetime, date
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from pathlib import Path
from dotenv import load_dotenv

base = Path(__file__).resolve().parent
load_dotenv(base / ".env")
load_dotenv(base / ".env.local", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Asia/Seoul")


def _build_changes(stats: dict) -> dict:
    return {
        "신규":       stats.get("new", 0),
        "재입고":     stats.get("restocked", 0),
        "가격변동":   stats.get("price_change", 0),
        "상품명변경": stats.get("name_change", 0),
        "이미지변경": stats.get("image_change", 0),
        "품절단종":   stats.get("missing", 0),
        "삭제":       stats.get("discontinued_candidate", 0),
    }


def _collect_wholesaler(wholesaler_code: str, name: str, flask_app, run_time: str) -> bool:
    """단일 도매처 수집 + 마스터 업데이트 + 텔레그램 알림. 성공 여부 반환."""
    from app.collectors.orchestrator import run_collection
    from notifiers.telegram import notify_changes, notify_failure

    logger.info(f"[scheduler] {name} 수집 시작")
    try:
        with flask_app.app_context():
            result = run_collection(wholesaler_code, trigger_type="scheduled")

        if result.get("success"):
            notify_changes(
                name,
                result.get("total_items", 0),
                run_time,
                _build_changes(result.get("master_stats") or {}),
            )
            logger.info(f"[scheduler] {name} 수집 완료 ({result.get('total_items', 0)}건)")
            return True
        elif result.get("not_configured"):
            logger.info(f"[scheduler] {name} 설정 미완료 — 건너뜀 (알림 없음)")
            return False
        else:
            error = result.get("error") or "알 수 없는 오류"
            logger.error(f"[scheduler] {name} 수집 실패: {error}")
            notify_failure(name, str(error)[:300], run_time)
            return False

    except Exception as e:
        err_str = str(e)
        _config_kw = ("미설정", "환경변수 없음", "환경변수없음", "LOGIN_ID", "LOGIN_PASSWORD")
        if any(kw in err_str for kw in _config_kw):
            logger.info(f"[scheduler] {name} 설정 미완료 — 건너뜀 (알림 없음): {err_str}")
        else:
            logger.error(f"[scheduler] {name} 수집 예외: {e}")
            notify_failure(name, err_str[:300], run_time)
        return False


def run_store_sync(flask_app, run_time: str):
    """스마트스토어 전체 동기화"""
    from notifiers.telegram import notify_failure

    logger.info(f"[scheduler] 스토어 동기화 시작")
    try:
        from app.store import sync_store_products
        with flask_app.app_context():
            stats = sync_store_products()
        logger.info(f"[scheduler] 스토어 동기화 완료: {stats}")
    except Exception as e:
        logger.error(f"[scheduler] 스토어 동기화 실패: {e}")
        notify_failure("스토어동기화", str(e)[:300], run_time)


def run_match_and_signal(flask_app, run_time: str):
    """마스터↔스토어 매칭 + 액션 시그널 감지"""
    logger.info(f"[scheduler] 매칭 및 시그널 감지 시작")
    try:
        from app.actions import detect_action_signals
        from app.wholesalers.models import Wholesaler
        with flask_app.app_context():
            wholesalers = Wholesaler.query.filter_by(is_active=True).all()
            for ws in wholesalers:
                stats = detect_action_signals(ws.id)
                logger.info(f"[scheduler] {ws.name} 시그널: {stats}")
    except Exception as e:
        logger.error(f"[scheduler] 매칭/시그널 감지 실패: {e}")


def run_early_pipeline():
    """새벽 2시 — 오너클랜 → 철물박사 → 스토어동기화 → 시그널"""
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 새벽 파이프라인 시작 ({run_time})")

    from app import create_app
    flask_app = create_app()

    # 오너클랜 먼저 (20분 대기 포함 — 수집 자체가 오래 걸림)
    _collect_wholesaler("ownerclan", "오너클랜", flask_app, run_time)

    # 성공/실패 무관하게 철물박사 연속 진행
    _collect_wholesaler("metaldiy", "철물박사", flask_app, run_time)

    # 수집 완료 후 스토어 동기화 + 시그널 갱신
    run_store_sync(flask_app, run_time)
    run_match_and_signal(flask_app, run_time)

    logger.info(f"[scheduler] 새벽 파이프라인 완료 ({run_time})")


def run_ownerclan_retry():
    """새벽 6시 — 오너클랜 오늘 성공 기록 없으면 재시도"""
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 오너클랜 재시도 확인 ({run_time})")

    from app import create_app
    flask_app = create_app()

    try:
        with flask_app.app_context():
            from app.execution_logs.models import CollectionRun
            from app.wholesalers.models import Wholesaler

            ownerclan = Wholesaler.query.filter_by(code="ownerclan").first()
            if not ownerclan:
                logger.warning("[scheduler] 오너클랜 도매처 DB 없음 — 재시도 건너뜀")
                return

            today_start = datetime.combine(date.today(), datetime.min.time())
            success_today = CollectionRun.query.filter(
                CollectionRun.wholesaler_id == ownerclan.id,
                CollectionRun.started_at >= today_start,
                CollectionRun.status == "success",
            ).first()

            if success_today:
                logger.info("[scheduler] 오너클랜 오늘 이미 성공 — 재시도 건너뜀")
                return

    except Exception as e:
        logger.error(f"[scheduler] 오너클랜 재시도 확인 중 오류: {e}")
        return

    logger.info("[scheduler] 오너클랜 오늘 성공 기록 없음 — 재시도 시작")
    _collect_wholesaler("ownerclan", "오너클랜(6시재시도)", flask_app, run_time)


if __name__ == "__main__":
    import tempfile
    _lock_path = Path(tempfile.gettempdir()) / "wholesaler_scheduler.lock"

    def _write_lock(path):
        with open(path, "x") as f:
            f.write(str(os.getpid()))

    try:
        _write_lock(_lock_path)
    except FileExistsError:
        try:
            old_pid = int(_lock_path.read_text().strip())
            import psutil
            if psutil.pid_exists(old_pid):
                logger.error(f"[scheduler] 이미 실행 중 (PID {old_pid}). 종료합니다.")
                sys.exit(1)
            else:
                _lock_path.unlink()
                _write_lock(_lock_path)
        except (FileNotFoundError, ValueError):
            _lock_path.unlink(missing_ok=True)
            _write_lock(_lock_path)

    import atexit
    atexit.register(lambda: _lock_path.unlink(missing_ok=True))

    scheduler = BlockingScheduler(timezone=TIMEZONE)

    scheduler.add_job(
        run_early_pipeline,
        trigger="cron",
        hour=2,
        minute=0,
        id="early_pipeline",
    )
    scheduler.add_job(
        run_ownerclan_retry,
        trigger="cron",
        hour=6,
        minute=0,
        id="ownerclan_retry",
    )

    logger.info(f"[scheduler] 시작 — 02:00 새벽파이프라인 / 06:00 오너클랜재시도 ({TIMEZONE})")
    logger.info("[scheduler] Ctrl+C로 중단")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 종료")
