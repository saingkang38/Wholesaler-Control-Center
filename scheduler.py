import os
import sys
import logging
from datetime import datetime
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

SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "2"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))
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
        else:
            error = result.get("error") or "알 수 없는 오류"
            logger.error(f"[scheduler] {name} 수집 실패: {error}")
            notify_failure(name, str(error)[:300], run_time)
            return False

    except Exception as e:
        logger.error(f"[scheduler] {name} 수집 예외: {e}")
        notify_failure(name, str(e)[:300], run_time)
        return False


def run_all_wholesalers(flask_app, run_time: str):
    """전체 도매처 순차 수집 — DB 활성 도매처 기준 (하드코딩 목록 제거)"""
    logger.info(f"[scheduler] 전체 도매처 수집 시작 ({run_time})")

    with flask_app.app_context():
        from app.wholesalers.models import Wholesaler
        wholesalers = [(w.code, w.name) for w in Wholesaler.query.filter_by(is_active=True).all()]

    results = {}
    for code, name in wholesalers:
        results[code] = _collect_wholesaler(code, name, flask_app, run_time)

    # 실패한 도매처 1회 재시도
    retry_targets = [(code, name) for code, name in wholesalers if not results.get(code)]
    if retry_targets:
        logger.info(f"[scheduler] 재시도 대상: {[n for _, n in retry_targets]}")
        for code, name in retry_targets:
            results[code] = _collect_wholesaler(code, f"{name}(재시도)", flask_app, run_time)

    logger.info(f"[scheduler] 전체 도매처 수집 완료")


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


def run_daily_pipeline():
    """매일 새벽 — 수집 → 스토어동기화 → 매칭 순차 실행"""
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 일일 파이프라인 시작 ({run_time})")

    # 매 실행마다 새 앱 인스턴스 생성 — 전날 DB 세션 오염 방지
    from app import create_app
    flask_app = create_app()

    run_all_wholesalers(flask_app, run_time)
    run_store_sync(flask_app, run_time)
    run_match_and_signal(flask_app, run_time)

    logger.info(f"[scheduler] 일일 파이프라인 완료 ({run_time})")


if __name__ == "__main__":
    import tempfile
    _lock_path = Path(tempfile.gettempdir()) / "wholesaler_scheduler.lock"
    def _write_lock(path):
        with open(path, "x") as f:
            f.write(str(os.getpid()))

    try:
        _write_lock(_lock_path)
    except FileExistsError:
        # 락 파일이 남아있으면 PID 확인 후 판단
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
        run_daily_pipeline,
        trigger="cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        id="daily_pipeline",
    )

    logger.info(f"[scheduler] 시작 - 매일 {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} ({TIMEZONE}) 실행")
    logger.info("[scheduler] Ctrl+C로 중단")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 종료")
