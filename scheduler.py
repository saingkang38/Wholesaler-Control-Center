import os
import sys
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SCHEDULE_HOUR = int(os.getenv("SCHEDULE_HOUR", "3"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))
TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "Asia/Seoul")


def run_store_sync():
    """매일 새벽 3시 - 스마트스토어 전체 동기화"""
    from notifiers.telegram import notify_failure
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 스토어 동기화 시작 ({run_time})")

    try:
        from app import create_app
        from app.store import sync_store_products
        from app.wholesalers.models import Wholesaler

        flask_app = create_app()
        with flask_app.app_context():
            wholesaler = Wholesaler.query.filter_by(code="ownerclan").first()
            if wholesaler:
                store_stats = sync_store_products(wholesaler.id)
                logger.info(f"[scheduler] 스토어 동기화 완료: {store_stats}")
    except Exception as e:
        logger.error(f"[scheduler] 스토어 동기화 실패: {e}")
        notify_failure("스토어동기화", str(e)[:300], run_time)


def run_match_and_signal():
    """매일 새벽 5시 - 마스터↔스토어 매칭 + 액션 시그널 감지"""
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 매칭 및 시그널 감지 시작 ({run_time})")

    try:
        from app import create_app
        from app.actions import detect_action_signals
        from app.wholesalers.models import Wholesaler

        flask_app = create_app()
        with flask_app.app_context():
            wholesaler = Wholesaler.query.filter_by(code="ownerclan").first()
            if wholesaler:
                signal_stats = detect_action_signals(wholesaler.id)
                logger.info(f"[scheduler] 액션 시그널: {signal_stats}")
    except Exception as e:
        logger.error(f"[scheduler] 매칭/시그널 감지 실패: {e}")


def _collect_wholesaler(wholesaler_code: str, name: str, flask_app, run_time: str):
    """단일 도매처 수집 + 마스터 업데이트. 성공 여부 반환."""
    from app.collectors.orchestrator import run_collection

    logger.info(f"[scheduler] {name} 수집 시작")
    try:
        with flask_app.app_context():
            result = run_collection(wholesaler_code, trigger_type="scheduled")
        if result.get("success"):
            logger.info(f"[scheduler] {name} 수집 완료")
            return True
        else:
            logger.error(f"[scheduler] {name} 수집 실패: {result.get('error')}")
            return False
    except Exception as e:
        logger.error(f"[scheduler] {name} 수집 예외: {e}")
        return False


def run_all_wholesalers():
    """매일 새벽 1시 - 전체 도매처 순차 수집"""
    from notifiers.telegram import notify_changes, notify_failure
    from comparators import load_snapshot, save_snapshot, compare
    from collectors.ownerclan import OwnerclanCollector

    downloads_dir = os.getenv("DOWNLOADS_DIR", "/tmp/downloads")
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 전체 도매처 수집 시작 ({run_time})")

    from app import create_app
    flask_app = create_app()

    # 1. 오너클랜 (파일 스냅샷 + DB 마스터 갱신)
    logger.info("[scheduler] 오너클랜 수집 시작")
    try:
        result = OwnerclanCollector().run()
    except Exception as e:
        logger.error(f"[scheduler] 오너클랜 수집 예외: {e}")
        notify_failure("오너클랜", str(e)[:300], run_time)
        result = {"success": False}

    if result.get("success"):
        items = result.get("items", [])
        old_snapshot = load_snapshot(downloads_dir)
        compare(old_snapshot, items)
        save_snapshot(items, downloads_dir)
        telegram_changes = None
        try:
            from app.master import process_master_update
            from app.wholesalers.models import Wholesaler
            with flask_app.app_context():
                wholesaler = Wholesaler.query.filter_by(code="ownerclan").first()
                if wholesaler:
                    master_stats = process_master_update(wholesaler.id, items)
                    telegram_changes = {
                        "신규": master_stats.get("new", 0),
                        "삭제": master_stats.get("discontinued_candidate", 0),
                        "재입고": master_stats.get("restocked", 0),
                        "품절단종": master_stats.get("missing", 0),
                        "가격변동": master_stats.get("price_change", 0),
                        "이미지변경": master_stats.get("image_change", 0),
                        "상품명변경": master_stats.get("name_change", 0),
                    }
        except Exception as e:
            logger.error(f"[scheduler] 오너클랜 마스터 업데이트 실패: {e}")
        notify_changes("오너클랜", result.get("total_items", 0), run_time, telegram_changes)
        logger.info(f"[scheduler] 오너클랜 완료: {result.get('total_items', 0)}건")
    else:
        notify_failure("오너클랜", result.get("error_summary", "알 수 없는 오류"), run_time)

    # 2. JTC코리아
    jtc_ok = _collect_wholesaler("jtckorea", "JTC코리아", flask_app, run_time)

    # 3. 철물박사
    metal_ok = _collect_wholesaler("metaldiy", "철물박사", flask_app, run_time)

    # 4. DS도매
    ds_ok = _collect_wholesaler("ds1008", "DS도매", flask_app, run_time)

    # 5. 히트가구
    hit_ok = _collect_wholesaler("hitdesign", "히트가구", flask_app, run_time)

    # 5. 실패한 도매처 1회 재시도
    retry_targets = []
    if not jtc_ok:
        retry_targets.append(("jtckorea", "JTC코리아"))
    if not metal_ok:
        retry_targets.append(("metaldiy", "철물박사"))
    if not ds_ok:
        retry_targets.append(("ds1008", "DS도매"))
    if not hit_ok:
        retry_targets.append(("hitdesign", "히트가구"))

    if retry_targets:
        logger.info(f"[scheduler] 재시도 대상: {[n for _, n in retry_targets]}")
        for code, name in retry_targets:
            _collect_wholesaler(code, f"{name}(재시도)", flask_app, run_time)

    logger.info(f"[scheduler] 전체 도매처 수집 완료 ({run_time})")


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        run_all_wholesalers,
        trigger="cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        id="all_wholesalers_daily",
    )
    scheduler.add_job(
        run_store_sync,
        trigger="cron",
        hour=3,
        minute=0,
        id="store_sync_daily",
    )
    scheduler.add_job(
        run_match_and_signal,
        trigger="cron",
        hour=5,
        minute=0,
        id="match_signal_daily",
    )

    logger.info(f"[scheduler] 시작 - 매일 {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} ({TIMEZONE}) 실행")
    logger.info("[scheduler] Ctrl+C로 중단")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 종료")
