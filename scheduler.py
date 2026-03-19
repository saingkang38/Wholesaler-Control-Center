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


def run_ownerclan():
    from collectors.ownerclan import OwnerclanCollector
    from notifiers.telegram import notify_changes, notify_failure
    from comparators import load_snapshot, save_snapshot, compare

    downloads_dir = os.getenv("DOWNLOADS_DIR", "/tmp/downloads")
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 오너클랜 수집 시작 ({run_time})")

    try:
        result = OwnerclanCollector().run()
    except Exception as e:
        logger.error(f"[scheduler] 오너클랜 수집 예외: {e}")
        notify_failure("오너클랜", str(e)[:300], run_time)
        return

    if result.get("success"):
        total = result.get("total_items", 0)
        items = result.get("items", [])

        # 파일 기반 스냅샷 저장 (스냅샷 유지용, 텔레그램에는 미사용)
        old_snapshot = load_snapshot(downloads_dir)
        compare(old_snapshot, items)
        save_snapshot(items, downloads_dir)

        # DB 마스터 갱신 + 스토어 동기화 + 액션 시그널 감지
        telegram_changes = None
        try:
            from app import create_app
            from app.master import process_master_update
            from app.wholesalers.models import Wholesaler

            flask_app = create_app()
            with flask_app.app_context():
                wholesaler = Wholesaler.query.filter_by(code="ownerclan").first()
                if wholesaler:
                    master_stats = process_master_update(wholesaler.id, items)
                    logger.info(f"[scheduler] 마스터 업데이트: {master_stats}")
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
            logger.error(f"[scheduler] 마스터 업데이트 실패: {e}")

        logger.info(f"[scheduler] 오너클랜 수집 완료: {total}건")
        notify_changes("오너클랜", total, run_time, telegram_changes)
    else:
        error = result.get("error_summary", "알 수 없는 오류")
        logger.error(f"[scheduler] 오너클랜 수집 실패: {error}")
        notify_failure("오너클랜", error, run_time)


if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        run_ownerclan,
        trigger="cron",
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        id="ownerclan_daily",
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
