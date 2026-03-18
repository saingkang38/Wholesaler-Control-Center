import os
import sys
import logging
from datetime import datetime

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


def run_ownerclan():
    from collectors.ownerclan import OwnerclanCollector
    from notifiers.telegram import notify_success, notify_failure

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 오너클랜 수집 시작 ({run_time})")

    try:
        result = OwnerclanCollector().run()
    except Exception as e:
        logger.error(f"[scheduler] 오너클랜 수집 예외: {e}")
        notify_failure("오너클랜", str(e)[:300], run_time)
        return

    if result.get("success"):
        count = result.get("total_items", 0)
        logger.info(f"[scheduler] 오너클랜 수집 완료: {count}건")
        notify_success("오너클랜", count, run_time)
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

    logger.info(f"[scheduler] 시작 - 매일 {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} ({TIMEZONE}) 실행")
    logger.info("[scheduler] Ctrl+C로 중단")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 종료")
