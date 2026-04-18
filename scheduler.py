import os
import sys
import logging
import concurrent.futures
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
    from app import log_buffer

    logger.info(f"[scheduler] {name} 수집 시작")
    log_buffer.push(f"[수집] {name} 시작")
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
            cnt = result.get("total_items", 0)
            logger.info(f"[scheduler] {name} 수집 완료 ({cnt}건)")
            log_buffer.push(f"[수집] {name} 완료 ({cnt}건)")
            return True
        elif result.get("not_configured"):
            logger.info(f"[scheduler] {name} 설정 미완료 — 건너뜀 (알림 없음)")
            return False
        else:
            error = result.get("error") or "알 수 없는 오류"
            logger.error(f"[scheduler] {name} 수집 실패: {error}")
            log_buffer.push(f"[수집] {name} 실패: {str(error)[:100]}")
            notify_failure(name, str(error)[:300], run_time)
            return False

    except Exception as e:
        err_str = str(e)
        _config_kw = ("미설정", "환경변수 없음", "환경변수없음", "LOGIN_ID", "LOGIN_PASSWORD")
        if any(kw in err_str for kw in _config_kw):
            logger.info(f"[scheduler] {name} 설정 미완료 — 건너뜀 (알림 없음): {err_str}")
        else:
            logger.error(f"[scheduler] {name} 수집 예외: {e}")
            log_buffer.push(f"[수집] {name} 예외: {err_str[:100]}")
            notify_failure(name, err_str[:300], run_time)
        return False


COLLECTOR_TIMEOUT_SECS = 90 * 60  # 도매처별 최대 90분


def _timed_collect(wholesaler_code: str, name: str, flask_app, run_time: str) -> bool:
    """_collect_wholesaler를 최대 90분 타임아웃으로 실행. 초과 시 실패 알림 후 계속."""
    from notifiers.telegram import notify_failure

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_collect_wholesaler, wholesaler_code, name, flask_app, run_time)
        try:
            return future.result(timeout=COLLECTOR_TIMEOUT_SECS)
        except concurrent.futures.TimeoutError:
            logger.error(f"[scheduler] {name} 타임아웃 ({COLLECTOR_TIMEOUT_SECS // 60}분) — 건너뜀")
            notify_failure(name, f"수집 타임아웃 ({COLLECTOR_TIMEOUT_SECS // 60}분 초과)", run_time)
            return False
        except Exception as e:
            logger.error(f"[scheduler] {name} 타임아웃 래퍼 예외: {e}")
            return False


def _ownerclan_trigger(flask_app, run_time: str) -> bool:
    """오너클랜 트리거만 실행 (다운로드 세트 요청). DB 저장 없음."""
    from notifiers.telegram import notify_failure
    from collectors.ownerclan import OwnerclanCollector

    logger.info("[scheduler] 오너클랜 트리거 시작")
    try:
        with flask_app.app_context():
            result = OwnerclanCollector().run(phase="trigger")
        if result.get("success"):
            logger.info(f"[scheduler] 오너클랜 트리거 완료 (idx={result.get('trigger_idx')})")
            return True
        else:
            error = result.get("error_summary") or "알 수 없는 오류"
            logger.error(f"[scheduler] 오너클랜 트리거 실패: {error}")
            notify_failure("오너클랜(트리거)", str(error)[:300], run_time)
            return False
    except Exception as e:
        logger.error(f"[scheduler] 오너클랜 트리거 예외: {e}")
        notify_failure("오너클랜(트리거)", str(e)[:300], run_time)
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


def run_noon_pipeline():
    """
    23:00 전체 수집 파이프라인 (순차 실행)

    순서:
      1. 오너클랜 트리거 (다운로드 세트 요청)
      2. API 도매처: 친구도매 → 젠트레이드 → 3MRO
      3. 오너클랜 다운로드 (API 수집 중 파일 준비됨)
      4. 크롤링/파일 도매처: 철물박사 → JTC코리아 → 필우커머스 → 식자재마트
                            → 히트가구 → DS도매 → 도매토피아 → 온채널
      5. 스토어 동기화 + 시그널 갱신
    """
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 낮 파이프라인 시작 ({run_time})")

    from app import create_app
    flask_app = create_app()

    # 1. 오너클랜 트리거 (다운로드 세트 요청만, 빠름)
    _ownerclan_trigger(flask_app, run_time)

    # 2. API 도매처 (오너클랜 파일 준비되는 동안 수집)
    _timed_collect("chingudome", "친구도매", flask_app, run_time)
    _timed_collect("zentrade",   "젠트레이드", flask_app, run_time)
    _timed_collect("mro3",       "3MRO",      flask_app, run_time)

    # 3. 오너클랜 다운로드 (API 수집 소요 시간 ≒ 20분 대기 완료)
    _timed_collect("ownerclan", "오너클랜", flask_app, run_time)

    # 4. 크롤링 도매처 (시간 오래 걸리는 순으로)
    _timed_collect("metaldiy",   "철물박사",   flask_app, run_time)
    _timed_collect("jtckorea",   "JTC코리아",  flask_app, run_time)
    _timed_collect("feelwoo",    "필우커머스", flask_app, run_time)
    _timed_collect("sikjaje",    "식자재마트", flask_app, run_time)
    _timed_collect("hitdesign",  "히트가구",   flask_app, run_time)
    _timed_collect("ds1008",     "DS도매",     flask_app, run_time)
    _timed_collect("dometopia",  "도매토피아", flask_app, run_time)
    _timed_collect("onch3",      "온채널",     flask_app, run_time)

    # 5. 스토어 동기화 + 시그널
    run_store_sync(flask_app, run_time)
    run_match_and_signal(flask_app, run_time)

    logger.info(f"[scheduler] 낮 파이프라인 완료 ({run_time})")


def run_ownerclan_retry():
    """04:59 — 오너클랜 최근 10시간 내 성공 기록 없으면 재시도 (1회만)"""
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 오너클랜 재시도 확인 ({run_time})")

    from app import create_app
    flask_app = create_app()

    try:
        with flask_app.app_context():
            from app.execution_logs.models import CollectionRun
            from app.wholesalers.models import Wholesaler
            from datetime import timedelta

            ownerclan = Wholesaler.query.filter_by(code="ownerclan").first()
            if not ownerclan:
                logger.warning("[scheduler] 오너클랜 도매처 DB 없음 — 재시도 건너뜀")
                return

            # 파이프라인은 23:00 시작, 오너클랜 수집은 자정 전 완료 → 날짜 기준 아닌 시간 기준 체크
            # 04:59 기준 최근 10시간(= 전날 18:59 이후) 내 성공 기록 확인
            cutoff = datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None) - timedelta(hours=10)
            success_recent = CollectionRun.query.filter(
                CollectionRun.wholesaler_id == ownerclan.id,
                CollectionRun.started_at >= cutoff,
                CollectionRun.status == "success",
            ).first()

            if success_recent:
                logger.info(f"[scheduler] 오너클랜 최근 수집 성공({str(success_recent.started_at)[:19]}) — 재시도 건너뜀")
                return

    except Exception as e:
        logger.error(f"[scheduler] 오너클랜 재시도 확인 중 오류: {e}")
        return

    logger.info("[scheduler] 오너클랜 최근 수집 성공 없음 — 재시도 시작")
    _collect_wholesaler("ownerclan", "오너클랜(재시도)", flask_app, run_time)


def run_option_sync():
    """매일 05:30 — 스토어 옵션 추가금 동기화 (applied_option_diffs 초기화)"""
    from app import create_app
    from notifiers.telegram import notify_failure
    flask_app = create_app()
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    logger.info(f"[scheduler] 옵션 동기화 시작 ({run_time})")
    try:
        from app.store import sync_store_option_state, detect_option_mismatches
        result = sync_store_option_state(flask_app)
        logger.info(
            f"[scheduler] 옵션 동기화 완료 — "
            f"확인 {result.get('checked', 0)}건 / 기록 {result.get('matched', 0)}건"
        )
        m_result = detect_option_mismatches(flask_app)
        logger.info(
            f"[scheduler] 단품↔옵션 불일치 감지 — "
            f"신규 {m_result.get('created', 0)}건 / 갱신 {m_result.get('updated', 0)}건"
        )
    except Exception as e:
        logger.error(f"[scheduler] 옵션 동기화 실패: {e}", exc_info=True)
        notify_failure("옵션 동기화", str(e), run_time)


def run_db_cleanup():
    """매일 03:00 — 오래된 데이터 정리"""
    from app import create_app
    from datetime import timedelta

    flask_app = create_app()
    today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()

    cutoffs = {
        "NormalizedProduct": today_kst - timedelta(days=7),
        "CollectionRun":     today_kst - timedelta(days=60),
        "ProductEvent":      today_kst - timedelta(days=90),
        "ActionSignal":      today_kst - timedelta(days=30),
    }

    try:
        with flask_app.app_context():
            from app.infrastructure import db
            from app.normalization.models import NormalizedProduct
            from app.execution_logs.models import CollectionRun
            from app.master.models import ProductEvent
            from app.actions.models import ActionSignal

            # NormalizedProduct: 7일 이상 지난 것
            n = NormalizedProduct.query.filter(
                NormalizedProduct.collected_at < cutoffs["NormalizedProduct"]
            ).delete(synchronize_session=False)

            # CollectionRun: 60일 이상 지난 것
            c = CollectionRun.query.filter(
                CollectionRun.started_at < cutoffs["CollectionRun"]
            ).delete(synchronize_session=False)

            # ProductEvent: 90일 이상 지난 것
            e = ProductEvent.query.filter(
                ProductEvent.event_date < cutoffs["ProductEvent"]
            ).delete(synchronize_session=False)

            # ActionSignal: 처리 완료된 것 중 30일 이상 지난 것
            a = ActionSignal.query.filter(
                ActionSignal.status.in_(["executed", "reverted", "rejected", "skipped", "failed"]),
                ActionSignal.detected_at < cutoffs["ActionSignal"],
            ).delete(synchronize_session=False)

            db.session.commit()
            logger.info(f"[scheduler] DB 정리 완료 — NormalizedProduct:{n} CollectionRun:{c} ProductEvent:{e} ActionSignal:{a}")

    except Exception as ex:
        logger.error(f"[scheduler] DB 정리 실패: {ex}")


def run_db_backup():
    """매일 03:00 — DB 백업 (최근 7일치 보관)"""
    import shutil
    run_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    db_path = Path(__file__).resolve().parent / "instance" / "wholesaler.db"
    backup_dir = Path(__file__).resolve().parent / "instance" / "backups"
    backup_dir.mkdir(exist_ok=True)

    timestamp = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"wholesaler_{timestamp}.db"

    try:
        shutil.copy2(db_path, backup_path)
        logger.info(f"[scheduler] DB 백업 완료: {backup_path.name}")
    except Exception as e:
        logger.error(f"[scheduler] DB 백업 실패: {e}")
        return

    # 7일 초과 백업 삭제
    backups = sorted(backup_dir.glob("wholesaler_*.db"))
    for old in backups[:-7]:
        old.unlink()
        logger.info(f"[scheduler] 오래된 백업 삭제: {old.name}")


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
        run_noon_pipeline,
        trigger="cron",
        hour=23,
        minute=0,
        id="daily_pipeline",
        timezone=TIMEZONE,
    )

    scheduler.add_job(
        run_ownerclan_retry,
        trigger="cron",
        hour=4,
        minute=59,
        id="ownerclan_retry",
        timezone=TIMEZONE,
    )
    scheduler.add_job(
        lambda: (run_db_backup(), run_db_cleanup()),
        trigger="cron",
        hour=1,
        minute=59,
        id="db_backup",
        timezone=TIMEZONE,
    )
    scheduler.add_job(
        run_option_sync,
        trigger="cron",
        hour=5,
        minute=30,
        id="option_sync",
        timezone=TIMEZONE,
    )

    logger.info(f"[scheduler] 시작 — 매일 23:00 파이프라인 / 04:59 오너클랜 재시도 / 01:59 DB백업+정리 / 05:30 옵션동기화 ({TIMEZONE})")
    logger.info("[scheduler] Ctrl+C로 중단")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 종료")
