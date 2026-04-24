import os
import sys
import logging
import concurrent.futures
from datetime import datetime, timedelta
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
KST = ZoneInfo("Asia/Seoul")

CHAIN_GAP_SECONDS = 20 * 60
COLLECTOR_TIMEOUT_SECS = 90 * 60
COLLECTOR_TIMEOUT_OVERRIDES = {
    "hitdesign": 240 * 60,
    "ownerclan": 240 * 60,
}

# (slot_type, code, display_name, phase)
# slot_type: 'trigger' | 'collect'  (collect에는 일반 수집 + ownerclan-download 모두 포함)
CHAIN_SEQUENCE = [
    ("collect", "jtckorea",   "JTC코리아",        None),
    ("collect", "feelwoo",    "필우커머스",       None),
    ("collect", "zentrade",   "젠트레이드",       None),
    ("collect", "chingudome", "친구도매",         None),
    ("collect", "sikjaje",    "식자재마트",       None),
    ("collect", "mro3",       "3MRO",             None),
    ("collect", "ds1008",     "DS도매",           None),
    ("collect", "metaldiy",   "철물박사",         None),
    ("collect", "hitdesign",  "히트가구",         None),
    ("trigger", "ownerclan",  "오너클랜(트리거)",   "trigger"),
    ("collect", "ownerclan",  "오너클랜(다운로드)", "download"),
]

_scheduler_ref: BlockingScheduler | None = None
_flask_app_ref = None


def _today_kst_midnight() -> datetime:
    return datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None)


def _run_with_timeout(func, timeout_seconds: int):
    """별도 스레드에서 func 실행, 타임아웃 초과 시 (None, 에러문자열) 반환."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func)
        try:
            return future.result(timeout=timeout_seconds), None
        except concurrent.futures.TimeoutError:
            return None, f"타임아웃 ({timeout_seconds // 60}분 초과)"
        except Exception as e:
            return None, str(e)


def _find_resume_index() -> int:
    """오늘 chain으로 완료(success/skipped)된 마지막 슬롯 인덱스 + 1을 반환."""
    from app.execution_logs.models import CollectionRun
    from app.wholesalers.models import Wholesaler

    today_midnight = _today_kst_midnight()
    runs = (
        CollectionRun.query
        .filter(
            CollectionRun.trigger_type == "chain",
            CollectionRun.started_at >= today_midnight,
            CollectionRun.status.in_(["success", "skipped"]),
        )
        .all()
    )
    if not runs:
        return 0

    wids = {r.wholesaler_id for r in runs}
    code_by_id = {w.id: w.code for w in Wholesaler.query.filter(Wholesaler.id.in_(wids)).all()}
    completed_codes = {code_by_id.get(r.wholesaler_id) for r in runs}

    last_completed = -1
    for i, (_slot_type, code, _name, _phase) in enumerate(CHAIN_SEQUENCE):
        if code in completed_codes:
            last_completed = i
    return last_completed + 1


def _collect_today_slot_results() -> list:
    """오늘 chain run 결과를 슬롯 순서로 정렬해 반환."""
    from app.execution_logs.models import CollectionRun
    from app.wholesalers.models import Wholesaler

    today_midnight = _today_kst_midnight()
    runs = (
        CollectionRun.query
        .filter(
            CollectionRun.trigger_type == "chain",
            CollectionRun.started_at >= today_midnight,
        )
        .order_by(CollectionRun.started_at.asc())
        .all()
    )
    id_to_name = {w.id: w.name for w in Wholesaler.query.all()}
    return [
        {
            "name": id_to_name.get(r.wholesaler_id, str(r.wholesaler_id)),
            "status": r.status,
            "total_items": r.total_items or 0,
        }
        for r in runs
    ]


def _today_chain_started_at() -> datetime | None:
    from app.execution_logs.models import CollectionRun
    today_midnight = _today_kst_midnight()
    first = (
        CollectionRun.query
        .filter(CollectionRun.trigger_type == "chain", CollectionRun.started_at >= today_midnight)
        .order_by(CollectionRun.started_at.asc())
        .first()
    )
    return first.started_at if first else None


def _execute_chain_slot(slot_index: int):
    """단일 체인 슬롯 실행 → 알림 → 다음 슬롯 예약. 실패해도 체인은 이어감."""
    if _scheduler_ref is None or _flask_app_ref is None:
        logger.error("[chain] scheduler/flask 레퍼런스 미초기화 — 슬롯 중단")
        return

    if slot_index >= len(CHAIN_SEQUENCE):
        _schedule_finalize()
        return

    slot_type, code, name, phase = CHAIN_SEQUENCE[slot_index]
    started_at = datetime.now(KST)
    started_str = started_at.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"[chain] 슬롯 {slot_index} ({name}) 시작 @ {started_str}")

    status = "failed"
    error_msg: str | None = None
    stats_payload: dict | None = None

    try:
        if slot_type == "trigger":
            # 오너클랜 트리거: DB 기록 없음, phase='trigger' 직접 실행
            from collectors.ownerclan import OwnerclanCollector

            def _do_trigger():
                with _flask_app_ref.app_context():
                    return OwnerclanCollector().run(phase="trigger")

            r, err = _run_with_timeout(_do_trigger, 10 * 60)
            if err:
                status = "failed"
                error_msg = err
            elif r and r.get("success"):
                status = "success"
                stats_payload = {"total_items": 0, "master_stats": {}}
            else:
                status = "failed"
                error_msg = (r or {}).get("error_summary") or (r or {}).get("error") or "트리거 실패"

        else:
            from app.collectors.orchestrator import run_collection

            def _do_collect():
                with _flask_app_ref.app_context():
                    return run_collection(code, trigger_type="chain", phase=phase)

            r, err = _run_with_timeout(_do_collect, COLLECTOR_TIMEOUT_OVERRIDES.get(code, COLLECTOR_TIMEOUT_SECS))
            if err:
                status = "failed"
                error_msg = err
            elif r and r.get("success"):
                status = "success"
                stats_payload = {
                    "total_items": r.get("total_items", 0),
                    "master_stats": r.get("master_stats") or {},
                }
            elif r and r.get("not_configured"):
                status = "skipped"
                error_msg = r.get("error") or "설정 미완료"
            else:
                status = "failed"
                error_msg = (r or {}).get("error") or "알 수 없는 오류"

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.exception(f"[chain] 슬롯 {slot_index} ({name}) 외곽 예외")

    finally:
        finished_at = datetime.now(KST)
        finished_str = finished_at.strftime("%Y-%m-%d %H:%M:%S")
        elapsed = int((finished_at - started_at).total_seconds())

        try:
            from notifiers.telegram import notify_wholesaler_done
            notify_wholesaler_done(
                name=name,
                status=status,
                started_at=started_str,
                finished_at=finished_str,
                elapsed_seconds=elapsed,
                stats=stats_payload,
                error=error_msg,
            )
        except Exception as notify_err:
            logger.warning(f"[chain] 알림 실패 (무시): {notify_err}")

        logger.info(
            f"[chain] 슬롯 {slot_index} ({name}) 종료: status={status} elapsed={elapsed}s"
        )

        _schedule_next_slot(slot_index + 1)


def _schedule_next_slot(next_index: int):
    """다음 슬롯 또는 최종 마무리를 now+CHAIN_GAP_SECONDS 에 예약."""
    if _scheduler_ref is None:
        logger.error("[chain] scheduler 레퍼런스 없음 — 다음 예약 실패")
        return

    run_date = datetime.now(KST) + timedelta(seconds=CHAIN_GAP_SECONDS)

    if next_index >= len(CHAIN_SEQUENCE):
        _scheduler_ref.add_job(
            _finalize_chain,
            trigger="date",
            run_date=run_date,
            id="chain_finalize",
            replace_existing=True,
        )
        logger.info(f"[chain] 최종 마무리 예약 @ {run_date.strftime('%H:%M:%S')}")
        return

    _, _code, name, _ = CHAIN_SEQUENCE[next_index]
    _scheduler_ref.add_job(
        _execute_chain_slot,
        trigger="date",
        run_date=run_date,
        args=[next_index],
        id=f"chain_slot_{next_index}",
        replace_existing=True,
    )
    logger.info(f"[chain] 다음 슬롯 {next_index} ({name}) 예약 @ {run_date.strftime('%H:%M:%S')}")


def _schedule_finalize():
    _schedule_next_slot(len(CHAIN_SEQUENCE))


def _finalize_chain():
    """마지막 슬롯 완료 후 스마트스토어 재수집 + 시그널 감지 + 최종 알림."""
    logger.info("[chain] 최종 마무리 시작")
    chain_finished_at = datetime.now(KST)
    store_sync_stats: dict | None = None
    match_stats_total: dict = {}

    if _flask_app_ref is None:
        logger.error("[chain] flask 레퍼런스 없음 — 마무리 중단")
        return

    try:
        from app.store import sync_store_products
        with _flask_app_ref.app_context():
            store_sync_stats = sync_store_products()
        logger.info(f"[chain] 스토어 재수집 완료: {store_sync_stats}")
    except Exception as e:
        logger.exception("[chain] 스토어 재수집 실패")
        store_sync_stats = {"error": str(e)}

    try:
        from app.actions import detect_action_signals
        from app.wholesalers.models import Wholesaler
        with _flask_app_ref.app_context():
            for ws in Wholesaler.query.filter_by(is_active=True).all():
                try:
                    s = detect_action_signals(ws.id)
                    for k, v in (s or {}).items():
                        match_stats_total[k] = match_stats_total.get(k, 0) + (v or 0)
                except Exception as per_err:
                    logger.error(f"[chain] {ws.name} 시그널 감지 실패: {per_err}")
    except Exception as e:
        logger.exception("[chain] 시그널 감지 전체 실패")

    try:
        with _flask_app_ref.app_context():
            slot_results = _collect_today_slot_results()
            started_kst = _today_chain_started_at()
        if started_kst is None:
            started_kst = chain_finished_at.replace(tzinfo=None)
        # DB의 started_at은 naive. KST 기준으로 저장된다고 가정 (utils.kst_now 사용)
        started_display = started_kst.strftime("%Y-%m-%d %H:%M:%S")
        finished_display = chain_finished_at.strftime("%Y-%m-%d %H:%M:%S")
        total_elapsed = int((chain_finished_at.replace(tzinfo=None) - started_kst).total_seconds())

        from notifiers.telegram import notify_chain_final
        notify_chain_final(
            chain_started_at=started_display,
            chain_finished_at=finished_display,
            total_elapsed_seconds=total_elapsed,
            slot_results=slot_results,
            store_sync_stats=store_sync_stats,
            match_stats=match_stats_total,
        )
    except Exception as e:
        logger.warning(f"[chain] 최종 알림 실패 (무시): {e}")

    logger.info("[chain] 체인 완료")


def start_chain_today():
    """매일 19:00 Asia/Seoul cron — 체인 시작 (중복 방지 포함)."""
    if _scheduler_ref is None or _flask_app_ref is None:
        logger.error("[chain] 19:00 cron — 레퍼런스 미초기화, 시작 불가")
        return

    try:
        with _flask_app_ref.app_context():
            resume_idx = _find_resume_index()
    except Exception as e:
        logger.error(f"[chain] 19:00 cron — resume 체크 실패, 처음부터 시작: {e}")
        resume_idx = 0

    if resume_idx >= len(CHAIN_SEQUENCE):
        logger.info("[chain] 오늘 이미 전부 완료 — 19:00 cron 무시")
        return
    if resume_idx > 0:
        logger.info(f"[chain] 오늘 {resume_idx}번까지 완료 상태 — 19:00 cron 무시 (체인 진행 중 가정)")
        return

    logger.info("[chain] 19:00 cron — 슬롯 0부터 시작")
    _execute_chain_slot(0)


def resume_chain_if_needed():
    """스케줄러 프로세스 시작 시 호출 — 오늘 체인 상태에 따라 재개/대기 결정."""
    if _scheduler_ref is None or _flask_app_ref is None:
        logger.error("[chain] resume — 레퍼런스 미초기화")
        return

    try:
        with _flask_app_ref.app_context():
            resume_idx = _find_resume_index()
    except Exception as e:
        logger.error(f"[chain] resume 판단 실패: {e}")
        return

    now_kst = datetime.now(KST)
    today_19 = now_kst.replace(hour=19, minute=0, second=0, microsecond=0)

    if resume_idx >= len(CHAIN_SEQUENCE):
        logger.info("[chain] 오늘 전부 완료 — 내일 19:00 대기")
        return

    if resume_idx == 0:
        if now_kst >= today_19:
            logger.info(f"[chain] 오늘 이력 없음 + 19:00 이후 ({now_kst.strftime('%H:%M')}) — 즉시 시작")
            _scheduler_ref.add_job(
                _execute_chain_slot,
                trigger="date",
                run_date=now_kst + timedelta(seconds=3),
                args=[0],
                id="chain_slot_0",
                replace_existing=True,
            )
        else:
            logger.info(f"[chain] 오늘 이력 없음 + 19:00 전 ({now_kst.strftime('%H:%M')}) — cron 대기")
        return

    _, _code, name, _ = CHAIN_SEQUENCE[resume_idx]
    run_date = now_kst + timedelta(seconds=CHAIN_GAP_SECONDS)
    _scheduler_ref.add_job(
        _execute_chain_slot,
        trigger="date",
        run_date=run_date,
        args=[resume_idx],
        id=f"chain_slot_{resume_idx}",
        replace_existing=True,
    )
    logger.info(
        f"[chain] 재시작 감지 — 슬롯 {resume_idx} ({name})부터 재개 @ {run_date.strftime('%H:%M:%S')}"
    )


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

    from app import create_app
    flask_app = create_app()

    _scheduler_ref = scheduler
    _flask_app_ref = flask_app

    scheduler.add_job(
        start_chain_today,
        trigger="cron",
        hour=19,
        minute=0,
        id="chain_start",
        timezone=TIMEZONE,
    )

    resume_chain_if_needed()

    logger.info(f"[scheduler] 시작 — 매일 19:00 체인 실행 ({TIMEZONE})")
    logger.info("[scheduler] Ctrl+C로 중단")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] 종료")
