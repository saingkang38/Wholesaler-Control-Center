from datetime import datetime
from app.infrastructure import db
from app.execution_logs.models import CollectionRun
from app.wholesalers.models import Wholesaler


def _build_registry():
    from collectors.ownerclan import OwnerclanCollector
    from collectors.jtckorea import JtckoreaCollector
    return {
        "ownerclan": OwnerclanCollector,
        "jtckorea": JtckoreaCollector,
    }


def run_collection(wholesaler_code: str, trigger_type: str = "manual", user_id: int = None):
    wholesaler = Wholesaler.query.filter_by(code=wholesaler_code, is_active=True).first()
    if not wholesaler:
        return {"success": False, "error": f"도매처를 찾을 수 없음: {wholesaler_code}"}

    registry = _build_registry()
    collector_class = registry.get(wholesaler_code)
    if not collector_class:
        return {"success": False, "error": f"{wholesaler_code} collector 미등록"}

    run = CollectionRun(
        wholesaler_id=wholesaler.id,
        trigger_type=trigger_type,
        status="running",
        started_at=datetime.now(),
        created_by_user_id=user_id,
    )
    db.session.add(run)
    db.session.commit()

    try:
        collector = collector_class()
        result = collector.run()

        run.status = "success" if result.get("success") else "failed"
        run.total_items = result.get("total_items", 0)
        run.total_pages = result.get("total_pages", 0)
        run.success_count = result.get("success_count", 0)
        run.fail_count = result.get("fail_count", 0)
        run.error_summary = result.get("error_summary")

        if result.get("success") and result.get("items"):
            from app.normalization import save_normalized_products
            from app.master import process_master_update
            saved = save_normalized_products(
                wholesaler_id=wholesaler.id,
                run_id=run.id,
                items=result["items"]
            )
            print(f"[orchestrator] 저장 완료: {saved}건")
            process_master_update(wholesaler.id, result["items"])
            print(f"[orchestrator] 마스터 업데이트 완료")

    except Exception as e:
        print(f"[orchestrator] 오류 발생: {e}")
        run.status = "failed"
        run.error_summary = str(e)
        result = {"success": False, "error": str(e)}

    finally:
        run.finished_at = datetime.now()
        db.session.commit()

    return {
        "success": run.status == "success",
        "run_id": run.id,
        "error": run.error_summary,
    }
