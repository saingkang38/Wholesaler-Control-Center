from datetime import datetime
from app.infrastructure import db
from app.execution_logs.models import CollectionRun
from app.wholesalers.models import Wholesaler


def _build_registry():
    from collectors.ownerclan import OwnerclanCollector
    from collectors.jtckorea import JtckoreaCollector
    from collectors.metaldiy import MetaldiyCollector
    from collectors.ds1008 import Ds1008Collector
    from collectors.hitdesign import HitdesignCollector
    from collectors.chingudome import ChingudomeCollector
    from collectors.zentrade import ZentraldeCollector
    from collectors.mro3 import Mro3Collector
    from collectors.feelwoo import FeelwooCollector
    from collectors.sikjaje import SikjajeCollector
    from collectors.onch3 import Onch3Collector
    return {
        "ownerclan": OwnerclanCollector,
        "jtckorea": JtckoreaCollector,
        "metaldiy": MetaldiyCollector,
        "ds1008": Ds1008Collector,
        "hitdesign": HitdesignCollector,
        "chingudome": ChingudomeCollector,
        "zentrade": ZentraldeCollector,
        "mro3": Mro3Collector,
        "feelwoo": FeelwooCollector,
        "sikjaje": SikjajeCollector,
        "onch3": Onch3Collector,
    }


def _save_desktop_xlsx(wholesaler_code: str, items: list):
    import openpyxl
    from pathlib import Path
    from datetime import datetime

    if not items:
        return

    try:
        desktop = Path.home() / "Desktop" / "예시"
        desktop.mkdir(parents=True, exist_ok=True)

        all_keys = []
        for item in items:
            for k in item:
                if k != "extra" and k not in all_keys:
                    all_keys.append(k)
            for k in (item.get("extra") or {}):
                if k not in all_keys:
                    all_keys.append(k)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(all_keys)

        for item in items:
            extra = item.get("extra") or {}
            row = []
            for k in all_keys:
                if k in item and k != "extra":
                    row.append(item[k])
                else:
                    row.append(extra.get(k))
            ws.append(row)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = desktop / f"{wholesaler_code}_{timestamp}.xlsx"
        wb.save(str(path))
        print(f"[orchestrator] 데스크탑 저장: {path} ({len(items)}건, {len(all_keys)}컬럼)")
    except Exception as e:
        print(f"[orchestrator] 데스크탑 저장 실패 (무시): {e}")


def _save_raw_json(wholesaler_code: str, items: list):
    import json
    from pathlib import Path
    from datetime import datetime

    save_dir = Path.home() / "OneDrive" / "supplier_sync" / wholesaler_code
    save_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = save_dir / f"{wholesaler_code}_{timestamp}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"[orchestrator] 원본 저장: {path} ({len(items)}건)")
    except Exception as e:
        print(f"[orchestrator] 원본 저장 실패 (무시): {e}")


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

    master_stats = {}
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
            _save_raw_json(wholesaler_code, result["items"])
            _save_desktop_xlsx(wholesaler_code, result["items"])
            from app.normalization import save_normalized_products
            from app.master import process_master_update
            saved = save_normalized_products(
                wholesaler_id=wholesaler.id,
                run_id=run.id,
                items=result["items"]
            )
            print(f"[orchestrator] 저장 완료: {saved}건")
            master_stats = process_master_update(wholesaler.id, result["items"])
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
        "total_items": run.total_items,
        "master_stats": master_stats,
        "error": run.error_summary,
    }
