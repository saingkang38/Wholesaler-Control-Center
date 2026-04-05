import logging
from app.infrastructure import db
from app.utils import kst_now
from app.execution_logs.models import CollectionRun
from app.wholesalers.models import Wholesaler

logger = logging.getLogger(__name__)


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
    from collectors.dometopia import DometopiaCollector
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
        "dometopia": DometopiaCollector,
    }


def _save_desktop_xlsx(wholesaler_code: str, items: list):
    import openpyxl
    from pathlib import Path

    if not items:
        return

    try:
        desktop = Path(__file__).resolve().parents[2] / "downloads" / wholesaler_code
        desktop.mkdir(parents=True, exist_ok=True)

        COLUMN_MAP = {
            "source_product_code": "상품번호",
            "product_name": "상품명",
            "price": "가격",
            "supply_price": "공급가",
            "image_url": "대표이미지",
            "detail_url": "상품URL",
            "product_url": "상품URL",
            "category_name": "카테고리",
            "origin": "원산지",
            "detail_description": "상세설명",
            "shipping_fee": "배송비",
            "shipping_condition": "조건부 무료배송",
            "own_code": "자체코드",
            "status": "상태",
            "stock_qty": "재고",
        }

        all_keys = []
        for item in items:
            for k in item:
                if k != "extra" and k not in all_keys:
                    all_keys.append(k)
            for k in (item.get("extra") or {}):
                if k not in all_keys:
                    all_keys.append(k)

        headers = [COLUMN_MAP.get(k, k) for k in all_keys]

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(headers)

        STATUS_KO = {"active": "정상", "out_of_stock": "품절", "discontinued": "단종"}

        def to_cell(v, key=None):
            if v is None:
                return ""
            if key == "status":
                return STATUS_KO.get(str(v), str(v))
            if isinstance(v, (list, dict)):
                import json
                return json.dumps(v, ensure_ascii=False)
            return v

        for item in items:
            extra = item.get("extra") or {}
            row = []
            for k in all_keys:
                if k in item and k != "extra":
                    row.append(to_cell(item[k], key=k))
                else:
                    row.append(to_cell(extra.get(k), key=k))
            ws.append(row)

        timestamp = kst_now().strftime("%Y%m%d_%H%M%S")
        path = desktop / f"{wholesaler_code}_{timestamp}.xlsx"
        wb.save(str(path))
        logger.info(f"[orchestrator] 데스크탑 저장: {path} ({len(items)}건, {len(all_keys)}컬럼)")
    except Exception as e:
        logger.warning(f"[orchestrator] 데스크탑 저장 실패 (무시): {e}")


def _save_raw_json(wholesaler_code: str, items: list):
    import json
    from pathlib import Path

    save_dir = Path.home() / "OneDrive" / "supplier_sync" / wholesaler_code
    save_dir.mkdir(parents=True, exist_ok=True)
    timestamp = kst_now().strftime("%Y%m%d_%H%M%S")
    path = save_dir / f"{wholesaler_code}_{timestamp}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        logger.info(f"[orchestrator] 원본 저장: {path} ({len(items)}건)")
    except Exception as e:
        logger.warning(f"[orchestrator] 원본 저장 실패 (무시): {e}")


def run_collection(wholesaler_code: str, trigger_type: str = "manual", user_id: int = None):
    wholesaler = Wholesaler.query.filter_by(code=wholesaler_code, is_active=True).first()
    if not wholesaler:
        return {"success": False, "error": f"도매처를 찾을 수 없음: {wholesaler_code}"}

    registry = _build_registry()
    collector_class = registry.get(wholesaler_code)
    if not collector_class:
        return {"success": False, "error": f"{wholesaler_code} collector 미등록"}

    # 설정 미완료 여부를 미리 확인 (CollectionRun 생성 전)
    _CONFIG_KEYWORDS = ("미설정", "환경변수 없음", "환경변수없음", "not configured", "LOGIN_ID", "LOGIN_PASSWORD")

    run = CollectionRun(
        wholesaler_id=wholesaler.id,
        trigger_type=trigger_type,
        status="running",
        started_at=kst_now(),
        created_by_user_id=user_id,
    )
    db.session.add(run)
    db.session.commit()

    master_stats = {}  # try 블록 외부 선언 — 예외 발생 시에도 반환값 안전
    try:
        collector = collector_class()
        result = collector.run()

        # 설정 미완료로 수집 자체를 건너뛴 경우 — 알림 없이 로그만
        error_msg = result.get("error_summary") or result.get("error") or ""
        if not result.get("success") and any(kw in error_msg for kw in _CONFIG_KEYWORDS):
            run.status = "skipped"
            run.error_summary = error_msg
            run.finished_at = kst_now()
            db.session.commit()
            logger.info(f"[orchestrator] {wholesaler_code} 설정 미완료 — 수집 건너뜀: {error_msg}")
            return {"success": False, "not_configured": True, "error": error_msg}

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
            logger.info(f"[orchestrator] 저장 완료: {saved}건")
            master_stats = process_master_update(wholesaler.id, result["items"])
            logger.info(f"[orchestrator] 마스터 업데이트 완료")
            from app.actions import detect_action_signals
            detect_action_signals(wholesaler.id)
            logger.info(f"[orchestrator] 액션 시그널 갱신 완료")

    except Exception as e:
        logger.error(f"[orchestrator] 오류 발생: {e}")
        run.status = "failed"
        run.error_summary = str(e)
        result = {"success": False, "error": str(e)}

    finally:
        run.finished_at = kst_now()
        db.session.commit()

    return {
        "success": run.status == "success",
        "run_id": run.id,
        "total_items": run.total_items,
        "master_stats": master_stats,
        "error": run.error_summary,
    }
