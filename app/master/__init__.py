import json
import logging
from datetime import date
from app.utils import kst_now
from flask import Blueprint
from app.infrastructure import db
from app.master.models import MasterProduct, ProductEvent

master_bp = Blueprint("master", __name__)
logger = logging.getLogger(__name__)


def _normalize_diffs(diffs: str | None) -> str | None:
    """option_diffs가 전부 0이면 None 반환 (추가금 없음으로 분류)."""
    if not diffs or not diffs.strip():
        return None
    try:
        if all(float(v.strip()) == 0 for v in diffs.split("\n") if v.strip()):
            return None
    except ValueError:
        return None
    return diffs


@master_bp.route("/changes")
def changes():
    from flask_login import login_required, current_user
    from flask import render_template, request
    from app.wholesalers.models import Wholesaler

    CHANGE_TYPES = ["IMAGE_CHANGE", "NAME_CHANGE", "DETAIL_CHANGE", "SHIPPING_CHANGE", "PRICE_CHANGE"]

    wholesaler_id = request.args.get("wholesaler_id", type=int)
    event_type = request.args.get("event_type", "")
    page = request.args.get("page", 1, type=int)
    per_page = 50

    query = ProductEvent.query.filter(ProductEvent.event_type.in_(CHANGE_TYPES))
    if wholesaler_id:
        query = query.join(MasterProduct).filter(MasterProduct.wholesaler_id == wholesaler_id)
    if event_type and event_type in CHANGE_TYPES:
        query = query.filter(ProductEvent.event_type == event_type)

    query = query.order_by(ProductEvent.event_date.desc(), ProductEvent.id.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    wholesalers = Wholesaler.query.filter_by(is_active=True).order_by(Wholesaler.name).all()

    return render_template(
        "changes.html",
        events=pagination.items,
        pagination=pagination,
        wholesalers=wholesalers,
        selected_wholesaler_id=wholesaler_id,
        selected_event_type=event_type,
        change_types=CHANGE_TYPES,
    )

# 연속 미수집 N일 이상 → 상태 전환 기준
MISSING_DAYS_CANDIDATE = 3    # missing_candidate
MISSING_DAYS_DISCONTINUED = 7  # discontinued_candidate


def process_master_update(wholesaler_id: int, items: list, snapshot_date: date = None) -> dict:
    if snapshot_date is None:
        snapshot_date = kst_now().date()

    from app.wholesalers.models import Wholesaler
    wholesaler = Wholesaler.query.get(wholesaler_id)
    prefix = (wholesaler.prefix or "") if wholesaler else ""

    today_map = {
        f"{prefix}{item['source_product_code']}": item
        for item in items
        if item.get("source_product_code")
    }

    stats = {
        "new": 0,
        "restocked": 0,
        "missing": 0,
        "discontinued_candidate": 0,
        "price_change": 0,
        "image_change": 0,
        "name_change": 0,
        "detail_change": 0,
        "shipping_change": 0,
    }

    existing = MasterProduct.query.filter_by(wholesaler_id=wholesaler_id).all()
    existing_map = {p.supplier_product_code: p for p in existing}
    existing_codes = set(existing_map.keys())

    new_events = []

    # 1. 오늘 수집된 상품 처리
    for code, item in today_map.items():
        if code not in existing_map:
            # 신규 상품
            extra = item.get("extra") or {}
            _opt_text = extra.get("옵션")
            _opt_diffs = extra.get("옵션가")
            _opt_stocks = extra.get("옵션재고")
            _init_status = "active"
            if item.get("status") == "out_of_stock" and not _opt_text:
                _init_status = "out_of_stock"
            master = MasterProduct(
                wholesaler_id=wholesaler_id,
                supplier_product_code=code,
                product_name=item.get("product_name"),
                price=item.get("price"),
                supply_price=item.get("supply_price"),
                image_url=item.get("image_url"),
                category_name=item.get("category_name"),
                detail_description=item.get("detail_description"),
                product_url=item.get("product_url") or item.get("detail_url"),
                origin=item.get("origin"),
                shipping_fee=item.get("shipping_fee"),
                shipping_condition=item.get("shipping_condition"),
                options_text=_opt_text if isinstance(_opt_text, str) else None,
                option_diffs=_normalize_diffs(_opt_diffs if isinstance(_opt_diffs, str) else None),
                option_stocks=_opt_stocks if isinstance(_opt_stocks, str) else None,
                current_status=_init_status,
                first_seen_date=snapshot_date,
                last_seen_date=snapshot_date,
                missing_days=0,
                last_status_change_date=snapshot_date,
            )
            db.session.add(master)
            db.session.flush()
            new_events.append(ProductEvent(
                master_product_id=master.id,
                event_type="NEW",
                event_date=snapshot_date,
                after_value=json.dumps({"price": item.get("price")}, ensure_ascii=False),
            ))
            stats["new"] += 1

        else:
            master = existing_map[code]
            prev_status = master.current_status

            # 이전에 미수집/단종후보/품절이었으면 재입고
            if prev_status in ("missing", "discontinued_candidate", "out_of_stock"):
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="RESTOCKED",
                    event_date=snapshot_date,
                    before_value=json.dumps({
                        "status": prev_status,
                        "missing_days": master.missing_days
                    }, ensure_ascii=False),
                    after_value=json.dumps({"status": "active"}, ensure_ascii=False),
                ))
                stats["restocked"] += 1

            # 가격 변동
            new_price = item.get("price")
            if master.price and new_price and master.price != new_price:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="PRICE_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"price": master.price}, ensure_ascii=False),
                    after_value=json.dumps({"price": new_price}, ensure_ascii=False),
                ))
                stats["price_change"] += 1

            # 이미지 변동
            new_img = item.get("image_url")
            if master.image_url and new_img and master.image_url != new_img:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="IMAGE_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"image_url": master.image_url}, ensure_ascii=False),
                    after_value=json.dumps({"image_url": new_img}, ensure_ascii=False),
                ))
                stats["image_change"] += 1

            # 상품명 변동
            new_name = item.get("product_name")
            if master.product_name and new_name and master.product_name != new_name:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="NAME_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"name": master.product_name}, ensure_ascii=False),
                    after_value=json.dumps({"name": new_name}, ensure_ascii=False),
                ))
                stats["name_change"] += 1

            # 상세페이지 변동
            new_detail = item.get("detail_description")
            if master.detail_description and new_detail and master.detail_description != new_detail:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="DETAIL_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"chars": len(master.detail_description)}, ensure_ascii=False),
                    after_value=json.dumps({"chars": len(new_detail), "url": master.product_url or ""}, ensure_ascii=False),
                ))
                stats["detail_change"] += 1

            # 배송비/배송조건 변동
            new_shipping_fee = item.get("shipping_fee")
            new_shipping_cond = item.get("shipping_condition")
            shipping_fee_changed = (
                new_shipping_fee is not None
                and master.shipping_fee is not None
                and master.shipping_fee != new_shipping_fee
            )
            shipping_cond_changed = (
                new_shipping_cond
                and master.shipping_condition
                and master.shipping_condition != new_shipping_cond
            )
            if shipping_fee_changed or shipping_cond_changed:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="SHIPPING_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({
                        "shipping_fee": master.shipping_fee,
                        "shipping_condition": master.shipping_condition,
                    }, ensure_ascii=False),
                    after_value=json.dumps({
                        "shipping_fee": new_shipping_fee,
                        "shipping_condition": new_shipping_cond,
                    }, ensure_ascii=False),
                ))
                stats["shipping_change"] += 1

            # 옵션 변동 (문자열 형식만 저장 — 도매처별 표준화 전까지 리스트는 무시)
            extra = item.get("extra") or {}
            _raw_options = extra.get("옵션")
            _raw_diffs = extra.get("옵션가")
            _raw_stocks = extra.get("옵션재고")
            new_options = _raw_options if isinstance(_raw_options, str) else None
            new_diffs = _normalize_diffs(_raw_diffs if isinstance(_raw_diffs, str) else None)
            new_option_stocks = _raw_stocks if isinstance(_raw_stocks, str) else None

            if master.option_diffs and new_diffs and master.option_diffs != new_diffs:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="OPTION_PRICE_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"option_diffs": master.option_diffs}, ensure_ascii=False),
                    after_value=json.dumps({"option_diffs": new_diffs}, ensure_ascii=False),
                ))

            if master.option_stocks is not None and new_option_stocks is not None and master.option_stocks != new_option_stocks:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="OPTION_STOCK_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"option_stocks": master.option_stocks}, ensure_ascii=False),
                    after_value=json.dumps({"option_stocks": new_option_stocks}, ensure_ascii=False),
                ))

            # 마스터 갱신
            master.last_seen_date = snapshot_date
            master.missing_days = 0

            # 품절 상태 처리: 옵션 없는 상품이 OOS로 수집되면 out_of_stock으로 표시
            item_status = item.get("status", "active")
            if item_status == "out_of_stock" and not new_options:
                if master.current_status != "out_of_stock":
                    master.last_status_change_date = snapshot_date
                master.current_status = "out_of_stock"
            else:
                if master.current_status != "active":
                    master.last_status_change_date = snapshot_date
                master.current_status = "active"

            master.option_stocks = new_option_stocks
            if new_name:
                master.product_name = new_name
            if new_price is not None:
                master.price = new_price
            new_supply = item.get("supply_price")
            if new_supply is not None:
                master.supply_price = new_supply
            if new_img:
                master.image_url = new_img
            new_cat = item.get("category_name")
            if new_cat:
                master.category_name = new_cat
            if item.get("detail_description"):
                master.detail_description = item.get("detail_description")
            new_url = item.get("product_url") or item.get("detail_url")
            if new_url:
                master.product_url = new_url
            if item.get("origin"):
                master.origin = item.get("origin")
            if item.get("shipping_fee") is not None:
                master.shipping_fee = item.get("shipping_fee")
            if item.get("shipping_condition"):
                master.shipping_condition = item.get("shipping_condition")
            extra = item.get("extra") or {}
            if item.get("brand_name") or extra.get("브랜드"):
                master.brand_name = item.get("brand_name") or extra.get("브랜드")
            if item.get("manufacturer") or extra.get("제조사"):
                master.manufacturer = item.get("manufacturer") or extra.get("제조사")
            if item.get("model_name") or extra.get("모델명"):
                master.model_name = item.get("model_name") or extra.get("모델명")
            if item.get("keywords") or extra.get("키워드"):
                master.keywords = item.get("keywords") or extra.get("키워드")
            if item.get("tax_type") or extra.get("과세여부"):
                master.tax_type = item.get("tax_type") or extra.get("과세여부")
            if item.get("certification") or extra.get("인증정보"):
                import json as _json
                val = item.get("certification") or extra.get("인증정보")
                master.certification = _json.dumps(val, ensure_ascii=False) if isinstance(val, (list, dict)) else str(val)
            # 추가이미지: extra["추가이미지1"..5] → 줄바꿈 구분 문자열
            add_imgs = [
                extra.get("추가이미지1") or extra.get("additional_image_1"),
                extra.get("추가이미지2") or extra.get("additional_image_2"),
                extra.get("추가이미지3") or extra.get("additional_image_3"),
                extra.get("추가이미지4") or extra.get("additional_image_4"),
                extra.get("추가이미지5") or extra.get("additional_image_5"),
            ]
            add_imgs = [u for u in add_imgs if u]
            if add_imgs:
                master.additional_images = "\n".join(add_imgs)
            master.options_text = new_options
            master.option_diffs = new_diffs

    # 2. 오늘 수집에서 빠진 상품 처리 (미수집)
    missing_codes = existing_codes - set(today_map.keys())
    for code in missing_codes:
        master = existing_map[code]

        if master.current_status == "discontinued":
            continue  # 이미 단종 확정이면 건너뜀

        master.missing_days = (master.missing_days or 0) + 1

        if master.missing_days >= MISSING_DAYS_DISCONTINUED:
            if master.current_status != "discontinued_candidate":
                master.current_status = "discontinued_candidate"
                master.last_status_change_date = snapshot_date
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="DISCONTINUED_CANDIDATE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"missing_days": master.missing_days}, ensure_ascii=False),
                ))
                stats["discontinued_candidate"] += 1

        elif master.missing_days >= MISSING_DAYS_CANDIDATE:
            if master.current_status != "missing":
                master.current_status = "missing"
                master.last_status_change_date = snapshot_date
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="MISSING",
                    event_date=snapshot_date,
                    before_value=json.dumps({"missing_days": master.missing_days}, ensure_ascii=False),
                ))
                stats["missing"] += 1

    for event in new_events:
        db.session.add(event)

    db.session.commit()
    logger.info(f"[master] 업데이트 완료: {stats}")
    return stats
