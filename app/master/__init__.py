import json
from datetime import date, datetime
from flask import Blueprint
from app.infrastructure import db
from app.master.models import MasterProduct, ProductEvent

master_bp = Blueprint("master", __name__)

# 연속 미수집 N일 이상 → 상태 전환 기준
MISSING_DAYS_CANDIDATE = 3    # missing_candidate
MISSING_DAYS_DISCONTINUED = 7  # discontinued_candidate


def process_master_update(wholesaler_id: int, items: list, snapshot_date: date = None) -> dict:
    if snapshot_date is None:
        snapshot_date = datetime.utcnow().date()

    today_map = {
        item["source_product_code"]: item
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
    }

    existing = MasterProduct.query.filter_by(wholesaler_id=wholesaler_id).all()
    existing_map = {p.supplier_product_code: p for p in existing}
    existing_codes = set(existing_map.keys())

    new_events = []

    # 1. 오늘 수집된 상품 처리
    for code, item in today_map.items():
        if code not in existing_map:
            # 신규 상품
            master = MasterProduct(
                wholesaler_id=wholesaler_id,
                supplier_product_code=code,
                product_name=item.get("product_name"),
                price=item.get("price"),
                supply_price=item.get("supply_price"),
                image_url=item.get("image_url"),
                category_name=item.get("category_name"),
                current_status="active",
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

            # 이전에 미수집/단종후보였으면 재입고
            if prev_status in ("missing", "discontinued_candidate"):
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

            # 마스터 갱신
            master.last_seen_date = snapshot_date
            master.missing_days = 0
            if master.current_status != "active":
                master.last_status_change_date = snapshot_date
            master.current_status = "active"
            master.product_name = new_name or master.product_name
            master.price = new_price or master.price
            master.supply_price = item.get("supply_price") or master.supply_price
            master.image_url = new_img or master.image_url
            master.category_name = item.get("category_name") or master.category_name

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
    print(f"[master] 업데이트 완료: {stats}")
    return stats
