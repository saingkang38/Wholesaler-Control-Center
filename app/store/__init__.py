from flask import Blueprint
from datetime import datetime
from app.infrastructure import db
from app.store.models import StoreProduct
from app.master.models import MasterProduct

store_bp = Blueprint("store", __name__)


def sync_store_products(wholesaler_id: int) -> dict:
    from store.naver import get_all_products

    raw_items = get_all_products()
    stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    for item in raw_items:
        origin_no = item.get("originProductNo")
        channel_no = item.get("channelProductNo")
        seller_code = item.get("sellerManagementCode", "").strip()
        name = item.get("name", "")
        status = item.get("statusType", "")
        price = item.get("salePrice")

        if not origin_no:
            continue

        store = StoreProduct.query.filter_by(origin_product_no=origin_no).first()

        if store:
            store.store_status = status
            store.sale_price = price
            store.product_name = name
            store.seller_management_code = seller_code
            store.last_synced_at = datetime.utcnow()
            stats["updated"] += 1
        else:
            store = StoreProduct(
                origin_product_no=origin_no,
                channel_product_no=channel_no,
                seller_management_code=seller_code,
                product_name=name,
                store_status=status,
                sale_price=price,
                last_synced_at=datetime.utcnow(),
            )
            db.session.add(store)
            db.session.flush()
            stats["created"] += 1

        # 마스터 매칭 (판매자관리코드 = 도매처 상품코드)
        if seller_code:
            master = MasterProduct.query.filter_by(
                wholesaler_id=wholesaler_id,
                supplier_product_code=seller_code
            ).first()
            if master:
                store.master_product_id = master.id
                stats["matched"] += 1
            else:
                stats["unmatched"] += 1

    db.session.commit()
    print(f"[store] 동기화 완료: {stats}")
    return stats
