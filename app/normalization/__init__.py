from flask import Blueprint
from app.infrastructure import db
from app.normalization.models import NormalizedProduct
from datetime import datetime

normalization_bp = Blueprint("normalization", __name__)

BATCH_SIZE = 500

def save_normalized_products(wholesaler_id: int, run_id: int, items: list):
    saved = 0

    for item in items:
        code = item.get("source_product_code")
        if not code:
            continue

        unique_key = f"{wholesaler_id}_{code}"
        existing = NormalizedProduct.query.filter_by(unique_product_key=unique_key).first()

        if existing:
            existing.product_name = item.get("product_name")
            existing.price = item.get("price")
            existing.supply_price = item.get("supply_price")
            existing.stock_qty = item.get("stock_qty")
            existing.status = item.get("status", "active")
            existing.image_url = item.get("image_url")
            existing.detail_url = item.get("detail_url")
            existing.category_name = item.get("category_name")
            existing.collection_run_id = run_id
            existing.collected_at = datetime.utcnow()
            existing.updated_at = datetime.utcnow()
        else:
            product = NormalizedProduct(
                wholesaler_id=wholesaler_id,
                collection_run_id=run_id,
                source_product_code=code,
                unique_product_key=unique_key,
                product_name=item.get("product_name"),
                price=item.get("price"),
                supply_price=item.get("supply_price"),
                stock_qty=item.get("stock_qty"),
                status=item.get("status", "active"),
                image_url=item.get("image_url"),
                detail_url=item.get("detail_url"),
                category_name=item.get("category_name"),
            )
            db.session.add(product)

        saved += 1
        if saved % BATCH_SIZE == 0:
            db.session.commit()

    db.session.commit()
    return saved