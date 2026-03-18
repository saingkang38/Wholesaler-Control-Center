from app.infrastructure import db
from datetime import datetime

class NormalizedProduct(db.Model):
    __tablename__ = "normalized_products"

    id = db.Column(db.Integer, primary_key=True)
    wholesaler_id = db.Column(db.Integer, db.ForeignKey("wholesalers.id"), nullable=False)
    collection_run_id = db.Column(db.Integer, db.ForeignKey("collection_runs.id"))
    source_product_code = db.Column(db.String(128), nullable=False)
    unique_product_key = db.Column(db.String(256), nullable=False)
    product_name = db.Column(db.String(512))
    option_name = db.Column(db.String(256))
    price = db.Column(db.Integer)
    supply_price = db.Column(db.Integer)
    stock_qty = db.Column(db.Integer)
    status = db.Column(db.String(32), default="active")  # active / out_of_stock / discontinued / unknown
    image_url = db.Column(db.String(512))
    detail_url = db.Column(db.String(512))
    category_name = db.Column(db.String(256))
    brand_name = db.Column(db.String(128))
    is_active = db.Column(db.Boolean, default=True)
    collected_at = db.Column(db.DateTime, default=datetime.utcnow)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    wholesaler = db.relationship("Wholesaler", backref="products")