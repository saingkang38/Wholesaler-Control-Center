from app.infrastructure import db
from datetime import datetime


class MasterProduct(db.Model):
    __tablename__ = "master_products"

    id = db.Column(db.Integer, primary_key=True)
    wholesaler_id = db.Column(db.Integer, db.ForeignKey("wholesalers.id"), nullable=False)
    supplier_product_code = db.Column(db.String(128), nullable=False)

    product_name = db.Column(db.String(512))
    price = db.Column(db.Integer)
    supply_price = db.Column(db.Integer)
    image_url = db.Column(db.String(512))
    category_name = db.Column(db.String(256))

    # 상태: active / missing / discontinued_candidate / discontinued
    current_status = db.Column(db.String(32), default="active", nullable=False)
    first_seen_date = db.Column(db.Date)
    last_seen_date = db.Column(db.Date)
    missing_days = db.Column(db.Integer, default=0)
    last_status_change_date = db.Column(db.Date)
    discontinued_flag = db.Column(db.Boolean, default=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    wholesaler = db.relationship("Wholesaler", backref="master_products")
    events = db.relationship("ProductEvent", backref="product", lazy="dynamic")

    __table_args__ = (
        db.UniqueConstraint("wholesaler_id", "supplier_product_code", name="uq_master_product"),
    )


class ProductEvent(db.Model):
    __tablename__ = "product_events"

    id = db.Column(db.Integer, primary_key=True)
    master_product_id = db.Column(db.Integer, db.ForeignKey("master_products.id"), nullable=False)

    # NEW / RESTOCKED / MISSING / PRICE_CHANGE / IMAGE_CHANGE / NAME_CHANGE
    # DISCONTINUED_CANDIDATE / DISCONTINUED
    event_type = db.Column(db.String(32), nullable=False)
    event_date = db.Column(db.Date, nullable=False)

    before_value = db.Column(db.Text)   # JSON
    after_value = db.Column(db.Text)    # JSON
    note = db.Column(db.String(256))

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
