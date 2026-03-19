from app.infrastructure import db
from datetime import datetime


class StoreProduct(db.Model):
    __tablename__ = "store_products"

    id = db.Column(db.Integer, primary_key=True)

    # 스마트스토어 기준
    origin_product_no = db.Column(db.BigInteger, unique=True)   # 원상품번호
    channel_product_no = db.Column(db.BigInteger)               # 채널상품번호
    seller_management_code = db.Column(db.String(128))          # 판매자관리코드 (= 도매처 상품코드)
    product_name = db.Column(db.String(512))

    # 판매 상태: SALE / SUSPENSION / CLOSE
    store_status = db.Column(db.String(32))
    sale_price = db.Column(db.Integer)

    # 마스터 매칭
    master_product_id = db.Column(db.Integer, db.ForeignKey("master_products.id"), nullable=True)

    last_synced_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    master = db.relationship("MasterProduct", backref="store_products")
