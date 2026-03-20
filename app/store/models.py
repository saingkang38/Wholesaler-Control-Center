from app.infrastructure import db
from datetime import datetime


class NaverStore(db.Model):
    __tablename__ = "naver_stores"

    id = db.Column(db.Integer, primary_key=True)
    store_name = db.Column(db.String(128), nullable=False)   # 표시용 이름 (예: 심플홈샵)
    client_id = db.Column(db.String(128), nullable=False)
    client_secret = db.Column(db.String(256), nullable=False)
    is_active = db.Column(db.Boolean, default=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    products = db.relationship("StoreProduct", backref="naver_store", lazy="dynamic")


class StoreProduct(db.Model):
    __tablename__ = "store_products"

    id = db.Column(db.Integer, primary_key=True)

    # 어느 스토어 상품인지
    naver_store_id = db.Column(db.Integer, db.ForeignKey("naver_stores.id"), nullable=True)

    # 스마트스토어 기준
    origin_product_no = db.Column(db.BigInteger)             # 원상품번호
    channel_product_no = db.Column(db.BigInteger)            # 채널상품번호
    seller_management_code = db.Column(db.String(128))       # 판매자관리코드 (= 도매처 상품코드)
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

    __table_args__ = (
        db.UniqueConstraint("naver_store_id", "origin_product_no", name="uq_store_product"),
    )


class ProductExclusion(db.Model):
    __tablename__ = "product_exclusions"

    id = db.Column(db.Integer, primary_key=True)
    store_product_id = db.Column(db.Integer, db.ForeignKey("store_products.id"), unique=True, nullable=False)
    reason = db.Column(db.String(256))  # 예외 사유 (선택)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    store_product = db.relationship("StoreProduct", backref=db.backref("exclusion", uselist=False))
