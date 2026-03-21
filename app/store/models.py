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


class SyncLog(db.Model):
    __tablename__ = "sync_logs"

    id = db.Column(db.Integer, primary_key=True)
    naver_store_id = db.Column(db.Integer, db.ForeignKey("naver_stores.id"), nullable=True)
    action = db.Column(db.String(32))   # FULL_SYNC / REMATCH
    result = db.Column(db.String(32))   # success / error
    detail = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    naver_store = db.relationship("NaverStore", backref="sync_logs")


class DeliveryPreset(db.Model):
    __tablename__ = "delivery_presets"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)           # 예: 무료배송, 3000원 균일
    delivery_fee_type = db.Column(db.String(32), nullable=False)  # FREE | PAID | CONDITIONAL_FREE
    base_fee = db.Column(db.Integer, default=0)                # 기본 배송비
    free_condition_amount = db.Column(db.Integer, default=0)   # 조건부 무료 기준금액
    delivery_fee_pay_type = db.Column(db.String(16), default="PREPAID")  # PREPAID | COLLECT
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class BulkRegisterJob(db.Model):
    __tablename__ = "bulk_register_jobs"

    id = db.Column(db.Integer, primary_key=True)
    naver_store_id = db.Column(db.Integer, db.ForeignKey("naver_stores.id"), nullable=False)
    status = db.Column(db.String(16), default="pending")  # pending|running|done|error
    total = db.Column(db.Integer, default=0)
    completed = db.Column(db.Integer, default=0)
    failed = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    naver_store = db.relationship("NaverStore", backref="bulk_jobs")
    items = db.relationship("BulkRegisterItem", backref="job", lazy="dynamic")


class BulkRegisterItem(db.Model):
    __tablename__ = "bulk_register_items"

    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("bulk_register_jobs.id"), nullable=False)
    master_product_id = db.Column(db.Integer, db.ForeignKey("master_products.id"), nullable=False)
    status = db.Column(db.String(16), default="pending")  # pending|success|error
    error_msg = db.Column(db.Text)
    origin_product_no = db.Column(db.BigInteger)

    master = db.relationship("MasterProduct")


class ProductExclusion(db.Model):
    __tablename__ = "product_exclusions"

    id = db.Column(db.Integer, primary_key=True)
    store_product_id = db.Column(db.Integer, db.ForeignKey("store_products.id"), unique=True, nullable=False)
    reason = db.Column(db.String(256))  # 예외 사유 (선택)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    store_product = db.relationship("StoreProduct", backref=db.backref("exclusion", uselist=False))
