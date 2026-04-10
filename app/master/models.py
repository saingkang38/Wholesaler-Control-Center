from app.infrastructure import db
from app.utils import kst_now


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
    internal_code = db.Column(db.String(128), nullable=True)   # 내부 관리용 코드 (직접 입력)

    product_url        = db.Column(db.String(1024))                # 도매처 상품 링크
    detail_description = db.Column(db.Text)                       # 본문상세설명 (HTML)
    origin             = db.Column(db.String(128))                # 원산지
    brand_name         = db.Column(db.String(256), nullable=True) # 브랜드
    manufacturer       = db.Column(db.String(256), nullable=True) # 제조사
    model_name         = db.Column(db.String(256), nullable=True) # 모델명
    keywords           = db.Column(db.Text, nullable=True)        # 키워드 (줄바꿈 구분)
    tax_type           = db.Column(db.String(32), nullable=True)  # 과세유형 (taxable/tax_free)
    certification      = db.Column(db.Text, nullable=True)        # 인증정보 (JSON)
    additional_images  = db.Column(db.Text, nullable=True)        # 추가이미지 URL (줄바꿈 구분)
    shipping_fee       = db.Column(db.Integer)                    # 배송비
    shipping_condition = db.Column(db.String(256))                # 무료배송조건
    edited_name        = db.Column(db.String(512))                # 가공된 상품명 (NULL = 미가공)
    category_id        = db.Column(db.String(64))                 # 네이버 카테고리 ID (NULL = 미가공)
    is_prep_ready      = db.Column(db.Boolean, default=False)     # True = 등록 가능
    options_text       = db.Column(db.Text, nullable=True)        # 옵션명 (줄바꿈 구분)
    option_diffs       = db.Column(db.Text, nullable=True)        # 옵션가 차액 (줄바꿈 구분)
    option_stocks      = db.Column(db.Text, nullable=True)        # 옵션별 재고 (줄바꿈 구분, 예: "99\n0\n10")

    # 상태: active / missing / discontinued_candidate / discontinued
    current_status = db.Column(db.String(32), default="active", nullable=False)
    first_seen_date = db.Column(db.Date)
    last_seen_date = db.Column(db.Date)
    missing_days = db.Column(db.Integer, default=0)
    last_status_change_date = db.Column(db.Date)
    discontinued_flag = db.Column(db.Boolean, default=False)

    created_at = db.Column(db.DateTime, default=kst_now)
    updated_at = db.Column(db.DateTime, default=kst_now, onupdate=kst_now)

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

    created_at = db.Column(db.DateTime, default=kst_now)
