from app.infrastructure import db
from app.utils import kst_now


class AddonProduct(db.Model):
    """
    "추가상품" 정책이 결정된 옵션의 절대가격 + 네이버 추가상품 ID를 보관.
    수집 재실행 후에도 이 테이블을 기준으로 네이버 supplementProductInfo 를 유지한다.
    """
    __tablename__ = "addon_products"

    id = db.Column(db.Integer, primary_key=True)
    master_product_id    = db.Column(db.Integer, db.ForeignKey("master_products.id"), nullable=False, index=True)
    option_name          = db.Column(db.String(300), nullable=False)   # 도매처 옵션명 (policy key)
    wholesaler_price     = db.Column(db.Integer)                       # 도매처 기준 절대가격
    naver_supplement_id  = db.Column(db.BigInteger)                    # 네이버가 부여한 추가상품 ID
    last_synced_at       = db.Column(db.DateTime)
    created_at           = db.Column(db.DateTime, default=kst_now)
    updated_at           = db.Column(db.DateTime, default=kst_now, onupdate=kst_now)

    master = db.relationship("MasterProduct", backref="addon_products")

    __table_args__ = (
        db.UniqueConstraint("master_product_id", "option_name", name="uq_addon_product"),
    )


class OptionReviewPolicy(db.Model):
    __tablename__ = "option_review_policy"

    id = db.Column(db.Integer, primary_key=True)
    master_product_id = db.Column(
        db.Integer, db.ForeignKey("master_products.id"), nullable=False, index=True
    )
    option_name       = db.Column(db.String(300), nullable=False)
    option_price      = db.Column(db.Integer)
    main_option_name  = db.Column(db.String(300))
    main_option_price = db.Column(db.Integer)
    diff_pct          = db.Column(db.Float)           # 메인 대비 차이율(%)  음수
    cheap_option_count= db.Column(db.Integer)         # 해당 상품의 저가옵션 총 개수
    accessory_keywords= db.Column(db.String(300))     # 매칭된 키워드 (comma)
    risk_score        = db.Column(db.Integer)
    risk_grade        = db.Column(db.String(20))      # 최우선확인/강한의심/검토필요

    # 검토 결과
    decision   = db.Column(db.String(20), default="pending")  # pending/keep/addon/exclude
    note       = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=kst_now)
    reviewed_at= db.Column(db.DateTime)

    master = db.relationship("MasterProduct", backref="option_reviews")

    __table_args__ = (
        db.UniqueConstraint("master_product_id", "option_name", name="uq_option_review"),
    )
