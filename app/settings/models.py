from app.infrastructure import db
from datetime import datetime


class PrepSetting(db.Model):
    __tablename__ = "prep_settings"

    id = db.Column(db.Integer, primary_key=True)
    excel_dir           = db.Column(db.String(512))  # 엑셀 저장 경로
    image_dir           = db.Column(db.String(512))  # 원본 이미지 저장 경로
    processed_image_dir = db.Column(db.String(512))  # 가공 이미지 저장 경로
    side_panel_url      = db.Column(db.String(1024)) # 우측 패널에 열 URL

    # 이미지 가공 설정
    img_inner_scale  = db.Column(db.Integer, default=100)  # 내부 크기 % (100 = 원본, 80 = 여백)
    img_rotation     = db.Column(db.Integer, default=0)    # 회전 각도 (0, 90, -90, 180)
    img_output_size  = db.Column(db.Integer)               # 출력 파일 크기 px (정사각형)
    img_quality      = db.Column(db.Integer, default=100)  # 용량 압축 % (100 = 무압축)

    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @classmethod
    def get(cls):
        s = cls.query.first()
        if not s:
            from pathlib import Path
            base = Path.home() / "Desktop" / "상품가공"
            s = cls(
                excel_dir=str(base),
                image_dir=str(base / "이미지"),
                processed_image_dir=str(base / "가공이미지"),
                side_panel_url="https://namingfactory.ai.kr",
                img_inner_scale=100,
                img_rotation=0,
                img_output_size=None,
                img_quality=100,
            )
            db.session.add(s)
            db.session.commit()
        return s


class SmartStoreSetting(db.Model):
    __tablename__ = "smartstore_settings"

    id = db.Column(db.Integer, primary_key=True)

    # 배송 설정
    delivery_method        = db.Column(db.String(64),  default="택배,소포,등기")
    delivery_fee_type      = db.Column(db.String(32),  default="조건부무료")
    delivery_fee           = db.Column(db.Integer,     default=3000)
    free_condition_amount  = db.Column(db.Integer,     default=30000)
    delivery_fee_pay_type  = db.Column(db.String(32),  default="착불또는선결제")
    return_fee             = db.Column(db.Integer,     default=2500)
    exchange_fee           = db.Column(db.Integer,     default=2500)
    dispatch_days          = db.Column(db.Integer,     default=1)

    # 반품지
    return_location_name   = db.Column(db.String(128))
    return_zip             = db.Column(db.String(16))
    return_address         = db.Column(db.String(256))
    return_address_detail  = db.Column(db.String(256))

    # 출고지
    departure_location_name  = db.Column(db.String(128))
    departure_zip            = db.Column(db.String(16))
    departure_address        = db.Column(db.String(256))
    departure_address_detail = db.Column(db.String(256))

    # 배송비 템플릿 (네이버 템플릿 선택 시 사용)
    delivery_template_code = db.Column(db.String(32))   # 템플릿 코드 (e.g. 3355516)
    delivery_template_name = db.Column(db.String(128))  # 템플릿명 (표시용)

    # A/S
    as_phone = db.Column(db.String(32))
    as_guide = db.Column(db.String(512))

    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @classmethod
    def get(cls):
        s = cls.query.first()
        if not s:
            s = cls()
            db.session.add(s)
            db.session.commit()
        return s


class MarginRule(db.Model):
    __tablename__ = "margin_rules"

    id = db.Column(db.Integer, primary_key=True)
    price_from = db.Column(db.Integer, nullable=False)       # 이상 (원)
    price_to = db.Column(db.Integer, nullable=True)          # 이하 (원), None = 제한없음
    margin_rate = db.Column(db.Float, nullable=False)        # 마진율 (예: 0.3 = 30%)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
