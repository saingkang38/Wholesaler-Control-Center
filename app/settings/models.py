from app.infrastructure import db
from app.utils import kst_now


class MarginRule(db.Model):
    __tablename__ = "margin_rules"

    id = db.Column(db.Integer, primary_key=True)
    price_from = db.Column(db.Integer, nullable=False)       # 이상 (원)
    price_to = db.Column(db.Integer, nullable=True)          # 이하 (원), None = 제한없음
    margin_rate = db.Column(db.Float, nullable=False)        # 마진율 (예: 0.3 = 30%)

    created_at = db.Column(db.DateTime, default=kst_now)
    updated_at = db.Column(db.DateTime, default=kst_now, onupdate=kst_now)
