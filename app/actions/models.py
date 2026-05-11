from app.infrastructure import db
from app.utils import kst_now


class ActionSignal(db.Model):
    __tablename__ = "action_signals"

    id = db.Column(db.Integer, primary_key=True)

    # 연결 정보
    master_product_id = db.Column(db.Integer, db.ForeignKey("master_products.id"), nullable=False)
    store_product_id = db.Column(db.Integer, db.ForeignKey("store_products.id"), nullable=True)

    # 시그널 유형
    # PRICE_UP_NEEDED / PRICE_DOWN_POSSIBLE
    # SUSPEND_NEEDED / RESUME_POSSIBLE / DISCONTINUE_NEEDED
    signal_type = db.Column(db.String(32), nullable=False)

    # 참고 값
    current_value = db.Column(db.Text)   # JSON (현재 스토어 값)
    suggested_value = db.Column(db.Text) # JSON (제안값)

    # 처리 상태: pending / executed / reverted / rejected / skipped / failed / awaiting_input
    # awaiting_input — Naver가 필수값 누락으로 PUT 거부, 운영자가 직접 채워야 진행 가능
    status = db.Column(db.String(16), default="pending", nullable=False)
    error_message = db.Column(db.Text, nullable=True)  # 실패 시 Naver API 오류 메시지

    # awaiting_input 상태에서 채워질 필드 메타데이터(JSON 배열)
    # [{"name": "originProduct...consumptionDate", "message": "...", "kind": "text", "label": "...", "hint": "..."}]
    required_fields_missing = db.Column(db.Text, nullable=True)

    detected_at = db.Column(db.DateTime, default=kst_now)
    resolved_at = db.Column(db.DateTime, nullable=True)

    master = db.relationship("MasterProduct", backref="action_signals")
    store = db.relationship("StoreProduct", backref="action_signals")
