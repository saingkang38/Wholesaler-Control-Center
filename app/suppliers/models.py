from app.infrastructure import db
from datetime import datetime

class SupplierCode(db.Model):
    __tablename__ = "supplier_codes"

    id = db.Column(db.Integer, primary_key=True)
    wholesaler_id = db.Column(db.Integer, db.ForeignKey("wholesalers.id"), nullable=False)
    supplier_code = db.Column(db.String(128), nullable=False)
    supplier_name = db.Column(db.String(256))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    wholesaler = db.relationship("Wholesaler", backref="supplier_codes")