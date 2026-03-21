from app.infrastructure import db
from datetime import datetime

class Wholesaler(db.Model):
    __tablename__ = "wholesalers"

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(64), unique=True, nullable=False)
    name = db.Column(db.String(128), nullable=False)
    site_url = db.Column(db.String(256))
    is_active = db.Column(db.Boolean, default=True)
    login_required = db.Column(db.Boolean, default=True)
    prefix = db.Column(db.String(20), nullable=True)
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)