from app.infrastructure import db
from app.utils import kst_now

class CollectionRun(db.Model):
    __tablename__ = "collection_runs"

    id = db.Column(db.Integer, primary_key=True)
    wholesaler_id = db.Column(db.Integer, db.ForeignKey("wholesalers.id"), nullable=False)
    run_type = db.Column(db.String(32), default="full")        # full / partial / manual_test
    trigger_type = db.Column(db.String(32), default="manual")  # manual / scheduled / system
    status = db.Column(db.String(32), default="pending")       # pending / running / success / failed / partial_success
    started_at = db.Column(db.DateTime)
    finished_at = db.Column(db.DateTime)
    total_items = db.Column(db.Integer, default=0)
    total_pages = db.Column(db.Integer, default=0)
    success_count = db.Column(db.Integer, default=0)
    fail_count = db.Column(db.Integer, default=0)
    error_summary = db.Column(db.Text)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("users.id"))
    created_at = db.Column(db.DateTime, default=kst_now)

    wholesaler = db.relationship("Wholesaler", backref="collection_runs")
    created_by = db.relationship("User", backref="collection_runs")