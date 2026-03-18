from flask import Blueprint, render_template
from flask_login import login_required
from app.execution_logs.models import CollectionRun

dashboard_bp = Blueprint("dashboard", __name__)

@dashboard_bp.route("/")
@login_required
def index():
    recent_runs = CollectionRun.query.order_by(CollectionRun.created_at.desc()).limit(10).all()
    return render_template("dashboard.html", recent_runs=recent_runs)