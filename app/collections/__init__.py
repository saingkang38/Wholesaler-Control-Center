from flask import Blueprint, jsonify
from flask_login import login_required, current_user
from app.collectors.orchestrator import run_collection

collections_bp = Blueprint("collections", __name__)

@collections_bp.route("/api/collect/<wholesaler_code>", methods=["POST"])
@login_required
def trigger_collection(wholesaler_code):
    result = run_collection(
        wholesaler_code=wholesaler_code,
        trigger_type="manual",
        user_id=current_user.id
    )
    return jsonify(result)