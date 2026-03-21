import io
from flask import Blueprint, request, send_file, render_template
from flask_login import login_required

exporter_bp = Blueprint("exporter", __name__)


@exporter_bp.route("/exporter/smartstore")
@login_required
def smartstore_form():
    from app.wholesalers.models import Wholesaler
    wholesalers = Wholesaler.query.order_by(Wholesaler.name).all()
    return render_template("exporter_smartstore.html", wholesalers=wholesalers)


@exporter_bp.route("/exporter/smartstore/download")
@login_required
def smartstore_download():
    wholesaler_id = request.args.get("wholesaler_id", type=int)
    status = request.args.get("status", "active")

    from app.wholesalers.models import Wholesaler
    from app.exporter.smartstore import generate_smartstore_excel

    wholesaler = Wholesaler.query.get_or_404(wholesaler_id)
    excel_bytes = generate_smartstore_excel(wholesaler_id, status)
    filename = f"smartstore_{wholesaler.code}_{status}.xlsx"

    return send_file(
        io.BytesIO(excel_bytes),
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
