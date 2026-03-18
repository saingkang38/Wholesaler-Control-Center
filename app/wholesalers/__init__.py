from flask import Blueprint
from app.wholesalers.models import Wholesaler
from app.infrastructure import db

wholesalers_bp = Blueprint("wholesalers", __name__)

def get_or_create_ownerclan():
    w = Wholesaler.query.filter_by(code="ownerclan").first()
    if not w:
        w = Wholesaler(
            code="ownerclan",
            name="오너클랜",
            site_url="https://www.ownerclan.com"
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 오너클랜 도매처 등록 완료")
    return w