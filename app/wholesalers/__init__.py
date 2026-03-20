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


def get_or_create_jtckorea():
    w = Wholesaler.query.filter_by(code="jtckorea").first()
    if not w:
        w = Wholesaler(
            code="jtckorea",
            name="JTC코리아",
            site_url="https://www.1001094.com"
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] JTC코리아 도매처 등록 완료")
    return w


def get_or_create_hitdesign():
    w = Wholesaler.query.filter_by(code="hitdesign").first()
    if not w:
        w = Wholesaler(
            code="hitdesign",
            name="히트가구",
            site_url="https://b2b-hitdesign.com",
            login_required=True,
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 히트가구 도매처 등록 완료")
    return w


def get_or_create_ds1008():
    w = Wholesaler.query.filter_by(code="ds1008").first()
    if not w:
        w = Wholesaler(
            code="ds1008",
            name="DS도매",
            site_url="https://www.ds1008.com",
            login_required=True,
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] DS도매 도매처 등록 완료")
    return w


def get_or_create_metaldiy():
    w = Wholesaler.query.filter_by(code="metaldiy").first()
    if not w:
        w = Wholesaler(
            code="metaldiy",
            name="철물박사",
            site_url="https://www.metaldiy.com",
            login_required=True,
            notes="기업회원 로그인 필요 (할인가 수집)"
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 철물박사 도매처 등록 완료")
    return w