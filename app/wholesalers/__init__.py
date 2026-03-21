from flask import Blueprint
from app.wholesalers.models import Wholesaler
from app.infrastructure import db

wholesalers_bp = Blueprint("wholesalers", __name__)


def get_or_create_dometopia():
    w = Wholesaler.query.filter_by(code="dometopia").first()
    if not w:
        w = Wholesaler(
            code="dometopia",
            name="도매토피아",
            site_url="https://www.dometopia.com",
            prefix="doto_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 도매토피아 도매처 등록 완료")
    return w


def get_or_create_ownerclan():
    w = Wholesaler.query.filter_by(code="ownerclan").first()
    if not w:
        w = Wholesaler(
            code="ownerclan",
            name="오너클랜",
            site_url="https://www.ownerclan.com",
            prefix="on_",
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
            site_url="https://www.1001094.com",
            prefix="jtc_",
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
            prefix="hit_",
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
            prefix="ds_",
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
            notes="기업회원 로그인 필요 (할인가 수집)",
            prefix="cm_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 철물박사 도매처 등록 완료")
    return w


def get_or_create_mro3():
    w = Wholesaler.query.filter_by(code="mro3").first()
    if not w:
        w = Wholesaler(
            code="mro3",
            name="3MRO",
            site_url="https://www.3mro.co.kr",
            prefix="mro_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 3MRO 도매처 등록 완료")
    return w


def get_or_create_zentrade():
    w = Wholesaler.query.filter_by(code="zentrade").first()
    if not w:
        w = Wholesaler(
            code="zentrade",
            name="젠트레이드",
            site_url="https://zentrade.co.kr",
            prefix="zt_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 젠트레이드 도매처 등록 완료")
    return w


def get_or_create_chingudome():
    w = Wholesaler.query.filter_by(code="chingudome").first()
    if not w:
        w = Wholesaler(
            code="chingudome",
            name="친구도매",
            site_url="https://www.chingudome.com",
            prefix="79_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 친구도매 도매처 등록 완료")
    return w


def get_or_create_sikjaje():
    w = Wholesaler.query.filter_by(code="sikjaje").first()
    if not w:
        w = Wholesaler(
            code="sikjaje",
            name="식자재코리아",
            site_url="https://www.sikjajekr.com",
            login_required=True,
            prefix="sik_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 식자재코리아 도매처 등록 완료")
    return w


def get_or_create_feelwoo():
    w = Wholesaler.query.filter_by(code="feelwoo").first()
    if not w:
        w = Wholesaler(
            code="feelwoo",
            name="필우커머스",
            site_url="https://feelwoo.com",
            login_required=True,
            prefix="fwc_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 필우커머스 도매처 등록 완료")
    return w


def get_or_create_onch3():
    w = Wholesaler.query.filter_by(code="onch3").first()
    if not w:
        w = Wholesaler(
            code="onch3",
            name="온채널",
            site_url="https://www.onch3.co.kr",
            login_required=True,
            prefix="onch_",
        )
        db.session.add(w)
        db.session.commit()
        print("[초기화] 온채널 도매처 등록 완료")
    return w
