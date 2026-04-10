import logging
logger = logging.getLogger(__name__)
from flask import Blueprint, render_template
from flask_login import login_required
from app.wholesalers.models import Wholesaler
from app.infrastructure import db

wholesalers_bp = Blueprint("wholesalers", __name__)


@wholesalers_bp.route("/wholesalers")
@login_required
def wholesalers_page():
    wholesalers = Wholesaler.query.order_by(Wholesaler.name).all()
    return render_template("wholesalers.html", wholesalers=wholesalers)


def _ensure_wholesaler(code, name, site_url, prefix, login_required=False, notes=None):
    """도매처가 없으면 생성, 있으면 그대로 반환."""
    w = Wholesaler.query.filter_by(code=code).first()
    if not w:
        w = Wholesaler(
            code=code,
            name=name,
            site_url=site_url,
            prefix=prefix,
            login_required=login_required,
            notes=notes,
        )
        db.session.add(w)
        db.session.commit()
        logger.info(f"[초기화] {name} 도매처 등록 완료")
    return w


def get_or_create_ownerclan():
    return _ensure_wholesaler("ownerclan", "오너클랜", "https://www.ownerclan.com", "on_")

def get_or_create_jtckorea():
    return _ensure_wholesaler("jtckorea", "JTC코리아", "https://www.1001094.com", "jtc_")

def get_or_create_metaldiy():
    return _ensure_wholesaler("metaldiy", "철물박사", "https://www.metaldiy.com", "cm_",
                              login_required=True, notes="기업회원 로그인 필요 (할인가 수집)")

def get_or_create_ds1008():
    return _ensure_wholesaler("ds1008", "DS도매", "https://www.ds1008.com", "ds_",
                              login_required=True)

def get_or_create_hitdesign():
    return _ensure_wholesaler("hitdesign", "히트가구", "https://b2b-hitdesign.com", "hit_",
                              login_required=True)

def get_or_create_mro3():
    return _ensure_wholesaler("mro3", "3MRO", "https://www.3mro.co.kr", "mro_")

def get_or_create_zentrade():
    return _ensure_wholesaler("zentrade", "젠트레이드", "https://zentrade.co.kr", "zt_")

def get_or_create_chingudome():
    return _ensure_wholesaler("chingudome", "친구도매", "https://www.chingudome.com", "79_")

def get_or_create_sikjaje():
    return _ensure_wholesaler("sikjaje", "식자재코리아", "https://www.sikjajekr.com", "sik_",
                              login_required=True)

def get_or_create_feelwoo():
    return _ensure_wholesaler("feelwoo", "필우커머스", "https://feelwoo.com", "fwc_",
                              login_required=True)

def get_or_create_onch3():
    return _ensure_wholesaler("onch3", "온채널", "https://www.onch3.co.kr", "onch_",
                              login_required=True)

def get_or_create_dometopia():
    return _ensure_wholesaler("dometopia", "도매토피아", "https://www.dometopia.com", "doto_")
