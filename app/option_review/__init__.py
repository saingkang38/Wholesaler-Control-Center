import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import func

from app.infrastructure import db
from app.option_review.models import OptionReviewPolicy, AddonProduct
from app.master.models import MasterProduct

option_review_bp = Blueprint("option_review", __name__)
logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")

# ── 상수 ─────────────────────────────────────────────────────
ACCESSORY_KW = [
    "노즐","스티커","뚜껑","빨대","파우치","케이스","리필","추가","캡","부속",
    "구성품","교체용","부품","악세서리","액세서리","호스","필터","받침","받침대",
    "걸이","고리","클립","끈","스트랩","배터리","충전","어댑터","아답터","커버",
    "덮개","망사","그물","체인","잠금","자물쇠","너트","볼트","나사","패드",
    "가스켓","실링","오링","세제","청소","닦이","와이퍼","패킹","씰",
    "소모품","부자재","별도","선택","추가구매","따로","분리","단품",
]
DECISION_LABEL = {"pending":"미검토","keep":"옵션유지","addon":"추가상품","exclude":"제외"}


# ── 내부 헬퍼 ────────────────────────────────────────────────
def _find_kw(name):
    return [kw for kw in ACCESSORY_KW if kw in name] if name else []

def _calc_risk(min_pct, cheap_count, has_kw):
    s = 0
    if min_pct <= -60:   s += 5
    elif min_pct <= -50: s += 3
    elif min_pct <= -35: s += 2
    elif min_pct <= -20: s += 1
    if cheap_count >= 3: s += 2
    elif cheap_count >= 2: s += 1
    if has_kw: s += 3
    return s

def _grade(score):
    if score >= 6: return "최우선확인"
    if score >= 4: return "강한의심"
    if score >= 1: return "검토필요"
    return None

def _run_analysis():
    products = MasterProduct.query.filter(
        MasterProduct.options_text != None, MasterProduct.options_text != "",
        MasterProduct.option_diffs != None, MasterProduct.option_diffs != "",
    ).all()
    stats = {"new": 0, "updated": 0}
    for p in products:
        try:
            names = [x.strip() for x in p.options_text.split("\n") if x.strip()]
            diffs = [int(x.strip()) for x in p.option_diffs.split("\n") if x.strip()]
        except Exception:
            continue
        n = min(len(names), len(diffs))
        if n < 2: continue
        names, diffs = names[:n], diffs[:n]
        base = p.price or 0
        if base <= 0: continue
        main_price = base + diffs[0]
        if main_price <= 0: continue

        cheap = []
        for i in range(1, n):
            op = base + diffs[i]
            if op < main_price:
                pct = (op - main_price) / main_price * 100
                cheap.append({"name": names[i], "price": op, "pct": round(pct,1), "kws": _find_kw(names[i])})
        if not cheap: continue

        cnt = len(cheap); min_pct = min(x["pct"] for x in cheap)
        any_kw = any(x["kws"] for x in cheap)
        rs = _calc_risk(min_pct, cnt, any_kw); g = _grade(rs)
        if not g: continue

        for opt in cheap:
            kw_str = ", ".join(opt["kws"])
            ex = OptionReviewPolicy.query.filter_by(master_product_id=p.id, option_name=opt["name"]).first()
            if ex:
                ex.option_price = opt["price"]; ex.main_option_name = names[0]
                ex.main_option_price = main_price; ex.diff_pct = opt["pct"]
                ex.cheap_option_count = cnt; ex.accessory_keywords = kw_str
                ex.risk_score = rs; ex.risk_grade = g
                stats["updated"] += 1
            else:
                db.session.add(OptionReviewPolicy(
                    master_product_id=p.id, option_name=opt["name"],
                    option_price=opt["price"], main_option_name=names[0],
                    main_option_price=main_price, diff_pct=opt["pct"],
                    cheap_option_count=cnt, accessory_keywords=kw_str,
                    risk_score=rs, risk_grade=g,
                ))
                stats["new"] += 1
    db.session.commit()
    return stats


def _product_option_rows(master):
    """master 의 전체 옵션을 파싱해 row list 반환."""
    if not master.options_text:
        return []
    names = [x.strip() for x in master.options_text.split("\n") if x.strip()]
    diffs = []
    if master.option_diffs:
        try: diffs = [int(x.strip()) for x in master.option_diffs.split("\n") if x.strip()]
        except ValueError: pass
    base = master.price or 0
    n = min(len(names), len(diffs)) if diffs else len(names)
    main_price = (base + diffs[0]) if diffs else base

    # 현재 policy 로드
    policies = {r.option_name: r for r in OptionReviewPolicy.query.filter_by(master_product_id=master.id).all()}

    rows = []
    for i in range(len(names)):
        nm = names[i]
        diff = diffs[i] if i < len(diffs) else 0
        abs_price = base + diff
        pct = round((abs_price - main_price) / main_price * 100, 1) if main_price else 0
        pol = policies.get(nm)
        rows.append({
            "idx":       i,
            "name":      nm,
            "abs_price": abs_price,
            "diff":      diff,
            "pct":       pct,
            "is_main":   i == 0,
            "is_cheap":  pct < -20 and i > 0,
            "keywords":  _find_kw(nm),
            "decision":  pol.decision if pol else ("keep" if i == 0 else "keep"),
            "risk_grade": pol.risk_grade if pol else "",
            "risk_score": pol.risk_score if pol else 0,
            "policy_id": pol.id if pol else None,
        })
    return rows


def _find_store(master):
    """MasterProduct → StoreProduct 탐색 (seller_management_code 매칭)."""
    from app.store.models import StoreProduct
    return StoreProduct.query.filter_by(
        seller_management_code=master.supplier_product_code
    ).first()


# ═══════════════════════════════════════════════════════════════
# 1. 메인 목록 — 상품별
# ═══════════════════════════════════════════════════════════════
@option_review_bp.route("/option-review")
@login_required
def option_review_page():
    if OptionReviewPolicy.query.count() == 0:
        _run_analysis()

    status_filter = request.args.get("status", "pending")  # pending/done/all
    grade_filter  = request.args.get("grade", "")
    search        = request.args.get("q", "").strip()
    page          = request.args.get("page", 1, type=int)
    per_page      = 40

    # 상품별 집계 서브쿼리
    sub = (
        db.session.query(
            OptionReviewPolicy.master_product_id.label("mid"),
            func.count(OptionReviewPolicy.id).label("total"),
            func.sum(db.case((OptionReviewPolicy.decision == "pending", 1), else_=0)).label("pending_cnt"),
            func.max(OptionReviewPolicy.risk_score).label("max_score"),
            func.max(OptionReviewPolicy.risk_grade).label("top_grade"),
        )
        .group_by(OptionReviewPolicy.master_product_id)
        .subquery()
    )

    q = db.session.query(
        MasterProduct,
        sub.c.total,
        sub.c.pending_cnt,
        sub.c.max_score,
        sub.c.top_grade,
    ).join(sub, MasterProduct.id == sub.c.mid)

    if grade_filter:
        q = q.filter(sub.c.top_grade == grade_filter)
    if status_filter == "pending":
        q = q.filter(sub.c.pending_cnt > 0)
    elif status_filter == "done":
        q = q.filter(sub.c.pending_cnt == 0)
    if search:
        like = f"%{search}%"
        q = q.filter(db.or_(
            MasterProduct.product_name.ilike(like),
            MasterProduct.supplier_product_code.ilike(like),
        ))

    q = q.order_by(sub.c.pending_cnt.desc(), sub.c.max_score.desc())
    pagination = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for master, total, pending_cnt, max_score, top_grade in pagination.items:
        done = total - pending_cnt
        items.append({
            "master_id":    master.id,
            "product_code": master.supplier_product_code,
            "product_name": master.product_name or "-",
            "total":        total,
            "pending":      pending_cnt,
            "done":         done,
            "top_grade":    top_grade or "",
            "is_complete":  pending_cnt == 0,
        })

    # 탭 카운트
    total_products  = db.session.query(func.count(func.distinct(OptionReviewPolicy.master_product_id))).scalar()
    pending_products= db.session.query(func.count(func.distinct(OptionReviewPolicy.master_product_id))).filter(OptionReviewPolicy.decision == "pending").scalar()
    done_products   = total_products - pending_products

    return render_template(
        "option_review.html",
        items=items,
        pagination=pagination,
        status_filter=status_filter,
        grade_filter=grade_filter,
        search=search,
        total_products=total_products,
        pending_products=pending_products,
        done_products=done_products,
    )


# ═══════════════════════════════════════════════════════════════
# 2. 상품별 옵션 분류 화면
# ═══════════════════════════════════════════════════════════════
@option_review_bp.route("/option-review/product/<int:master_id>")
@login_required
def product_detail(master_id):
    master = MasterProduct.query.get_or_404(master_id)
    rows   = _product_option_rows(master)
    store  = _find_store(master)

    addon_records = {r.option_name: r for r in AddonProduct.query.filter_by(master_product_id=master_id).all()}
    for row in rows:
        ar = addon_records.get(row["name"])
        row["naver_id"] = ar.naver_supplement_id if ar else None

    # 다음/이전 상품 (미검토 상품 순서)
    sub = (
        db.session.query(OptionReviewPolicy.master_product_id)
        .filter(OptionReviewPolicy.decision == "pending")
        .distinct()
        .subquery()
    )
    pending_ids = [r[0] for r in db.session.query(sub).all()]
    try:
        idx = pending_ids.index(master_id)
        prev_id = pending_ids[idx - 1] if idx > 0 else None
        next_id = pending_ids[idx + 1] if idx < len(pending_ids) - 1 else None
    except ValueError:
        prev_id = next_id = None

    return render_template(
        "option_review_product.html",
        master=master,
        rows=rows,
        store=store,
        prev_id=prev_id,
        next_id=next_id,
        total_pending=len(pending_ids),
        DECISION_LABEL=DECISION_LABEL,
    )


@option_review_bp.route("/option-review/product/<int:master_id>/save", methods=["POST"])
@login_required
def save_decisions(master_id):
    """각 옵션의 결정을 저장."""
    master = MasterProduct.query.get_or_404(master_id)
    data   = request.get_json()
    if not data:
        return jsonify({"ok": False, "error": "no data"}), 400

    decisions = data.get("decisions", {})  # {option_name: decision}
    now = datetime.now(KST)

    opt_names = [x.strip() for x in (master.options_text or "").split("\n") if x.strip()]
    opt_diffs_raw = [x.strip() for x in (master.option_diffs or "").split("\n") if x.strip()]
    try: opt_diffs = [int(x) for x in opt_diffs_raw]
    except ValueError: opt_diffs = []
    base = master.price or 0
    main_price = (base + opt_diffs[0]) if opt_diffs else base

    for opt_name, decision in decisions.items():
        if decision not in ("keep", "addon", "exclude"):
            continue

        # 해당 옵션의 절대가 계산
        idx = next((i for i, n in enumerate(opt_names) if n == opt_name), None)
        diff = opt_diffs[idx] if idx is not None and idx < len(opt_diffs) else 0
        abs_price = base + diff
        pct = round((abs_price - main_price) / main_price * 100, 1) if main_price else 0

        pol = OptionReviewPolicy.query.filter_by(
            master_product_id=master_id, option_name=opt_name
        ).first()

        if decision == "keep":
            # keep 는 기본값이므로 기존 record 삭제 (있으면)
            if pol:
                db.session.delete(pol)
            # AddonProduct 정리
            AddonProduct.query.filter_by(master_product_id=master_id, option_name=opt_name).delete()
        else:
            kws = _find_kw(opt_name)
            if pol:
                pol.decision = decision
                pol.reviewed_at = now
                pol.option_price = abs_price
                pol.diff_pct = pct
                pol.accessory_keywords = ", ".join(kws)
            else:
                # OptionReviewPolicy 에 없던 옵션 (정상 옵션을 사용자가 addon/exclude 결정)
                cheap_count = sum(1 for i, n in enumerate(opt_names)
                                  if i > 0 and i < len(opt_diffs) and (base + opt_diffs[i]) < main_price)
                rs = _calc_risk(pct, cheap_count, bool(kws))
                g  = _grade(rs) or "검토필요"
                db.session.add(OptionReviewPolicy(
                    master_product_id=master_id, option_name=opt_name,
                    option_price=abs_price, main_option_name=opt_names[0] if opt_names else "",
                    main_option_price=main_price, diff_pct=pct,
                    cheap_option_count=cheap_count,
                    accessory_keywords=", ".join(kws),
                    risk_score=rs, risk_grade=g, decision=decision, reviewed_at=now,
                ))

            # AddonProduct 동기화
            if decision == "addon":
                ar = AddonProduct.query.filter_by(master_product_id=master_id, option_name=opt_name).first()
                if ar:
                    ar.wholesaler_price = abs_price
                else:
                    db.session.add(AddonProduct(
                        master_product_id=master_id, option_name=opt_name, wholesaler_price=abs_price
                    ))
            else:
                AddonProduct.query.filter_by(master_product_id=master_id, option_name=opt_name).delete()

    db.session.commit()
    return jsonify({"ok": True})


@option_review_bp.route("/option-review/product/<int:master_id>/apply", methods=["POST"])
@login_required
def apply_to_store(master_id):
    """현재 정책을 네이버 스토어에 즉시 적용."""
    master = MasterProduct.query.get_or_404(master_id)
    store  = _find_store(master)
    if not store:
        return jsonify({"ok": False, "error": "연결된 스토어 상품 없음"}), 404
    if not store.naver_store:
        return jsonify({"ok": False, "error": "네이버 스토어 설정 없음"}), 404
    if not store.origin_product_no:
        return jsonify({"ok": False, "error": "원상품번호 없음"}), 404

    cid    = store.naver_store.client_id
    csec   = store.naver_store.client_secret
    policies = get_option_policies(master_id)

    try:
        from store.naver.products import get_origin_product, update_origin_product
        from app.settings import calculate_option_pricing

        product_data = get_origin_product(store.origin_product_no, cid, csec)
        origin  = product_data.get("originProduct", {})
        detail  = origin.setdefault("detailAttribute", {})
        opt_info = detail.get("optionInfo", {})
        combos  = opt_info.get("optionCombinations", [])

        base_price = master.price or 0
        opt_names = [x.strip() for x in (master.options_text or "").split("\n") if x.strip()]
        opt_diffs_raw = []
        if master.option_diffs:
            try:
                opt_diffs_raw = [int(x.strip()) for x in master.option_diffs.split("\n") if x.strip()]
            except ValueError:
                pass

        # keep 옵션만 추출해서 pricing 재계산 (addon 제거 후 0원짜리 보장)
        keep_names, keep_diffs = [], []
        for i, nm in enumerate(opt_names):
            if policies.get(nm) not in ("addon", "exclude"):
                keep_names.append(nm)
                keep_diffs.append(opt_diffs_raw[i] if i < len(opt_diffs_raw) else 0)

        pricing = {}
        if keep_diffs:
            keep_diffs_text = "\n".join(str(d) for d in keep_diffs)
            pricing = calculate_option_pricing(base_price, keep_diffs_text)
        additions = pricing.get("additions", [])

        # keep 옵션만 combo 유지, 가격 업데이트
        new_combos = []
        for combo in combos:
            nm = combo.get("optionName1") or combo.get("optionName2") or ""
            if policies.get(nm) in ("addon", "exclude"):
                continue
            idx = next((j for j, n in enumerate(keep_names) if n == nm), None)
            if idx is not None and idx < len(additions):
                combo["price"] = additions[idx]
            new_combos.append(combo)

        if not new_combos:
            new_combos = combos  # 안전장치

        opt_info["optionCombinations"] = new_combos
        detail["optionInfo"] = opt_info

        # addon → supplementProductInfo
        supps = build_supplement_payload(master, base_price, policies, product_data)
        if supps:
            detail["supplementProductInfo"] = {
                "groupName": "추가상품",
                "supplementProducts": supps,
            }

        if pricing.get("list_price"):
            origin["salePrice"] = pricing["list_price"]
            if pricing.get("discount", 0) > 0:
                origin["customerBenefit"] = {
                    "immediateDiscountPolicy": {
                        "discountMethod": {"value": pricing["discount"], "unitType": "WON"}
                    }
                }
            else:
                origin["customerBenefit"] = {}

        payload = {
            "originProduct": origin,
            "smartstoreChannelProduct": product_data.get("smartstoreChannelProduct", {}),
        }
        resp = update_origin_product(store.origin_product_no, payload, cid, csec)

        # supplement ID 동기화
        if supps:
            has_new = any("id" not in s for s in supps)
            if has_new or not isinstance(resp, dict) or not resp:
                fresh = get_origin_product(store.origin_product_no, cid, csec)
                sync_addon_supplement_ids(master_id, fresh)
            else:
                sync_addon_supplement_ids(master_id, resp)

        logger.info(f"[option-review] apply_to_store: master_id={master_id}, combo={len(new_combos)}, addon={len(supps)}")
        return jsonify({"ok": True, "combo_count": len(new_combos), "addon_count": len(supps)})

    except Exception as e:
        logger.error(f"[option-review] apply_to_store 실패: {e}")
        return jsonify({"ok": False, "error": str(e)[:300]}), 500


# ═══════════════════════════════════════════════════════════════
# 3. 확정 상품 관리
# ═══════════════════════════════════════════════════════════════
@option_review_bp.route("/option-review/managed")
@login_required
def managed_page():
    page = request.args.get("page", 1, type=int)
    search = request.args.get("q", "").strip()

    # 확정(non-pending) 결정이 하나라도 있는 상품
    sub = (
        db.session.query(
            OptionReviewPolicy.master_product_id.label("mid"),
            func.sum(db.case((OptionReviewPolicy.decision == "keep",    1), else_=0)).label("keep_cnt"),
            func.sum(db.case((OptionReviewPolicy.decision == "addon",   1), else_=0)).label("addon_cnt"),
            func.sum(db.case((OptionReviewPolicy.decision == "exclude", 1), else_=0)).label("excl_cnt"),
            func.sum(db.case((OptionReviewPolicy.decision == "pending", 1), else_=0)).label("pend_cnt"),
        )
        .group_by(OptionReviewPolicy.master_product_id)
        .having(db.or_(
            func.sum(db.case((OptionReviewPolicy.decision == "addon",   1), else_=0)) > 0,
            func.sum(db.case((OptionReviewPolicy.decision == "exclude", 1), else_=0)) > 0,
        ))
        .subquery()
    )

    q = db.session.query(
        MasterProduct,
        sub.c.keep_cnt,
        sub.c.addon_cnt,
        sub.c.excl_cnt,
        sub.c.pend_cnt,
    ).join(sub, MasterProduct.id == sub.c.mid)
    if search:
        like = f"%{search}%"
        q = q.filter(db.or_(
            MasterProduct.product_name.ilike(like),
            MasterProduct.supplier_product_code.ilike(like),
        ))
    q = q.order_by(sub.c.addon_cnt.desc())
    pagination = q.paginate(page=page, per_page=40, error_out=False)

    items = []
    for master, keep_cnt, addon_cnt, excl_cnt, pend_cnt in pagination.items:
        store = _find_store(master)
        addon_recs = AddonProduct.query.filter_by(master_product_id=master.id).all()
        synced = sum(1 for a in addon_recs if a.naver_supplement_id)

        opt_count = len([x for x in (master.options_text or "").split("\n") if x.strip()])
        items.append({
            "master_id":    master.id,
            "product_code": master.supplier_product_code,
            "product_name": master.product_name or "-",
            "total_opts":   opt_count,
            "keep_cnt":     keep_cnt,
            "addon_cnt":    addon_cnt,
            "excl_cnt":     excl_cnt,
            "pend_cnt":     pend_cnt,
            "store_status": store.store_status if store else "-",
            "sale_price":   store.sale_price if store else 0,
            "has_store":    store is not None,
            "addon_synced": synced,
            "addon_total":  addon_cnt,
            "naver_link":   f"https://smartstore.naver.com/products/{store.channel_product_no}" if store and store.channel_product_no else "",
        })

    return render_template(
        "option_review_managed.html",
        items=items,
        pagination=pagination,
        search=search,
    )


# ═══════════════════════════════════════════════════════════════
# 기타 API
# ═══════════════════════════════════════════════════════════════
@option_review_bp.route("/option-review/refresh", methods=["POST"])
@login_required
def refresh_analysis():
    try:
        stats = _run_analysis()
        return jsonify({"ok": True, **stats})
    except Exception as e:
        logger.error(f"[option-review] 분석 갱신 실패: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── 공개 헬퍼 (actions 모듈에서 import) ─────────────────────
def get_option_policies(master_product_id: int) -> dict:
    rows = OptionReviewPolicy.query.filter(
        OptionReviewPolicy.master_product_id == master_product_id,
        OptionReviewPolicy.decision.in_(["keep", "addon", "exclude"]),
    ).all()
    return {r.option_name: r.decision for r in rows}


def get_addon_products(master_product_id: int) -> dict:
    rows = AddonProduct.query.filter_by(master_product_id=master_product_id).all()
    return {r.option_name: r for r in rows}


def sync_addon_supplement_ids(master_product_id: int, product_data: dict):
    supp_list = (
        product_data.get("originProduct", {})
        .get("detailAttribute", {})
        .get("supplementProductInfo", {})
        .get("supplementProducts", [])
    )
    changed = False
    for supp in supp_list:
        sid = supp.get("id"); name = supp.get("name", "")
        if not sid or not name: continue
        addon = AddonProduct.query.filter_by(master_product_id=master_product_id, option_name=name).first()
        if addon and addon.naver_supplement_id != sid:
            addon.naver_supplement_id = sid
            addon.last_synced_at = datetime.now(KST)
            changed = True
    if changed:
        db.session.commit()


def build_supplement_payload(master, base_price: int, policies: dict, existing_product_data: dict) -> list:
    from app.settings import apply_margin
    opt_names = [x.strip() for x in (master.options_text or "").split("\n") if x.strip()]
    opt_diffs = []
    if master.option_diffs:
        try: opt_diffs = [int(x.strip()) for x in master.option_diffs.split("\n") if x.strip()]
        except ValueError: pass

    existing_supps = (
        existing_product_data.get("originProduct", {})
        .get("detailAttribute", {})
        .get("supplementProductInfo", {})
        .get("supplementProducts", [])
    )
    existing_id_by_name = {s.get("name", ""): s.get("id") for s in existing_supps}
    addon_records = get_addon_products(master.id)

    supplements = []
    for i, name in enumerate(opt_names):
        if policies.get(name) != "addon": continue
        diff = opt_diffs[i] if i < len(opt_diffs) else 0
        selling = apply_margin(base_price + diff)
        entry = {"groupName": "추가상품", "name": name, "price": max(selling, 1), "stockQuantity": 99, "usable": True}
        nid = existing_id_by_name.get(name) or (addon_records[name].naver_supplement_id if name in addon_records else None)
        if nid: entry["id"] = nid
        supplements.append(entry)
    return supplements
