import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from flask_login import login_required

from app.infrastructure import db
from app.option_review.models import OptionReviewPolicy, AddonProduct
from app.master.models import MasterProduct

option_review_bp = Blueprint("option_review", __name__)
logger = logging.getLogger(__name__)


@option_review_bp.app_template_global("build_qs")
def build_qs(**overrides):
    """현재 URL 쿼리파라미터에 overrides 를 적용해 새 쿼리스트링 반환."""
    from flask import request as _req
    from urllib.parse import urlencode
    params = dict(_req.args)
    params.update({k: v for k, v in overrides.items() if v is not None})
    params = {k: v for k, v in params.items() if v != ""}
    return urlencode(params)

KST = ZoneInfo("Asia/Seoul")

# ── 분석 상수 ────────────────────────────────────────────────
ACCESSORY_KW = [
    "노즐", "스티커", "뚜껑", "빨대", "파우치", "케이스", "리필", "추가", "캡", "부속",
    "구성품", "교체용", "부품", "악세서리", "액세서리", "호스", "필터", "받침", "받침대",
    "걸이", "고리", "클립", "끈", "스트랩", "배터리", "충전", "어댑터", "아답터", "커버",
    "덮개", "망사", "그물", "체인", "잠금", "자물쇠", "너트", "볼트", "나사", "패드",
    "가스켓", "실링", "오링", "세제", "청소", "닦이", "와이퍼", "패킹", "씰",
    "소모품", "부자재", "별도", "선택", "추가구매", "따로", "분리", "단품",
]

GRADE_SCORE = {
    "최우선확인": (6, 999),
    "강한의심":   (4, 5),
    "검토필요":   (1, 3),
}

DECISION_LABEL = {
    "pending": "미검토",
    "keep":    "유지",
    "addon":   "추가상품",
    "exclude": "제외",
}


# ── 내부 헬퍼 ────────────────────────────────────────────────
def _find_kw(name: str) -> list[str]:
    return [kw for kw in ACCESSORY_KW if kw in name] if name else []


def _calc_risk(min_pct: float, cheap_count: int, has_kw: bool) -> int:
    score = 0
    if min_pct <= -60:   score += 5
    elif min_pct <= -50: score += 3
    elif min_pct <= -35: score += 2
    elif min_pct <= -20: score += 1
    if cheap_count >= 3: score += 2
    elif cheap_count >= 2: score += 1
    if has_kw:           score += 3
    return score


def _grade(score: int) -> str | None:
    if score >= 6: return "최우선확인"
    if score >= 4: return "강한의심"
    if score >= 1: return "검토필요"
    return None


def _run_analysis() -> dict:
    """DB의 option_with_extra 상품을 분석하고 OptionReviewPolicy 에 upsert.
    반환: {'new': int, 'updated': int, 'skipped': int}"""
    products = MasterProduct.query.filter(
        MasterProduct.options_text != None,
        MasterProduct.options_text != "",
        MasterProduct.option_diffs != None,
        MasterProduct.option_diffs != "",
    ).all()

    stats = {"new": 0, "updated": 0, "skipped": 0}

    for p in products:
        try:
            opt_names = [x.strip() for x in p.options_text.split("\n") if x.strip()]
            diffs     = [int(x.strip()) for x in p.option_diffs.split("\n") if x.strip()]
        except Exception:
            continue

        n = min(len(opt_names), len(diffs))
        if n < 2:
            continue
        opt_names = opt_names[:n]
        diffs = diffs[:n]

        base = p.price or 0
        if base <= 0:
            continue
        main_price = base + diffs[0]
        if main_price <= 0:
            continue

        cheap_opts = []
        for i in range(1, n):
            opt_price = base + diffs[i]
            if opt_price < main_price:
                pct  = (opt_price - main_price) / main_price * 100
                kws  = _find_kw(opt_names[i])
                cheap_opts.append({
                    "name":  opt_names[i],
                    "price": opt_price,
                    "pct":   round(pct, 1),
                    "kws":   kws,
                })

        if not cheap_opts:
            continue

        cheap_count = len(cheap_opts)
        min_pct     = min(x["pct"] for x in cheap_opts)
        any_kw      = any(x["kws"] for x in cheap_opts)
        rs          = _calc_risk(min_pct, cheap_count, any_kw)
        g           = _grade(rs)

        if g is None:
            continue

        for opt in cheap_opts:
            existing = OptionReviewPolicy.query.filter_by(
                master_product_id=p.id,
                option_name=opt["name"],
            ).first()

            kw_str = ", ".join(opt["kws"]) if opt["kws"] else ""

            if existing:
                # 가격/점수 갱신 (결정은 유지)
                existing.option_price       = opt["price"]
                existing.main_option_name   = opt_names[0]
                existing.main_option_price  = main_price
                existing.diff_pct           = opt["pct"]
                existing.cheap_option_count = cheap_count
                existing.accessory_keywords = kw_str
                existing.risk_score         = rs
                existing.risk_grade         = g
                stats["updated"] += 1
            else:
                db.session.add(OptionReviewPolicy(
                    master_product_id = p.id,
                    option_name       = opt["name"],
                    option_price      = opt["price"],
                    main_option_name  = opt_names[0],
                    main_option_price = main_price,
                    diff_pct          = opt["pct"],
                    cheap_option_count= cheap_count,
                    accessory_keywords= kw_str,
                    risk_score        = rs,
                    risk_grade        = g,
                ))
                stats["new"] += 1

    db.session.commit()
    return stats


# ── 라우트 ────────────────────────────────────────────────────
@option_review_bp.route("/option-review")
@login_required
def option_review_page():
    # 첫 방문 시 데이터 없으면 자동 분석
    if OptionReviewPolicy.query.count() == 0:
        _run_analysis()

    grade_filter    = request.args.get("grade", "")
    decision_filter = request.args.get("decision", "pending")
    page            = request.args.get("page", 1, type=int)
    per_page        = request.args.get("per_page", 50, type=int)
    search          = request.args.get("q", "").strip()

    q = OptionReviewPolicy.query.join(MasterProduct)

    if grade_filter:
        q = q.filter(OptionReviewPolicy.risk_grade == grade_filter)
    if decision_filter:
        q = q.filter(OptionReviewPolicy.decision == decision_filter)
    if search:
        like = f"%{search}%"
        q = q.filter(
            db.or_(
                MasterProduct.product_name.ilike(like),
                MasterProduct.supplier_product_code.ilike(like),
                OptionReviewPolicy.option_name.ilike(like),
            )
        )

    q = q.order_by(OptionReviewPolicy.risk_score.desc(), OptionReviewPolicy.diff_pct.asc())
    pagination = q.paginate(page=page, per_page=per_page, error_out=False)

    # 탭 카운트
    grade_counts = {}
    for g in ["최우선확인", "강한의심", "검토필요"]:
        grade_counts[g] = OptionReviewPolicy.query.filter_by(
            risk_grade=g, decision="pending"
        ).count()
    grade_counts["전체"] = OptionReviewPolicy.query.filter_by(decision="pending").count()

    decision_counts = {
        k: OptionReviewPolicy.query.filter_by(decision=k).count()
        for k in ["pending", "keep", "addon", "exclude"]
    }

    items = []
    for row in pagination.items:
        items.append({
            "id":          row.id,
            "product_id":  row.master.supplier_product_code if row.master else "-",
            "product_name":row.master.product_name if row.master else "-",
            "main_name":   row.main_option_name or "-",
            "main_price":  row.main_option_price or 0,
            "opt_name":    row.option_name,
            "opt_price":   row.option_price or 0,
            "diff_pct":    row.diff_pct or 0,
            "cheap_count": row.cheap_option_count or 0,
            "keywords":    row.accessory_keywords or "",
            "risk_score":  row.risk_score or 0,
            "grade":       row.risk_grade or "",
            "decision":    row.decision or "pending",
            "decision_label": DECISION_LABEL.get(row.decision, row.decision),
            "note":        row.note or "",
        })

    # addon 확정 현황 (사이드 정보)
    addon_total   = AddonProduct.query.count()
    addon_synced  = AddonProduct.query.filter(AddonProduct.naver_supplement_id != None).count()
    addon_unsynced= addon_total - addon_synced

    return render_template(
        "option_review.html",
        items=items,
        pagination=pagination,
        grade_filter=grade_filter,
        decision_filter=decision_filter,
        grade_counts=grade_counts,
        decision_counts=decision_counts,
        decision_label=DECISION_LABEL,
        search=search,
        per_page=per_page,
        addon_total=addon_total,
        addon_synced=addon_synced,
        addon_unsynced=addon_unsynced,
    )


@option_review_bp.route("/option-review/refresh", methods=["POST"])
@login_required
def refresh_analysis():
    try:
        stats = _run_analysis()
        return jsonify({"ok": True, **stats})
    except Exception as e:
        logger.error(f"[option-review] 분석 갱신 실패: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── 결정 확정 후 AddonProduct 동기화 ─────────────────────────
@option_review_bp.route("/option-review/decide", methods=["POST"])
@login_required
def decide():
    data = request.get_json()
    if not data:
        return jsonify({"ok": False, "error": "no data"}), 400

    ids      = data.get("ids", [])
    decision = data.get("decision", "")
    note     = data.get("note", "")

    if decision not in ("keep", "addon", "exclude", "pending"):
        return jsonify({"ok": False, "error": "invalid decision"}), 400

    now = datetime.now(KST)
    rows = OptionReviewPolicy.query.filter(OptionReviewPolicy.id.in_(ids)).all()
    for row in rows:
        row.decision    = decision
        row.note        = note or row.note
        row.reviewed_at = now if decision != "pending" else None

        # addon 결정 시 AddonProduct 레코드 생성 (없으면)
        if decision == "addon" and row.master_product_id:
            _ensure_addon_record(row)
        # 다른 결정으로 바뀌면 AddonProduct 삭제
        elif decision in ("keep", "exclude", "pending") and row.master_product_id:
            AddonProduct.query.filter_by(
                master_product_id=row.master_product_id,
                option_name=row.option_name,
            ).delete()

    db.session.commit()
    return jsonify({"ok": True, "updated": len(rows)})


def _ensure_addon_record(policy: OptionReviewPolicy):
    """OptionReviewPolicy.decision=='addon' 확정 시 AddonProduct 레코드 보장."""
    existing = AddonProduct.query.filter_by(
        master_product_id=policy.master_product_id,
        option_name=policy.option_name,
    ).first()
    if not existing:
        # 절대가격 = main_price - |diff| (main_option_price 기준)
        # option_price 는 이미 절대가격으로 저장되어 있음
        db.session.add(AddonProduct(
            master_product_id=policy.master_product_id,
            option_name=policy.option_name,
            wholesaler_price=policy.option_price,  # 절대 도매가
        ))


# ── 공개 헬퍼 (actions 모듈에서 import) ─────────────────────
def get_option_policies(master_product_id: int) -> dict[str, str]:
    """
    master_product_id 기준으로 {옵션명: decision} 반환.
    decision 없으면 빈 dict.
    확정된 것(keep/addon/exclude)만 포함, pending 은 제외.
    """
    rows = OptionReviewPolicy.query.filter(
        OptionReviewPolicy.master_product_id == master_product_id,
        OptionReviewPolicy.decision.in_(["keep", "addon", "exclude"]),
    ).all()
    return {r.option_name: r.decision for r in rows}


def get_addon_products(master_product_id: int) -> dict[str, "AddonProduct"]:
    """addon 결정된 옵션의 {옵션명: AddonProduct} 반환."""
    rows = AddonProduct.query.filter_by(master_product_id=master_product_id).all()
    return {r.option_name: r for r in rows}


def sync_addon_supplement_ids(master_product_id: int, product_data: dict):
    """
    Naver GET/PUT 응답의 supplementProductInfo 에서 ID를 읽어 AddonProduct 에 저장.
    product_data: get_origin_product() 반환값
    """
    supp_list = (
        product_data.get("originProduct", {})
        .get("detailAttribute", {})
        .get("supplementProductInfo", {})
        .get("supplementProducts", [])
    )
    changed = False
    for supp in supp_list:
        sid  = supp.get("id")
        name = supp.get("name", "")
        if not sid or not name:
            continue
        addon = AddonProduct.query.filter_by(
            master_product_id=master_product_id,
            option_name=name,
        ).first()
        if addon and addon.naver_supplement_id != sid:
            addon.naver_supplement_id = sid
            addon.last_synced_at = datetime.now(KST)
            changed = True
    if changed:
        db.session.commit()


def build_supplement_payload(master, base_price: int, policies: dict,
                              existing_product_data: dict) -> list:
    """
    addon 정책 옵션들을 네이버 supplementProducts 형식으로 변환.
    기존 naver_supplement_id 가 있으면 id 필드 포함 (업데이트).
    없으면 id 생략 (신규 생성).
    """
    from app.settings import apply_margin

    opt_names = [x.strip() for x in (master.options_text or "").split("\n") if x.strip()]
    opt_diffs: list[int] = []
    if master.option_diffs:
        try:
            opt_diffs = [int(x.strip()) for x in master.option_diffs.split("\n") if x.strip()]
        except ValueError:
            pass

    # 기존 네이버 추가상품 이름→ID 매핑
    existing_supps = (
        existing_product_data.get("originProduct", {})
        .get("detailAttribute", {})
        .get("supplementProductInfo", {})
        .get("supplementProducts", [])
    )
    existing_id_by_name = {s.get("name", ""): s.get("id") for s in existing_supps}

    # AddonProduct 에서 이미 저장된 Naver ID
    addon_records = get_addon_products(master.id)

    supplements = []
    for i, name in enumerate(opt_names):
        if policies.get(name) != "addon":
            continue
        diff = opt_diffs[i] if i < len(opt_diffs) else 0
        abs_wholesale = base_price + diff
        selling_price = apply_margin(abs_wholesale)

        entry: dict = {
            "name":          name,
            "price":         max(selling_price, 1),
            "stockQuantity": 99,
            "usable":        True,
        }
        # 기존 Naver ID 우선 사용
        naver_id = (
            existing_id_by_name.get(name)
            or (addon_records[name].naver_supplement_id if name in addon_records else None)
        )
        if naver_id:
            entry["id"] = naver_id
        supplements.append(entry)

    return supplements
