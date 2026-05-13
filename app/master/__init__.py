import json
import logging
import re
from datetime import date
from app.utils import kst_now
from flask import Blueprint
from app.infrastructure import db
from app.master.models import MasterProduct, ProductEvent

master_bp = Blueprint("master", __name__)
logger = logging.getLogger(__name__)


def _normalize_diffs(diffs: str | None) -> str | None:
    """option_diffs가 전부 0이면 None 반환 (추가금 없음으로 분류)."""
    if not diffs or not diffs.strip():
        return None
    try:
        if all(float(v.strip()) == 0 for v in diffs.split("\n") if v.strip()):
            return None
    except ValueError:
        return None
    return diffs


# 옵션 이름 안에 박힌 도매처 품절 표기 — 발견되면 그 옵션 자체를 제거.
# 옵션이 사라지면 우리 시스템이 자동으로 옵션 구성 변동을 감지해 네이버 쪽도
# 옵션 빼는 흐름으로 동기화됨(OPTION_ADD 시그널). 도매처가 다음에 표기를
# 떼고 다시 보내면 옵션이 복원되어 자연 복귀.
_OPTION_SOLD_OUT_PATTERNS = [
    re.compile(r"\[\s*장기품절\s*\]"),
    re.compile(r"\[\s*일시품절\s*\]"),
    re.compile(r"\[\s*품절\s*\]"),
    re.compile(r"\(\s*품\s*\)\s*\(\s*절\s*\)"),
    re.compile(r"\(\s*품절\s*\)"),
    re.compile(r"\*\s*품절\s*\*"),
]


def _is_option_sold_out(option_name: str) -> bool:
    if not option_name:
        return False
    return any(p.search(option_name) for p in _OPTION_SOLD_OUT_PATTERNS)


def _strip_sold_out_options(extra: dict) -> bool:
    """extra의 옵션 텍스트에서 품절 표기 박힌 줄을 제거(in-place).

    옵션/옵션가/옵션재고는 줄 순서 1:1 대응이라 같은 인덱스로 함께 제거.
    하나라도 제거되면 True 반환.
    """
    opts_text = extra.get("옵션")
    if not isinstance(opts_text, str) or not opts_text.strip():
        return False
    opt_lines = opts_text.split("\n")
    diffs_text = extra.get("옵션가") if isinstance(extra.get("옵션가"), str) else ""
    stocks_text = extra.get("옵션재고") if isinstance(extra.get("옵션재고"), str) else ""
    diff_lines = diffs_text.split("\n") if diffs_text else []
    stock_lines = stocks_text.split("\n") if stocks_text else []

    kept_opts: list[str] = []
    kept_diffs: list[str] = []
    kept_stocks: list[str] = []
    removed = 0
    for i, opt in enumerate(opt_lines):
        if _is_option_sold_out(opt):
            removed += 1
            continue
        kept_opts.append(opt)
        if diff_lines:
            kept_diffs.append(diff_lines[i] if i < len(diff_lines) else "")
        if stock_lines:
            kept_stocks.append(stock_lines[i] if i < len(stock_lines) else "")

    if removed == 0:
        return False

    extra["옵션"] = "\n".join(kept_opts)
    if diff_lines:
        extra["옵션가"] = "\n".join(kept_diffs)
    if stock_lines:
        extra["옵션재고"] = "\n".join(kept_stocks)
    return True


@master_bp.route("/image-check", methods=["GET", "POST"])
def image_check_page():
    """이미지 엑박 수동 검사 페이지.
    GET = 폼만, POST = 선택된 도매처+개수만큼 검사 후 결과 함께 반환.
    """
    from flask_login import login_required as _lr, current_user
    from flask import render_template, request
    from app.wholesalers.models import Wholesaler
    from app.image_check import check_batch

    if not getattr(current_user, "is_authenticated", False):
        from flask import redirect, url_for
        return redirect(url_for("auth.login"))

    wholesalers = Wholesaler.query.filter_by(is_active=True).order_by(Wholesaler.name).all()

    results = None
    checked_count = 0
    selected_wholesaler_id = None
    selected_limit = 50

    if request.method == "POST":
        selected_wholesaler_id = request.form.get("wholesaler_id", type=int)
        selected_limit = max(1, min(request.form.get("limit", default=50, type=int) or 50, 200))
        q = MasterProduct.query.filter(MasterProduct.current_status == "active")
        if selected_wholesaler_id:
            q = q.filter_by(wholesaler_id=selected_wholesaler_id)
        masters = q.limit(selected_limit).all()
        checked_count = len(masters)
        results = check_batch(masters)

    return render_template(
        "image_check.html",
        wholesalers=wholesalers,
        results=results,
        checked_count=checked_count,
        broken_count=len(results) if results is not None else 0,
        selected_wholesaler_id=selected_wholesaler_id,
        selected_limit=selected_limit,
    )


@master_bp.route("/changes")
def changes():
    from flask_login import login_required, current_user
    from flask import render_template, request
    from app.wholesalers.models import Wholesaler

    CHANGE_TYPES = ["IMAGE_CHANGE", "NAME_CHANGE", "DETAIL_CHANGE", "SHIPPING_CHANGE", "PRICE_CHANGE"]

    wholesaler_id = request.args.get("wholesaler_id", type=int)
    event_type = request.args.get("event_type", "")
    page = request.args.get("page", 1, type=int)
    per_page = 50

    query = ProductEvent.query.filter(ProductEvent.event_type.in_(CHANGE_TYPES))
    if wholesaler_id:
        query = query.join(MasterProduct).filter(MasterProduct.wholesaler_id == wholesaler_id)
    if event_type and event_type in CHANGE_TYPES:
        query = query.filter(ProductEvent.event_type == event_type)

    query = query.order_by(ProductEvent.event_date.desc(), ProductEvent.id.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    wholesalers = Wholesaler.query.filter_by(is_active=True).order_by(Wholesaler.name).all()

    return render_template(
        "changes.html",
        events=pagination.items,
        pagination=pagination,
        wholesalers=wholesalers,
        selected_wholesaler_id=wholesaler_id,
        selected_event_type=event_type,
        change_types=CHANGE_TYPES,
    )

# 연속 미수집 N일 이상 → 상태 전환 기준 (CLAUDE.md 9-3 준수)
MISSING_DAYS_CANDIDATE = 1     # active → missing
MISSING_DAYS_DISCONTINUED = 60  # missing → discontinued_candidate
MISSING_DAYS_FINAL = 365        # discontinued_candidate → discontinued (1년 이상 미수집)


def process_master_update(wholesaler_id: int, items: list, snapshot_date: date = None) -> dict:
    if snapshot_date is None:
        snapshot_date = kst_now().date()

    from app.wholesalers.models import Wholesaler
    wholesaler = Wholesaler.query.get(wholesaler_id)
    prefix = (wholesaler.prefix or "") if wholesaler else ""

    # 도매처가 옵션 이름에 박아 보낸 품절 표기를 제거(옵션 자체를 빼버림).
    # 도매처마다 표기 형식 다름: 철물박사 [장기품절]/[일시품절], 젠트레이드 (품)(절) 등.
    _sold_out_removed_total = 0
    for it in items:
        extra = it.get("extra")
        if isinstance(extra, dict) and _strip_sold_out_options(extra):
            _sold_out_removed_total += 1
    if _sold_out_removed_total:
        logger.info(f"[master] 옵션 품절 표기 제거: {_sold_out_removed_total}개 상품에서 옵션 정리됨")

    today_map = {
        f"{prefix}{item['source_product_code']}": item
        for item in items
        if item.get("source_product_code")
    }

    stats = {
        "new": 0,
        "restocked": 0,
        "missing": 0,
        "discontinued_candidate": 0,
        "discontinued": 0,
        "price_change": 0,
        "image_change": 0,
        "name_change": 0,
        "detail_change": 0,
        "shipping_change": 0,
    }

    existing = MasterProduct.query.filter_by(wholesaler_id=wholesaler_id).all()
    existing_map = {p.supplier_product_code: p for p in existing}
    existing_codes = set(existing_map.keys())

    new_events = []

    # 1. 오늘 수집된 상품 처리
    for code, item in today_map.items():
        if code not in existing_map:
            # 신규 상품
            extra = item.get("extra") or {}
            _opt_text = extra.get("옵션")
            _opt_diffs = extra.get("옵션가")
            _opt_stocks = extra.get("옵션재고")
            _init_status = "active"
            if item.get("status") == "out_of_stock":
                _init_status = "out_of_stock"
            master = MasterProduct(
                wholesaler_id=wholesaler_id,
                supplier_product_code=code,
                product_name=item.get("product_name"),
                price=item.get("price"),
                supply_price=item.get("supply_price"),
                image_url=item.get("image_url"),
                category_name=item.get("category_name"),
                detail_description=item.get("detail_description"),
                product_url=item.get("product_url") or item.get("detail_url"),
                origin=item.get("origin"),
                shipping_fee=item.get("shipping_fee"),
                shipping_condition=item.get("shipping_condition"),
                options_text=_opt_text if isinstance(_opt_text, str) else None,
                option_diffs=_normalize_diffs(_opt_diffs if isinstance(_opt_diffs, str) else None),
                option_stocks=_opt_stocks if isinstance(_opt_stocks, str) else None,
                current_status=_init_status,
                first_seen_date=snapshot_date,
                last_seen_date=snapshot_date,
                missing_days=0,
                last_status_change_date=snapshot_date,
            )
            db.session.add(master)
            db.session.flush()
            new_events.append(ProductEvent(
                master_product_id=master.id,
                event_type="NEW",
                event_date=snapshot_date,
                after_value=json.dumps({"price": item.get("price")}, ensure_ascii=False),
            ))
            stats["new"] += 1

        else:
            master = existing_map[code]
            prev_status = master.current_status

            # 이전에 미수집/단종후보/품절이었으면 재입고
            if prev_status in ("missing", "discontinued_candidate", "out_of_stock", "discontinued"):
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="RESTOCKED",
                    event_date=snapshot_date,
                    before_value=json.dumps({
                        "status": prev_status,
                        "missing_days": master.missing_days
                    }, ensure_ascii=False),
                    after_value=json.dumps({"status": "active"}, ensure_ascii=False),
                ))
                stats["restocked"] += 1

            # 가격 변동
            new_price = item.get("price")
            if master.price and new_price and master.price != new_price:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="PRICE_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"price": master.price}, ensure_ascii=False),
                    after_value=json.dumps({"price": new_price}, ensure_ascii=False),
                ))
                stats["price_change"] += 1

            # 이미지 변동
            new_img = item.get("image_url")
            if master.image_url and new_img and master.image_url != new_img:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="IMAGE_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"image_url": master.image_url}, ensure_ascii=False),
                    after_value=json.dumps({"image_url": new_img}, ensure_ascii=False),
                ))
                stats["image_change"] += 1

            # 상품명 변동
            new_name = item.get("product_name")
            if master.product_name and new_name and master.product_name != new_name:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="NAME_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"name": master.product_name}, ensure_ascii=False),
                    after_value=json.dumps({"name": new_name}, ensure_ascii=False),
                ))
                stats["name_change"] += 1

            # 상세페이지 변동
            new_detail = item.get("detail_description")
            if master.detail_description and new_detail and master.detail_description != new_detail:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="DETAIL_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"chars": len(master.detail_description)}, ensure_ascii=False),
                    after_value=json.dumps({"chars": len(new_detail), "url": master.product_url or ""}, ensure_ascii=False),
                ))
                stats["detail_change"] += 1
                # 매칭된 네이버 스토어 상품마다 상세페이지 갱신 시그널 생성.
                # 운영자가 액션관리 화면에서 승인하면 네이버 detailContent로 PUT.
                # 중복 방지: 같은 (master, store) 쌍에 pending이 이미 있으면 skip.
                # CLOSE/PROHIBITION/UNADMISSION 상품은 PUT 거부됨 → 시그널 생성 차단.
                from app.store.models import StoreProduct
                from app.actions.models import ActionSignal
                for _sp in StoreProduct.query.filter_by(master_product_id=master.id).all():
                    if not _sp.origin_product_no:
                        continue
                    if _sp.store_status in ("CLOSE", "PROHIBITION", "UNADMISSION"):
                        continue
                    _exists = ActionSignal.query.filter_by(
                        master_product_id=master.id,
                        store_product_id=_sp.id,
                        signal_type="DETAIL_CHANGE",
                        status="pending",
                    ).first()
                    if _exists:
                        continue
                    db.session.add(ActionSignal(
                        master_product_id=master.id,
                        store_product_id=_sp.id,
                        signal_type="DETAIL_CHANGE",
                        current_value=json.dumps(
                            {"chars": len(master.detail_description or "")},
                            ensure_ascii=False,
                        ),
                        suggested_value=json.dumps(
                            {"chars": len(new_detail or ""), "url": master.product_url or ""},
                            ensure_ascii=False,
                        ),
                    ))

            # 배송비/배송조건 변동
            new_shipping_fee = item.get("shipping_fee")
            new_shipping_cond = item.get("shipping_condition")
            shipping_fee_changed = (
                new_shipping_fee is not None
                and master.shipping_fee is not None
                and master.shipping_fee != new_shipping_fee
            )
            shipping_cond_changed = (
                new_shipping_cond
                and master.shipping_condition
                and master.shipping_condition != new_shipping_cond
            )
            if shipping_fee_changed or shipping_cond_changed:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="SHIPPING_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({
                        "shipping_fee": master.shipping_fee,
                        "shipping_condition": master.shipping_condition,
                    }, ensure_ascii=False),
                    after_value=json.dumps({
                        "shipping_fee": new_shipping_fee,
                        "shipping_condition": new_shipping_cond,
                    }, ensure_ascii=False),
                ))
                stats["shipping_change"] += 1

            # 옵션 변동 (문자열 형식만 저장 — 도매처별 표준화 전까지 리스트는 무시)
            extra = item.get("extra") or {}
            _raw_options = extra.get("옵션")
            _raw_diffs = extra.get("옵션가")
            _raw_stocks = extra.get("옵션재고")
            new_options = _raw_options if isinstance(_raw_options, str) else None
            new_diffs = _normalize_diffs(_raw_diffs if isinstance(_raw_diffs, str) else None)
            new_option_stocks = _raw_stocks if isinstance(_raw_stocks, str) else None

            if master.option_diffs and new_diffs and master.option_diffs != new_diffs:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="OPTION_PRICE_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"option_diffs": master.option_diffs}, ensure_ascii=False),
                    after_value=json.dumps({"option_diffs": new_diffs}, ensure_ascii=False),
                ))

            if master.option_stocks is not None and new_option_stocks is not None and master.option_stocks != new_option_stocks:
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="OPTION_STOCK_CHANGE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"option_stocks": master.option_stocks}, ensure_ascii=False),
                    after_value=json.dumps({"option_stocks": new_option_stocks}, ensure_ascii=False),
                ))

            # 마스터 갱신
            master.last_seen_date = snapshot_date

            # 품절 상태 처리
            item_status = item.get("status", "active")

            # discontinued 확정 상태는 자동 active 복귀 금지 — 수동 복구만 허용 (CLAUDE.md 9-3)
            # last_seen_date 만 위에서 갱신했으므로 수동 복구 시 참조 가능. status·missing_days 변경 안 함.
            if master.current_status == "discontinued":
                pass
            elif item_status == "out_of_stock":
                # out_of_stock 누적: 품절 일수도 missing_days 에 함께 카운팅 (CLAUDE.md 9-3)
                master.missing_days = (master.missing_days or 0) + 1
                if master.current_status != "out_of_stock":
                    master.last_status_change_date = snapshot_date
                master.current_status = "out_of_stock"
            else:
                master.missing_days = 0
                if master.current_status != "active":
                    master.last_status_change_date = snapshot_date
                master.current_status = "active"

            master.option_stocks = new_option_stocks
            if new_name:
                master.product_name = new_name
            if new_price is not None:
                master.price = new_price
            new_supply = item.get("supply_price")
            if new_supply is not None:
                master.supply_price = new_supply
            if new_img:
                master.image_url = new_img
            new_cat = item.get("category_name")
            if new_cat:
                master.category_name = new_cat
            if item.get("detail_description"):
                master.detail_description = item.get("detail_description")
            new_url = item.get("product_url") or item.get("detail_url")
            if new_url:
                master.product_url = new_url
            if item.get("origin"):
                master.origin = item.get("origin")
            if item.get("shipping_fee") is not None:
                master.shipping_fee = item.get("shipping_fee")
            if item.get("shipping_condition"):
                master.shipping_condition = item.get("shipping_condition")
            extra = item.get("extra") or {}
            if item.get("brand_name") or extra.get("브랜드"):
                master.brand_name = item.get("brand_name") or extra.get("브랜드")
            if item.get("manufacturer") or extra.get("제조사"):
                master.manufacturer = item.get("manufacturer") or extra.get("제조사")
            if item.get("model_name") or extra.get("모델명"):
                master.model_name = item.get("model_name") or extra.get("모델명")
            if item.get("keywords") or extra.get("키워드"):
                master.keywords = item.get("keywords") or extra.get("키워드")
            if item.get("tax_type") or extra.get("과세여부"):
                master.tax_type = item.get("tax_type") or extra.get("과세여부")
            if item.get("certification") or extra.get("인증정보"):
                import json as _json
                val = item.get("certification") or extra.get("인증정보")
                master.certification = _json.dumps(val, ensure_ascii=False) if isinstance(val, (list, dict)) else str(val)
            # 추가이미지: extra["추가이미지1"..5] → 줄바꿈 구분 문자열
            add_imgs = [
                extra.get("추가이미지1") or extra.get("additional_image_1"),
                extra.get("추가이미지2") or extra.get("additional_image_2"),
                extra.get("추가이미지3") or extra.get("additional_image_3"),
                extra.get("추가이미지4") or extra.get("additional_image_4"),
                extra.get("추가이미지5") or extra.get("additional_image_5"),
            ]
            add_imgs = [u for u in add_imgs if u]
            if add_imgs:
                master.additional_images = "\n".join(add_imgs)
            master.options_text = new_options
            master.option_diffs = new_diffs

    # 2. 오늘 수집에서 빠진 상품 처리 (미수집)
    missing_codes = existing_codes - set(today_map.keys())
    for code in missing_codes:
        master = existing_map[code]

        if master.current_status == "discontinued":
            continue  # 이미 단종 확정이면 건너뜀

        master.missing_days = (master.missing_days or 0) + 1

        if master.missing_days >= MISSING_DAYS_FINAL:
            # 1년 이상 미수집 → 단종 확정
            if master.current_status != "discontinued":
                master.current_status = "discontinued"
                master.last_status_change_date = snapshot_date
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="DISCONTINUED",
                    event_date=snapshot_date,
                    before_value=json.dumps({"missing_days": master.missing_days}, ensure_ascii=False),
                ))
                stats["discontinued"] += 1
            continue  # 단종 확정, 아래 후보 전환 로직 스킵

        elif master.missing_days >= MISSING_DAYS_DISCONTINUED:
            if master.current_status != "discontinued_candidate":
                master.current_status = "discontinued_candidate"
                master.last_status_change_date = snapshot_date
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="DISCONTINUED_CANDIDATE",
                    event_date=snapshot_date,
                    before_value=json.dumps({"missing_days": master.missing_days}, ensure_ascii=False),
                ))
                stats["discontinued_candidate"] += 1

        elif master.missing_days >= MISSING_DAYS_CANDIDATE:
            if master.current_status != "missing":
                master.current_status = "missing"
                master.last_status_change_date = snapshot_date
                new_events.append(ProductEvent(
                    master_product_id=master.id,
                    event_type="MISSING",
                    event_date=snapshot_date,
                    before_value=json.dumps({"missing_days": master.missing_days}, ensure_ascii=False),
                ))
                stats["missing"] += 1

    for event in new_events:
        db.session.add(event)

    db.session.commit()
    logger.info(f"[master] 업데이트 완료: {stats}")
    return stats
