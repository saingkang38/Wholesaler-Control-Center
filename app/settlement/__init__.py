from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from app.store.models import NaverStore
from datetime import date

settlement_bp = Blueprint("settlement", __name__)


@settlement_bp.route("/settlement")
@login_required
def settlement_page():
    stores = NaverStore.query.order_by(NaverStore.store_name).all()
    store_id = request.args.get("store_id", type=int)
    today = date.today()
    start_date = request.args.get("start_date", today.replace(day=1).isoformat())
    end_date = request.args.get("end_date", today.isoformat())

    if not store_id and stores:
        store_id = stores[0].id

    selected_store = NaverStore.query.get(store_id) if store_id else None
    rows = []
    error = None

    if selected_store:
        try:
            from store.naver.settlement import get_daily_settlement
            result = get_daily_settlement(
                start_date, end_date,
                client_id=selected_store.client_id,
                client_secret=selected_store.client_secret,
            )
            rows = result.get("elements", result.get("data", []))
        except Exception as e:
            error = str(e)

    return render_template(
        "settlement.html",
        stores=stores,
        selected_store=selected_store,
        rows=rows,
        start_date=start_date,
        end_date=end_date,
        error=error,
    )


@settlement_bp.route("/settlement/debug")
@login_required
def settlement_debug():
    store_id = request.args.get("store_id", type=int)
    if not store_id:
        return jsonify({"error": "store_id 필요"})
    store = NaverStore.query.get_or_404(store_id)
    results = {}
    for label, fn_name, kwargs in [
        ("daily", "get_daily_settlement", {"start_date": "2026-03-01", "end_date": "2026-03-21"}),
        ("vat", "get_vat_daily", {"year_month": "2026-02"}),
    ]:
        try:
            import importlib
            mod = importlib.import_module("store.naver.settlement")
            fn = getattr(mod, fn_name)
            results[label] = fn(client_id=store.client_id, client_secret=store.client_secret, **kwargs)
        except Exception as e:
            results[label] = {"error": str(e)}

    # 부가세 엔드포인트 후보 탐색
    from store.naver import API_BASE, _get_access_token
    import requests as req
    token = _get_access_token(store.client_id, store.client_secret)
    headers = {"Authorization": f"Bearer {token}"}
    vat_candidates = [
        ("/v1/pay-settle/vat/daily", {"yearMonth": "202603"}),
        ("/v1/pay-settle/vat/daily", {"yearMonth": "2026-03"}),
        ("/v1/pay-settle/vat/monthly", {"yearMonth": "202603"}),
        ("/v1/pay-settle/vat", {"yearMonth": "202603"}),
        ("/v1/pay-settle/settle/vat", {"startDate": "2026-03-01", "endDate": "2026-03-21"}),
    ]
    vat_debug = {}
    for path, params in vat_candidates:
        try:
            r = req.get(f"{API_BASE}{path}", headers=headers, params=params, timeout=10)
            vat_debug[path + "?" + "&".join(f"{k}={v}" for k,v in params.items())] = {
                "status": r.status_code, "body": r.json() if r.ok else r.text[:400]
            }
        except Exception as e:
            vat_debug[path] = {"error": str(e)}
    results["vat_debug"] = vat_debug
    return jsonify(results)


@settlement_bp.route("/settlement/vat")
@login_required
def settlement_vat_page():
    stores = NaverStore.query.order_by(NaverStore.store_name).all()
    store_id = request.args.get("store_id", type=int)
    today = date.today()
    first_of_month = today.replace(day=1)
    prev_month = (first_of_month - __import__('datetime').timedelta(days=1))
    default_ym = prev_month.strftime("%Y-%m")
    year_month = request.args.get("year_month", default_ym)

    if not store_id and stores:
        store_id = stores[0].id

    selected_store = NaverStore.query.get(store_id) if store_id else None
    rows = []
    error = None

    vat_keys = None
    if selected_store:
        try:
            from store.naver.settlement import get_vat_daily
            result = get_vat_daily(
                year_month,
                client_id=selected_store.client_id,
                client_secret=selected_store.client_secret,
            )
            rows = result.get("elements", result.get("data", []))
            if rows:
                vat_keys = list(rows[0].keys())
        except Exception as e:
            error = str(e)

    totals = {k: sum(r.get(k, 0) or 0 for r in rows) for k in ["totalSalesAmount", "creditCardAmount", "cashInComeDeductionAmount", "cashOutGoingEvidenceAmount", "otherAmount"]} if rows else {}

    return render_template(
        "settlement.html",
        stores=stores,
        selected_store=selected_store,
        rows=rows,
        start_date=year_month + "-01",
        end_date=year_month,
        year_month=year_month,
        vat_mode=True,
        vat_keys=vat_keys,
        totals=totals,
        error=error,
    )
