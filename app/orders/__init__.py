from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required
from app.store.models import NaverStore

orders_bp = Blueprint("orders", __name__)

DELIVERY_COMPANIES = [
    ("CJGLS", "CJ대한통운"),
    ("HANJIN", "한진택배"),
    ("LOTTE", "롯데택배"),
    ("EPOST", "우체국택배"),
    ("KDEXP", "경동택배"),
    ("ILYANG", "일양로지스"),
    ("HYUNDAI", "현대택배"),
    ("DAESIN", "대신택배"),
    ("CHUNIL", "천일택배"),
    ("HDEXP", "합동택배"),
]


@orders_bp.route("/orders")
@login_required
def orders_page():
    stores = NaverStore.query.order_by(NaverStore.store_name).all()
    store_id = request.args.get("store_id", type=int)
    status_filter = request.args.get("status", "")
    search_mode = request.args.get("search_mode", "recent")  # recent | date_range
    from datetime import date, timedelta
    default_start = (date.today() - timedelta(days=7)).isoformat()
    default_end = date.today().isoformat()
    start_date = request.args.get("start_date", default_start)
    end_date = request.args.get("end_date", default_end)

    if not store_id and stores:
        store_id = stores[0].id

    selected_store = NaverStore.query.get(store_id) if store_id else None
    orders = []
    error = None

    if selected_store:
        try:
            if search_mode == "date_range":
                from store.naver.orders import search_orders
                result = search_orders(
                    start_date, end_date,
                    statuses=[status_filter] if status_filter else [],
                    client_id=selected_store.client_id,
                    client_secret=selected_store.client_secret,
                )
                raw = result.get("data", result.get("contents", result.get("elements", [])))
                orders = raw if isinstance(raw, list) else []
            else:
                from store.naver.orders import get_changed_order_statuses, query_product_orders
                from datetime import datetime, timedelta, timezone
                kst = timezone(timedelta(hours=9))
                since = (datetime.now(kst) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000+09:00")
                changed = get_changed_order_statuses(
                    since,
                    client_id=selected_store.client_id,
                    client_secret=selected_store.client_secret,
                )
                data_wrapper = changed.get("data", changed)
                if isinstance(data_wrapper, list):
                    status_list = data_wrapper
                else:
                    status_list = data_wrapper.get("lastChangeStatuses", [])
                ids = [item["productOrderId"] for item in status_list]
                if ids:
                    detail = query_product_orders(
                        ids,
                        client_id=selected_store.client_id,
                        client_secret=selected_store.client_secret,
                    )
                    raw = detail.get("data", detail.get("contents", []))
                    all_orders = raw if isinstance(raw, list) else []
                    if status_filter:
                        orders = [o for o in all_orders if o.get("productOrder", {}).get("productOrderStatus") == status_filter]
                    else:
                        orders = all_orders
        except Exception as e:
            error = str(e)

    return render_template(
        "orders.html",
        stores=stores,
        selected_store=selected_store,
        orders=orders,
        status_filter=status_filter,
        search_mode=search_mode,
        start_date=start_date,
        end_date=end_date,
        delivery_companies=DELIVERY_COMPANIES,
        error=error,
    )


@orders_bp.route("/orders/debug")
@login_required
def orders_debug():
    store_id = request.args.get("store_id", type=int)
    if not store_id:
        return jsonify({"error": "store_id 필요"})
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.orders import get_changed_order_statuses
        from datetime import datetime, timedelta, timezone
        kst = timezone(timedelta(hours=9))
        since = (datetime.now(kst) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.000+09:00")
        changed = get_changed_order_statuses(since, client_id=store.client_id, client_secret=store.client_secret)
        data_wrapper = changed.get("data", changed)
        status_list = data_wrapper.get("lastChangeStatuses", []) if isinstance(data_wrapper, dict) else data_wrapper
        ids = [item["productOrderId"] for item in status_list][:5]  # 최대 5개만 조회
        from store.naver.orders import query_product_orders
        detail = query_product_orders(ids, client_id=store.client_id, client_secret=store.client_secret) if ids else {}
        return jsonify({"changed_raw": changed, "query_raw": detail})
    except Exception as e:
        return jsonify({"error": str(e)})


@orders_bp.route("/orders/dispatch", methods=["POST"])
@login_required
def dispatch_order():
    store_id = request.form.get("store_id", type=int)
    product_order_id = request.form.get("product_order_id")
    delivery_company = request.form.get("delivery_company")
    tracking_number = request.form.get("tracking_number")

    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.orders import dispatch_orders
        dispatch_orders(
            [{"productOrderId": product_order_id, "deliveryCompanyCode": delivery_company, "trackingNumber": tracking_number}],
            client_id=store.client_id,
            client_secret=store.client_secret,
        )
        flash("발송 처리 완료", "success")
    except Exception as e:
        flash(f"발송 처리 실패: {e}", "error")
    return redirect(url_for("orders.orders_page", store_id=store_id, status="DELIVERING"))


@orders_bp.route("/orders/cancel", methods=["POST"])
@login_required
def cancel_order():
    store_id = request.form.get("store_id", type=int)
    product_order_id = request.form.get("product_order_id")
    reason = request.form.get("reason", "구매자 요청")

    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.orders import cancel_order as api_cancel
        api_cancel(product_order_id, reason, client_id=store.client_id, client_secret=store.client_secret)
        flash("취소 승인 완료", "success")
    except Exception as e:
        flash(f"취소 승인 실패: {e}", "error")
    return redirect(url_for("orders.orders_page", store_id=store_id, status="CANCEL_DONE"))


@orders_bp.route("/orders/return", methods=["POST"])
@login_required
def approve_return():
    store_id = request.form.get("store_id", type=int)
    product_order_id = request.form.get("product_order_id")

    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.orders import approve_return as api_return
        api_return(product_order_id, client_id=store.client_id, client_secret=store.client_secret)
        flash("반품 승인 완료", "success")
    except Exception as e:
        flash(f"반품 승인 실패: {e}", "error")
    return redirect(url_for("orders.orders_page", store_id=store_id, status="RETURN_DONE"))


@orders_bp.route("/orders/exchange", methods=["POST"])
@login_required
def approve_exchange():
    store_id = request.form.get("store_id", type=int)
    product_order_id = request.form.get("product_order_id")

    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.orders import approve_exchange as api_exchange
        api_exchange(product_order_id, client_id=store.client_id, client_secret=store.client_secret)
        flash("교환 승인 완료", "success")
    except Exception as e:
        flash(f"교환 승인 실패: {e}", "error")
    return redirect(url_for("orders.orders_page", store_id=store_id, status="EXCHANGE_REQUEST"))
