from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required
from app.store.models import NaverStore

inquiries_bp = Blueprint("inquiries", __name__)


@inquiries_bp.route("/inquiries")
@login_required
def inquiries_page():
    stores = NaverStore.query.order_by(NaverStore.store_name).all()
    store_id = request.args.get("store_id", type=int)
    answered_type = request.args.get("answered_type", "UNANSWERED")

    if not store_id and stores:
        store_id = stores[0].id

    selected_store = NaverStore.query.get(store_id) if store_id else None
    qnas = []
    templates = []
    error = None

    if selected_store:
        try:
            from store.naver.inquiries import get_qnas, get_answer_templates
            result = get_qnas(answered_type, client_id=selected_store.client_id, client_secret=selected_store.client_secret)
            qnas = result.get("contents", [])
            tmpl_result = get_answer_templates(client_id=selected_store.client_id, client_secret=selected_store.client_secret)
            templates = tmpl_result.get("contents", [])
        except Exception as e:
            error = str(e)

    return render_template(
        "inquiries.html",
        stores=stores,
        selected_store=selected_store,
        qnas=qnas,
        templates=templates,
        answered_type=answered_type,
        error=error,
    )


@inquiries_bp.route("/inquiries/debug")
@login_required
def inquiries_debug():
    store_id = request.args.get("store_id", type=int)
    if not store_id:
        return jsonify({"error": "store_id 필요"})
    store = NaverStore.query.get_or_404(store_id)
    from store.naver import API_BASE, _get_access_token
    import requests as req
    token = _get_access_token(store.client_id, store.client_secret)
    headers = {"Authorization": f"Bearer {token}"}
    results = {}
    candidates = [
        ("/v1/pay-user/inquiries", {"size": 10}),
        ("/v1/pay-user/inquiries", {"answeredType": "UNANSWERED", "size": 10}),
        ("/v1/seller/product-orders/inquiries", {"size": 10}),
        ("/v1/contents/seller/qnas", {"size": 10}),
    ]
    for path, params in candidates:
        try:
            r = req.get(f"{API_BASE}{path}", headers=headers, params=params, timeout=10)
            results[path + "?" + "&".join(f"{k}={v}" for k,v in params.items())] = {
                "status": r.status_code,
                "body": r.json() if r.ok else r.text[:300]
            }
        except Exception as e:
            results[path] = {"error": str(e)}
    return jsonify(results)


@inquiries_bp.route("/inquiries/<question_id>/answer", methods=["POST"])
@login_required
def answer_qna(question_id):
    store_id = request.form.get("store_id", type=int)
    answer_content = request.form.get("answer_content", "").strip()
    answered_type = request.form.get("answered_type", "UNANSWERED")

    if not answer_content:
        flash("답변 내용을 입력하세요.", "error")
        return redirect(url_for("inquiries.inquiries_page", store_id=store_id, answered_type=answered_type))

    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.inquiries import answer_qna as api_answer
        api_answer(question_id, answer_content, client_id=store.client_id, client_secret=store.client_secret)
        flash("답변 등록 완료", "success")
    except Exception as e:
        flash(f"답변 등록 실패: {e}", "error")
    return redirect(url_for("inquiries.inquiries_page", store_id=store_id, answered_type="UNANSWERED"))
