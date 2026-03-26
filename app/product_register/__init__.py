import os
import threading
from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify, current_app
from flask_login import login_required
from app.store.models import NaverStore, DeliveryPreset, BulkRegisterJob, BulkRegisterItem, StoreProduct
from app.infrastructure import db

product_register_bp = Blueprint("product_register", __name__)


@product_register_bp.route("/product-register")
@login_required
def product_register_page():
    stores = NaverStore.query.order_by(NaverStore.store_name).all()
    presets = DeliveryPreset.query.order_by(DeliveryPreset.id).all()
    store_id = request.args.get("store_id", type=int)
    master_id = request.args.get("master_id", type=int)

    if not store_id and stores:
        store_id = stores[0].id

    selected_store = NaverStore.query.get(store_id) if store_id else None
    master = None
    if master_id:
        from app.master.models import MasterProduct
        master = MasterProduct.query.get(master_id)

    return render_template(
        "product_register.html",
        stores=stores,
        selected_store=selected_store,
        presets=presets,
        master=master,
    )


@product_register_bp.route("/product-register/search-master")
@login_required
def search_master():
    q = request.args.get("q", "").strip()
    wholesaler_id = request.args.get("wholesaler_id", type=int)
    if not q:
        return jsonify([])
    from app.master.models import MasterProduct
    from app.wholesalers.models import Wholesaler
    query = MasterProduct.query.filter(
        MasterProduct.product_name.ilike(f"%{q}%")
    )
    if wholesaler_id:
        query = query.filter_by(wholesaler_id=wholesaler_id)
    results = query.limit(20).all()
    return jsonify([{
        "id": m.id,
        "product_name": m.product_name,
        "price": m.price,
        "supply_price": m.supply_price,
        "image_url": m.image_url,
        "supplier_product_code": m.supplier_product_code,
        "wholesaler_name": m.wholesaler.name if m.wholesaler else "",
    } for m in results])


@product_register_bp.route("/product-register/search-category")
@login_required
def search_category():
    q = request.args.get("q", "").strip()
    store_id = request.args.get("store_id", type=int)
    if not q or not store_id:
        return jsonify([])
    store = NaverStore.query.get_or_404(store_id)
    try:
        from store.naver.seller import get_categories
        result = get_categories(client_id=store.client_id, client_secret=store.client_secret)
        cats = result.get("categories", result.get("data", []))
        matched = [c for c in cats if q.lower() in c.get("wholeCategoryName", "").lower()][:20]
        return jsonify([{
            "id": c.get("id"),
            "name": c.get("wholeCategoryName", c.get("name", "")),
        } for c in matched])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@product_register_bp.route("/product-register/submit", methods=["POST"])
@login_required
def submit_register():
    store_id = request.form.get("store_id", type=int)
    store = NaverStore.query.get_or_404(store_id)

    name = request.form.get("name", "").strip()
    category_id = request.form.get("category_id", "").strip()
    sale_price = request.form.get("sale_price", type=int)
    stock_qty = request.form.get("stock_qty", 100, type=int)
    image_url = request.form.get("image_url", "").strip()
    detail_content = request.form.get("detail_content", "").strip()
    seller_code = request.form.get("seller_code", "").strip()
    preset_id = request.form.get("preset_id", type=int)
    after_service_tel = request.form.get("after_service_tel", "").strip()

    if not all([name, category_id, sale_price, image_url]):
        flash("상품명, 카테고리, 판매가, 이미지 URL은 필수입니다.", "error")
        return redirect(url_for("product_register.product_register_page", store_id=store_id))

    preset = DeliveryPreset.query.get(preset_id) if preset_id else None

    try:
        # 이미지 업로드
        from store.naver.products import upload_image_from_url, register_product
        naver_image_url = upload_image_from_url(image_url, store.client_id, store.client_secret)

        # 배송비 설정
        if preset:
            delivery_info = {
                "deliveryType": "DELIVERY",
                "deliveryAttributeType": "NORMAL",
                "deliveryFeeType": preset.delivery_fee_type,
                "baseFee": preset.base_fee,
                "freeConditionalAmount": preset.free_condition_amount if preset.delivery_fee_type == "CONDITIONAL_FREE" else 0,
                "deliveryFeePayType": preset.delivery_fee_pay_type,
            }
        else:
            delivery_info = {
                "deliveryType": "DELIVERY",
                "deliveryAttributeType": "NORMAL",
                "deliveryFeeType": "FREE",
                "baseFee": 0,
                "deliveryFeePayType": "PREPAID",
            }

        origin_product = {
            "statusType": "SALE",
            "saleType": "NEW",
            "leafCategoryId": category_id,
            "name": name,
            "detailContent": detail_content or name,
            "images": {
                "representativeImage": {"url": naver_image_url},
            },
            "salePrice": sale_price,
            "stockQuantity": stock_qty,
            "deliveryInfo": delivery_info,
            "detailAttribute": {
                "afterServiceInfo": {
                    "afterServiceTelephoneNumber": after_service_tel or os.getenv("DEFAULT_AS_TEL", "010-0000-0000"),
                    "afterServiceGuideContent": os.getenv("DEFAULT_AS_GUIDE", "고객센터로 문의해주세요."),
                },
                "originAreaInfo": {
                    "originAreaCode": "0200037",  # 국내산 기본값
                    "importer": "",
                },
            },
        }
        if seller_code:
            origin_product["sellerManagementCode"] = seller_code

        payload = {
            "originProduct": origin_product,
            "smartstoreChannelProduct": {
                "naverShoppingRegistration": True,
                "channelProductDisplayStatusType": "ON",
            },
        }

        result = register_product(payload, store.client_id, store.client_secret)
        origin_no = result.get("originProductNo") or result.get("data", {}).get("originProductNo")
        flash(f"상품 등록 완료! 원상품번호: {origin_no}", "success")

    except Exception as e:
        flash(f"등록 실패: {e}", "error")

    return redirect(url_for("product_register.product_register_page", store_id=store_id))


# 배송비 프리셋 관리
@product_register_bp.route("/delivery-presets")
@login_required
def delivery_presets_page():
    presets = DeliveryPreset.query.order_by(DeliveryPreset.id).all()
    return render_template("delivery_presets.html", presets=presets)


@product_register_bp.route("/delivery-presets/create", methods=["POST"])
@login_required
def create_preset():
    name = request.form.get("name", "").strip()
    fee_type = request.form.get("delivery_fee_type", "FREE")
    base_fee = request.form.get("base_fee", 0, type=int)
    free_cond = request.form.get("free_condition_amount", 0, type=int)
    pay_type = request.form.get("delivery_fee_pay_type", "PREPAID")

    if not name:
        flash("이름을 입력하세요.", "error")
        return redirect(url_for("product_register.delivery_presets_page"))

    p = DeliveryPreset(
        name=name,
        delivery_fee_type=fee_type,
        base_fee=base_fee,
        free_condition_amount=free_cond,
        delivery_fee_pay_type=pay_type,
    )
    db.session.add(p)
    db.session.commit()
    flash("배송비 프리셋 등록 완료", "success")
    return redirect(url_for("product_register.delivery_presets_page"))


@product_register_bp.route("/delivery-presets/<int:preset_id>/delete", methods=["POST"])
@login_required
def delete_preset(preset_id):
    p = DeliveryPreset.query.get_or_404(preset_id)
    db.session.delete(p)
    db.session.commit()
    flash("삭제 완료", "success")
    return redirect(url_for("product_register.delivery_presets_page"))


# ─── 대량 등록 ────────────────────────────────────────────────────────────────

@product_register_bp.route("/bulk-register")
@login_required
def bulk_register_page():
    stores = NaverStore.query.order_by(NaverStore.store_name).all()
    presets = DeliveryPreset.query.order_by(DeliveryPreset.id).all()
    store_id = request.args.get("store_id", type=int)
    if not store_id and stores:
        store_id = stores[0].id
    selected_store = NaverStore.query.get(store_id) if store_id else None
    return render_template(
        "bulk_register.html",
        stores=stores,
        selected_store=selected_store,
        presets=presets,
    )


@product_register_bp.route("/bulk-register/unregistered")
@login_required
def bulk_unregistered():
    store_id = request.args.get("store_id", type=int)
    if not store_id:
        return jsonify([])

    from app.master.models import MasterProduct
    # 해당 스토어에 StoreProduct가 없는 MasterProduct
    registered_ids = db.session.query(StoreProduct.master_product_id).filter(
        StoreProduct.naver_store_id == store_id,
        StoreProduct.master_product_id.isnot(None),
    ).subquery()

    products = MasterProduct.query.filter(
        MasterProduct.id.notin_(registered_ids),
        MasterProduct.is_prep_ready == True,
    ).order_by(MasterProduct.id.desc()).limit(500).all()

    return jsonify([{
        "id": m.id,
        "product_name": m.edited_name or m.product_name,
        "supply_price": m.supply_price,
        "price": m.price,
        "image_url": m.image_url,
        "supplier_product_code": m.supplier_product_code,
        "wholesaler_name": m.wholesaler.name if m.wholesaler else "",
    } for m in products])


@product_register_bp.route("/bulk-register/start", methods=["POST"])
@login_required
def bulk_register_start():
    data = request.get_json()
    store_id = data.get("store_id")
    master_ids = data.get("master_ids", [])
    preset_id = data.get("preset_id")
    margin_rate = data.get("margin_rate", 30)
    after_service_tel = data.get("after_service_tel", "010-0000-0000")

    if not store_id or not master_ids:
        return jsonify({"error": "store_id, master_ids 필수"}), 400

    job = BulkRegisterJob(
        naver_store_id=store_id,
        status="pending",
        total=len(master_ids),
        completed=0,
        failed=0,
    )
    db.session.add(job)
    db.session.flush()

    for mid in master_ids:
        db.session.add(BulkRegisterItem(job_id=job.id, master_product_id=mid))

    db.session.commit()
    job_id = job.id

    app = current_app._get_current_object()
    t = threading.Thread(
        target=_run_bulk_job,
        args=(job_id, app, preset_id, margin_rate, after_service_tel),
        daemon=True,
    )
    t.start()

    return jsonify({"job_id": job_id})


@product_register_bp.route("/bulk-register/status/<int:job_id>")
@login_required
def bulk_register_status(job_id):
    job = BulkRegisterJob.query.get_or_404(job_id)
    items = BulkRegisterItem.query.filter_by(job_id=job_id).all()
    return jsonify({
        "job_id": job.id,
        "status": job.status,
        "total": job.total,
        "completed": job.completed,
        "failed": job.failed,
        "items": [
            {
                "id": i.id,
                "master_product_id": i.master_product_id,
                "status": i.status,
                "error_msg": i.error_msg,
                "origin_product_no": i.origin_product_no,
            }
            for i in items
        ],
    })


def _run_bulk_job(job_id, app, preset_id, margin_rate, after_service_tel):
    import logging
    _logger = logging.getLogger(__name__)
    from store.naver.products import upload_image_from_url, register_product

    try:
        with app.app_context():
            job = BulkRegisterJob.query.get(job_id)
            if not job:
                return

            store = NaverStore.query.get(job.naver_store_id)
            preset = DeliveryPreset.query.get(preset_id) if preset_id else None

            job.status = "running"
            db.session.commit()

            if preset:
                delivery_info = {
                    "deliveryType": "DELIVERY",
                    "deliveryAttributeType": "NORMAL",
                    "deliveryFeeType": preset.delivery_fee_type,
                    "baseFee": preset.base_fee,
                    "freeConditionalAmount": preset.free_condition_amount if preset.delivery_fee_type == "CONDITIONAL_FREE" else 0,
                    "deliveryFeePayType": preset.delivery_fee_pay_type,
                }
            else:
                delivery_info = {
                    "deliveryType": "DELIVERY",
                    "deliveryAttributeType": "NORMAL",
                    "deliveryFeeType": "FREE",
                    "baseFee": 0,
                    "deliveryFeePayType": "PREPAID",
                }

            items = BulkRegisterItem.query.filter_by(job_id=job_id).all()
            for item in items:
                master = item.master
                try:
                    naver_image_url = upload_image_from_url(
                        master.image_url, store.client_id, store.client_secret
                    )
                    sale_price = round(master.supply_price * (1 + margin_rate / 100))

                    item_category_id = master.category_id or ""
                    item_name = master.edited_name or master.product_name

                    origin_product = {
                        "statusType": "SALE",
                        "saleType": "NEW",
                        "leafCategoryId": str(item_category_id),
                        "name": item_name,
                        "detailContent": item_name,
                        "images": {
                            "representativeImage": {"url": naver_image_url},
                        },
                        "salePrice": sale_price,
                        "stockQuantity": 100,
                        "deliveryInfo": delivery_info,
                        "detailAttribute": {
                            "afterServiceInfo": {
                                "afterServiceTelephoneNumber": after_service_tel,
                                "afterServiceGuideContent": "고객센터로 문의해주세요.",
                            },
                            "originAreaInfo": {
                                "originAreaCode": "0200037",
                                "importer": "",
                            },
                        },
                        "sellerManagementCode": master.supplier_product_code or "",
                    }

                    payload = {
                        "originProduct": origin_product,
                        "smartstoreChannelProduct": {
                            "naverShoppingRegistration": True,
                            "channelProductDisplayStatusType": "ON",
                        },
                    }

                    result = register_product(payload, store.client_id, store.client_secret)
                    origin_no = result.get("originProductNo") or result.get("data", {}).get("originProductNo")

                    sp = StoreProduct(
                        naver_store_id=store.id,
                        master_product_id=master.id,
                        origin_product_no=origin_no,
                        seller_management_code=master.supplier_product_code,
                        product_name=item_name,
                        sale_price=sale_price,
                        store_status="SALE",
                    )
                    db.session.add(sp)

                    item.status = "success"
                    item.origin_product_no = origin_no
                    job.completed += 1

                except Exception as e:
                    item.status = "error"
                    item.error_msg = str(e)[:500]
                    job.failed += 1

                db.session.commit()

            job.status = "done"
            db.session.commit()

    except Exception as e:
        _logger.error(f"[bulk_register] 작업 {job_id} 예상치 못한 오류: {e}")
        try:
            with app.app_context():
                job = BulkRegisterJob.query.get(job_id)
                if job and job.status in ("pending", "running"):
                    job.status = "failed"
                    db.session.commit()
        except Exception:
            pass
