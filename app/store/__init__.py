import logging
import threading
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from app.utils import kst_now
from app.infrastructure import db
from app.store.models import StoreProduct, NaverStore, SyncLog, StoreOptionMismatch
from app.master.models import MasterProduct
from app.wholesalers.models import Wholesaler

store_bp = Blueprint("store", __name__)
logger = logging.getLogger(__name__)

# ── 실시간 진행 상황 추적 ─────────────────────────────────
_sync_progress: dict = {}   # store_id → {logs, percent, done, error}
_sync_lock = threading.Lock()

# ── 옵션 불일치 감지 백그라운드 실행 상태 ──────────────────
_detect_mismatch_status = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "current_store": "",
    "store_progress": "",      # "2/3" 형식
    "items_processed": 0,
    "items_total": 0,
    "created": 0,
    "updated": 0,
    "error": None,
}
_detect_mismatch_lock = threading.Lock()


def _push_log(store_id: int, msg: str, percent: int = None):
    with _sync_lock:
        p = _sync_progress.setdefault(store_id, {"logs": [], "percent": 0, "done": False, "error": None})
        p["logs"].append(msg)
        if percent is not None:
            p["percent"] = percent


def get_sync_progress(store_id: int) -> dict | None:
    with _sync_lock:
        p = _sync_progress.get(store_id)
        return dict(p, logs=list(p["logs"])) if p else None


def start_sync_background(store_id: int, flask_app) -> bool:
    """백그라운드 스레드로 동기화 실행. 이미 진행 중이면 False 반환."""
    with _sync_lock:
        existing = _sync_progress.get(store_id, {})
        if existing and not existing.get("done") and existing.get("logs"):
            return False
        _sync_progress[store_id] = {"logs": [], "percent": 0, "done": False, "error": None}

    def _run():
        try:
            with flask_app.app_context():
                store = NaverStore.query.get(store_id)
                if not store:
                    _push_log(store_id, "스토어를 찾을 수 없습니다.", 100)
                    return

                def log_cb(msg, pct=None):
                    logger.info(f"[store] {msg}")
                    _push_log(store_id, msg, pct)

                stats = _sync_single_store(store, log_cb=log_cb)

                log_cb("액션 시그널 감지 중...", 88)
                from app.actions import detect_action_signals
                wholesaler_ids = db.session.query(MasterProduct.wholesaler_id.distinct())\
                    .join(StoreProduct, StoreProduct.master_product_id == MasterProduct.id)\
                    .filter(StoreProduct.naver_store_id == store_id).all()
                for (wid,) in wholesaler_ids:
                    detect_action_signals(wid)

                summary = (f"완료 — 신규 {stats['created']}건 / 갱신 {stats['updated']}건 / "
                           f"매칭 {stats['matched']}건 / 미매칭 {stats['unmatched']}건")
                log_cb(summary, 100)

                SyncLog.query.filter_by(naver_store_id=store_id, action="FULL_SYNC").delete()
                db.session.add(SyncLog(
                    naver_store_id=store_id,
                    action="FULL_SYNC",
                    result="success",
                    detail=f"신규 {stats['created']} / 갱신 {stats['updated']} / "
                           f"매칭 {stats['matched']} / 미매칭 {stats['unmatched']}",
                ))
                db.session.commit()

        except Exception as e:
            logger.error(f"[store] 동기화 오류: {e}")
            _push_log(store_id, f"오류 발생: {e}", 100)
            with _sync_lock:
                _sync_progress[store_id]["error"] = str(e)
                SyncLog.query.filter_by(naver_store_id=store_id, action="FULL_SYNC").delete()
                db.session.add(SyncLog(naver_store_id=store_id, action="FULL_SYNC", result="error", detail=str(e)))
                db.session.commit()
        finally:
            with _sync_lock:
                _sync_progress[store_id]["done"] = True

    threading.Thread(target=_run, daemon=True).start()
    return True


def _get_prefixes() -> list:
    """도매처 prefix 목록 — 여러 함수에서 공통 사용"""
    return [w.prefix for w in Wholesaler.query.filter(Wholesaler.prefix.isnot(None)).all()]


def _build_master_map(seller_codes: set, prefixes: list) -> dict:
    """seller_code → MasterProduct 매핑 딕셔너리 — 배치 IN 쿼리로 N+1 + SQLite 변수 제한 방지"""
    if not seller_codes:
        return {}
    all_candidates = set(seller_codes)
    for code in seller_codes:
        for p in prefixes:
            if p:
                all_candidates.add(f"{p}{code}")
    candidates_list = list(all_candidates)
    masters = []
    for i in range(0, len(candidates_list), 500):
        chunk = candidates_list[i:i + 500]
        masters.extend(
            MasterProduct.query.filter(
                MasterProduct.supplier_product_code.in_(chunk)
            ).all()
        )
    return {m.supplier_product_code: m for m in masters}


def _lookup_master(seller_code: str, master_map: dict, prefixes: list):
    """master_map에서 seller_code 또는 prefix+code 로 마스터 조회"""
    if master_map.get(seller_code):
        return master_map[seller_code]
    for p in prefixes:
        if p and master_map.get(f"{p}{seller_code}"):
            return master_map[f"{p}{seller_code}"]
    return None


def sync_store_products() -> dict:
    """모든 활성 NaverStore를 순회하며 상품 동기화"""
    stores = NaverStore.query.filter_by(is_active=True).all()
    total_stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0, "closed": 0}

    for naver_store in stores:
        stats = _sync_single_store(naver_store)
        for k in total_stats:
            total_stats[k] += stats[k]

    logger.info(f"[store] 전체 동기화 완료: {total_stats}")
    return total_stats


def _sync_single_store(naver_store: NaverStore, log_cb=None) -> dict:
    def log(msg, pct=None):
        if log_cb:
            log_cb(msg, pct)

    from store.naver import get_all_products

    sync_start = kst_now()  # 동기화 시작 시각 — API 미반환 상품 감지용
    log("네이버 API 토큰 발급 중...", 2)

    def on_page(page, total_pages, count):
        pct = int(5 + (page / max(total_pages, 1)) * 42)
        log(f"상품 목록 수신 중... {page}/{total_pages} 페이지 ({count}건 누적)", pct)

    raw_items = get_all_products(
        client_id=naver_store.client_id,
        client_secret=naver_store.client_secret,
        on_page=on_page,
    )
    log(f"API 수신 완료 — 총 {len(raw_items)}건, DB 저장 중...", 50)
    stats = {"created": 0, "updated": 0, "matched": 0, "unmatched": 0}

    prefixes = _get_prefixes()

    # 배치 준비: 전체 원본번호 / seller_code 수집
    parsed = []
    for item in raw_items:
        origin_no = item.get("originProductNo")
        if origin_no is None or origin_no == "":
            continue
        channel_products = item.get("channelProducts") or []
        channel = channel_products[0] if channel_products else None
        parsed.append({
            "origin_no": origin_no,
            "channel_no": channel.get("channelProductNo") if channel else None,
            "seller_code": (channel.get("sellerManagementCode") or "").strip() if channel else "",
            "name": channel.get("name", "") if channel else "",
            "status": channel.get("statusType", "") if channel else "",
            "price": channel.get("salePrice") if channel else None,
        })

    # 기존 StoreProduct 배치 로드 (SQLite 변수 제한 방지)
    all_origin_nos = [p["origin_no"] for p in parsed]
    existing_list = []
    for i in range(0, len(all_origin_nos), 500):
        chunk = all_origin_nos[i:i + 500]
        existing_list.extend(
            StoreProduct.query.filter(
                StoreProduct.naver_store_id == naver_store.id,
                StoreProduct.origin_product_no.in_(chunk),
            ).all()
        )
    existing_stores = {s.origin_product_no: s for s in existing_list}

    # MasterProduct 한 번에 로드 (N+1 방지)
    all_seller_codes = {p["seller_code"] for p in parsed if p["seller_code"]}
    master_map = _build_master_map(all_seller_codes, prefixes)

    for p in parsed:
        origin_no = p["origin_no"]
        seller_code = p["seller_code"]

        store = existing_stores.get(origin_no)
        if store:
            old_status = store.store_status
            store.store_status = p["status"]
            store.sale_price = p["price"]
            store.product_name = p["name"]
            store.seller_management_code = seller_code
            store.last_synced_at = kst_now()
            stats["updated"] += 1

            # SALE → 비판매(CLOSE/SUSPENSION 등) 전환 시 pending 시그널 즉시 취소
            if old_status == "SALE" and store.store_status != "SALE":
                from app.actions.models import ActionSignal
                ActionSignal.query.filter_by(
                    store_product_id=store.id,
                    status="pending",
                ).delete(synchronize_session=False)
        else:
            store = StoreProduct(
                naver_store_id=naver_store.id,
                origin_product_no=origin_no,
                channel_product_no=p["channel_no"],
                seller_management_code=seller_code,
                product_name=p["name"],
                store_status=p["status"],
                sale_price=p["price"],
                last_synced_at=kst_now(),
            )
            db.session.add(store)
            db.session.flush()
            stats["created"] += 1

        if seller_code:
            master = _lookup_master(seller_code, master_map, prefixes)
            if master:
                store.master_product_id = master.id
                stats["matched"] += 1
            else:
                stats["unmatched"] += 1

    # last_synced_at < sync_start 인 상품 = 이번 동기화에서 API 미반환 = 삭제된 상품
    gone = StoreProduct.query.filter(
        StoreProduct.naver_store_id == naver_store.id,
        StoreProduct.last_synced_at < sync_start,
        StoreProduct.store_status.notin_(["CLOSE", "DELETE"]),
    ).all()
    for store in gone:
        logger.info(
            f"[store] API 미반환 상품 CLOSE 처리: "
            f"origin_no={store.origin_product_no}, "
            f"code={store.seller_management_code}"
        )
        store.store_status = "CLOSE"
        store.last_synced_at = sync_start
        from app.actions.models import ActionSignal
        ActionSignal.query.filter_by(
            store_product_id=store.id,
            status="pending",
        ).delete(synchronize_session=False)
    stats["closed"] = len(gone)

    db.session.commit()
    log(f"매칭 완료 — 매칭 {stats['matched']}건 / 미매칭 {stats['unmatched']}건", 82)
    logger.info(f"[store] {naver_store.store_name} 동기화: {stats}")
    return stats


def _rematch_by_codes(naver_store_id: int, codes: list) -> dict:
    """네이버 API 호출 없이 입력한 판매자관리코드만 재매칭"""
    prefixes = _get_prefixes()
    clean_codes = {c.strip() for c in codes if c.strip()}
    master_map = _build_master_map(clean_codes, prefixes)
    stats = {"matched": 0, "unmatched": 0, "not_found": 0}

    try:
        for code in clean_codes:
            store = StoreProduct.query.filter_by(
                naver_store_id=naver_store_id,
                seller_management_code=code,
            ).first()
            if not store:
                stats["not_found"] += 1
                continue
            master = _lookup_master(code, master_map, prefixes)
            if master:
                store.master_product_id = master.id
                stats["matched"] += 1
            else:
                stats["unmatched"] += 1

        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return stats


def propose_code_matches(naver_store_id: int, wholesaler_id: int = None, limit: int = 500) -> list:
    """미매칭 StoreProduct 중 seller_management_code가 MasterProduct와 exact match(prefix 포함/미포함)되는 상품 제안"""
    unmatched = StoreProduct.query.filter(
        StoreProduct.naver_store_id == naver_store_id,
        StoreProduct.master_product_id.is_(None),
        StoreProduct.seller_management_code.isnot(None),
        StoreProduct.seller_management_code != "",
    ).limit(limit).all()

    if not unmatched:
        return []

    prefixes = _get_prefixes()
    all_codes = {sp.seller_management_code.strip() for sp in unmatched if sp.seller_management_code}
    master_map = _build_master_map(all_codes, prefixes)

    # wholesaler_id 필터가 있으면 map에서 해당 도매처 상품만 남김
    if wholesaler_id:
        master_map = {k: v for k, v in master_map.items() if v.wholesaler_id == wholesaler_id}

    results = []
    for sp in unmatched:
        code = sp.seller_management_code.strip()
        master = _lookup_master(code, master_map, prefixes)

        if master:
            results.append({
                "store_product_id": sp.id,
                "origin_product_no": sp.origin_product_no,
                "store_name": sp.product_name,
                "current_code": code,
                "candidate_master_id": master.id,
                "candidate_code": master.supplier_product_code,
                "candidate_name": master.product_name,
            })

    return results


def push_seller_management_codes(naver_store: object, pairs: list) -> dict:
    """pairs: [{origin_product_no, supplier_product_code}, ...]
    Naver API로 sellerManagementCode 업데이트 후 로컬 DB에도 반영, 재매칭 실행
    """
    from store.naver import update_seller_management_code

    success_count = 0
    fail_count = 0
    success_codes = []  # API 호출 성공한 코드만 추적

    for pair in pairs:
        origin_no = pair.get("origin_product_no")
        code = pair.get("supplier_product_code", "").strip()
        if not origin_no or not code:
            fail_count += 1
            continue
        try:
            update_seller_management_code(
                origin_product_no=origin_no,
                seller_management_code=code,
                client_id=naver_store.client_id,
                client_secret=naver_store.client_secret,
            )
            sp = StoreProduct.query.filter_by(
                naver_store_id=naver_store.id,
                origin_product_no=origin_no,
            ).first()
            if sp:
                sp.seller_management_code = code
            success_codes.append(code)
            success_count += 1
        except Exception as e:
            logger.warning(f"[push_codes] origin={origin_no} 실패: {e}")
            fail_count += 1

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"[push_codes] 커밋 실패: {e}")
        raise

    # API 호출 성공한 코드만 재매칭
    if success_codes:
        _rematch_by_codes(naver_store.id, success_codes)

    return {"success_count": success_count, "fail_count": fail_count}


# ── 스토어 옵션 상태 동기화 ───────────────────────────────
# 목적: 스마트스토어 실제 옵션 추가금을 읽어 applied_option_diffs/base_price 초기화
# 효과: 이미 올바르게 적용된 상품의 OPTION_ADD 중복 신호 방지

def sync_store_option_state(flask_app=None) -> dict:
    """
    모든 활성 스토어의 옵션 상품에 대해 Naver API로 현재 옵션 추가금을 읽어
    master 데이터와 일치하면 applied_option_diffs / applied_option_base_price 저장.
    일치하지 않는 상품은 건드리지 않음 (→ OPTION_ADD 신호가 정상 생성됨).
    """
    import json
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from app.settings import calculate_option_pricing
    from store.naver.products import get_origin_product
    from store.naver import _get_access_token

    ctx = flask_app.app_context() if flask_app else None
    if ctx:
        ctx.push()

    try:
        stores = NaverStore.query.filter_by(is_active=True).all()
        total_checked = 0
        total_matched = 0
        total_stores = len(stores)

        for store_idx, naver_store in enumerate(stores, 1):
            logger.info(
                f"[option_sync] 스토어 {store_idx}/{total_stores}: {naver_store.store_name} 시작"
            )
            try:
                token = _get_access_token(naver_store.client_id, naver_store.client_secret)
            except Exception as e:
                logger.error(f"[option_sync] 토큰 발급 실패 ({naver_store.store_name}): {e}")
                continue

            # 옵션 있는 매칭 상품만 대상
            targets = (
                db.session.query(StoreProduct)
                .join(MasterProduct, StoreProduct.master_product_id == MasterProduct.id)
                .filter(
                    StoreProduct.naver_store_id == naver_store.id,
                    StoreProduct.origin_product_no != None,
                    MasterProduct.option_diffs != None,
                    MasterProduct.option_diffs != "",
                    MasterProduct.options_text != None,
                    MasterProduct.options_text != "",
                    MasterProduct.price != None,
                )
                .all()
            )
            logger.info(f"[option_sync] 대상 {len(targets)}건")

            checked = 0
            matched = 0
            commit_batch = []

            def _fetch_additions(origin_no: int, master_price: int, master_diffs: str):
                """API 호출 → (is_match, additions_str) 반환 — SQLAlchemy 객체 접근 없음"""
                try:
                    data = get_origin_product(origin_no, naver_store.client_id, naver_store.client_secret)
                    combos = (
                        data.get("originProduct", {})
                        .get("detailAttribute", {})
                        .get("optionInfo", {})
                        .get("optionCombinations", [])
                    )
                    if not combos:
                        return False, None
                    store_additions = [c.get("price", 0) for c in combos]
                    additions_str = "\n".join(str(a) for a in store_additions)
                    try:
                        expected = calculate_option_pricing(master_price, master_diffs)["additions"]
                        is_match = (len(store_additions) == len(expected) and store_additions == expected)
                    except Exception:
                        is_match = False
                    return is_match, additions_str
                except Exception as e:
                    logger.debug(f"[option_sync] origin={origin_no} 오류: {e}")
                    return False, None

            # primitive 값만 추출 후 스레드에 전달 (SQLAlchemy 세션 스레드 안전성 문제 방지)
            task_data = [
                (sp, sp.origin_product_no, sp.master.price, sp.master.option_diffs)
                for sp in targets
                if sp.master
            ]

            with ThreadPoolExecutor(max_workers=4) as executor:
                future_map = {
                    executor.submit(_fetch_additions, origin_no, m_price, m_diffs): sp
                    for sp, origin_no, m_price, m_diffs in task_data
                }
                for future in as_completed(future_map):
                    sp = future_map[future]
                    checked += 1
                    try:
                        is_match, additions_str = future.result()
                    except Exception:
                        is_match, additions_str = False, None

                    if additions_str is not None:
                        sp.naver_cached_additions = additions_str

                    if is_match:
                        sp.applied_options_text = sp.master.options_text
                        sp.applied_option_diffs = sp.master.option_diffs
                        sp.applied_option_base_price = sp.master.price
                        matched += 1
                    commit_batch.append(sp)

                    if checked % 100 == 0:
                        # 중간 커밋
                        try:
                            db.session.commit()
                        except Exception as ce:
                            db.session.rollback()
                            logger.error(f"[option_sync] 중간 커밋 실패: {ce}")
                        logger.info(
                            f"[option_sync] {naver_store.store_name}: "
                            f"{checked}/{len(targets)} 처리, {matched}건 일치"
                        )

            try:
                db.session.commit()
            except Exception as ce:
                db.session.rollback()
                logger.error(f"[option_sync] 최종 커밋 실패: {ce}")

            logger.info(
                f"[option_sync] {naver_store.store_name} 완료: "
                f"총 {checked}건 확인, {matched}건 적용 상태 기록"
            )
            total_checked += checked
            total_matched += matched

        logger.info(
            f"[option_sync] 전체 완료: {total_checked}건 확인, {total_matched}건 기록"
        )
        return {"checked": total_checked, "matched": total_matched}

    finally:
        if ctx:
            ctx.pop()


# ── 도매처 단품 ↔ Naver 옵션 불일치 감지 ────────────────────

def detect_option_mismatches(flask_app=None, progress_cb=None) -> dict:
    """
    master.options_text = NULL 이지만 Naver 스토어에 optionCombinations 가 있는 상품을 탐지.
    StoreOptionMismatch 레코드를 생성/갱신한다 (pending만 갱신, resolved/ignored는 건드리지 않음).

    progress_cb(stage, **kwargs): 진행 상황 콜백 (선택)
      - stage='store_start': store_idx, total_stores, store_name, items_total
      - stage='item_progress': items_processed, created, updated
      - stage='store_end': store_name, items_processed
      - stage='all_done': total_created, total_updated
    """
    import json
    from store.naver.products import get_origin_product

    ctx = flask_app.app_context() if flask_app else None
    if ctx:
        ctx.push()

    try:
        stores = NaverStore.query.filter_by(is_active=True).all()
        created = 0
        updated = 0

        for store_idx, naver_store in enumerate(stores, 1):
            # 도매처 단품(options_text NULL) + 매칭된 스토어 상품만 대상
            targets = (
                db.session.query(StoreProduct)
                .join(MasterProduct, StoreProduct.master_product_id == MasterProduct.id)
                .filter(
                    StoreProduct.naver_store_id == naver_store.id,
                    StoreProduct.origin_product_no != None,
                    db.or_(MasterProduct.options_text == None, MasterProduct.options_text == ""),
                    MasterProduct.price != None,
                )
                .all()
            )

            if progress_cb:
                progress_cb(
                    stage="store_start",
                    store_idx=store_idx,
                    total_stores=len(stores),
                    store_name=naver_store.store_name,
                    items_total=len(targets),
                )

            processed_in_store = 0
            for sp in targets:
                try:
                    data = get_origin_product(
                        sp.origin_product_no,
                        naver_store.client_id,
                        naver_store.client_secret,
                    )
                    combos = (
                        data.get("originProduct", {})
                        .get("detailAttribute", {})
                        .get("optionInfo", {})
                        .get("optionCombinations", [])
                    )
                    if not combos:
                        # Naver에도 옵션 없음 → 기존 불일치 해소
                        if sp.option_mismatch and sp.option_mismatch.status == "pending":
                            sp.option_mismatch.status = "resolved"
                            sp.option_mismatch.resolved_at = kst_now()
                        continue

                    combo_data = json.dumps(
                        [{"name": c.get("optionName1") or c.get("optionName2") or "", "price": c.get("price", 0)}
                         for c in combos],
                        ensure_ascii=False,
                    )

                    existing = sp.option_mismatch
                    if existing:
                        if existing.status == "pending":
                            existing.naver_combos = combo_data
                            existing.updated_at = kst_now()
                            updated += 1
                        # resolved/ignored는 건드리지 않음
                    else:
                        db.session.add(StoreOptionMismatch(
                            store_product_id=sp.id,
                            naver_combos=combo_data,
                            status="pending",
                        ))
                        created += 1
                except Exception as e:
                    logger.debug(f"[mismatch] origin={sp.origin_product_no} 오류: {e}")

                processed_in_store += 1

                # 50건마다 중간 commit + 진행 보고
                if processed_in_store % 50 == 0:
                    try:
                        db.session.commit()
                    except Exception as ce:
                        db.session.rollback()
                        logger.error(f"[mismatch] 중간 커밋 실패: {ce}")
                    if progress_cb:
                        progress_cb(
                            stage="item_progress",
                            items_processed=processed_in_store,
                            created=created,
                            updated=updated,
                        )

            try:
                db.session.commit()
            except Exception as ce:
                db.session.rollback()
                logger.error(f"[mismatch] 커밋 실패: {ce}")

            if progress_cb:
                progress_cb(
                    stage="store_end",
                    store_name=naver_store.store_name,
                    items_processed=processed_in_store,
                )

        if progress_cb:
            progress_cb(stage="all_done", total_created=created, total_updated=updated)

        logger.info(f"[mismatch] 감지 완료: 신규 {created}건, 갱신 {updated}건")
        return {"created": created, "updated": updated}

    finally:
        if ctx:
            ctx.pop()


# ── 옵션 불일치 관리 라우트 ─────────────────────────────────

@store_bp.route("/option-mismatch")
@login_required
def option_mismatch_page():
    import json as _json
    status_filter = request.args.get("status", "pending")
    store_filter = request.args.get("store_id", type=int)

    q = (
        db.session.query(StoreOptionMismatch)
        .join(StoreProduct, StoreOptionMismatch.store_product_id == StoreProduct.id)
        .join(MasterProduct, StoreProduct.master_product_id == MasterProduct.id)
    )
    if status_filter != "all":
        q = q.filter(StoreOptionMismatch.status == status_filter)
    if store_filter:
        q = q.filter(StoreProduct.naver_store_id == store_filter)

    mismatches = q.order_by(StoreOptionMismatch.created_at.desc()).all()

    rows = []
    for m in mismatches:
        sp = m.store_product
        master = sp.master if sp else None
        combos = _json.loads(m.naver_combos) if m.naver_combos else []
        rows.append({
            "id": m.id,
            "store_product_id": sp.id if sp else None,
            "status": m.status,
            "product_name": master.product_name if master else "-",
            "seller_code": sp.seller_management_code if sp else "-",
            "origin_product_no": sp.origin_product_no if sp else None,
            "store_name": sp.naver_store.store_name if sp and sp.naver_store else "-",
            "wholesaler_name": master.wholesaler.name if master and master.wholesaler else "-",
            "wholesale_price": master.price if master else None,
            "combos": combos,
            "created_at": m.created_at.strftime("%Y-%m-%d %H:%M") if m.created_at else "-",
            "resolved_at": m.resolved_at.strftime("%Y-%m-%d %H:%M") if m.resolved_at else None,
        })

    pending_count = StoreOptionMismatch.query.filter_by(status="pending").count()
    naver_stores = NaverStore.query.filter_by(is_active=True).order_by(NaverStore.store_name).all()
    with _detect_mismatch_lock:
        detect_status = dict(_detect_mismatch_status)
    return render_template(
        "option_mismatch.html",
        rows=rows,
        status_filter=status_filter,
        store_filter=store_filter,
        pending_count=pending_count,
        naver_stores=naver_stores,
        detect_status=detect_status,
    )


@store_bp.route("/option-mismatch/<int:mismatch_id>/preview")
@login_required
def preview_option_removal(mismatch_id):
    """옵션 제거 미리보기 — 네이버 현재 옵션 정보와 변경 후 모습을 반환 (API 변경 없음)."""
    from store.naver.products import get_origin_product
    from app.settings import apply_margin

    m = StoreOptionMismatch.query.get_or_404(mismatch_id)
    sp = m.store_product
    master = sp.master if sp else None

    if not sp or not master or not sp.origin_product_no:
        return {"ok": False, "error": "상품 정보 없음"}, 400
    if not sp.naver_store:
        return {"ok": False, "error": "스토어 정보 없음"}, 400

    try:
        client_id = sp.naver_store.client_id
        client_secret = sp.naver_store.client_secret

        product_data = get_origin_product(sp.origin_product_no, client_id, client_secret)
        origin = product_data.get("originProduct", {})
        option_info = origin.get("detailAttribute", {}).get("optionInfo", {})
        combinations = option_info.get("optionCombinations", [])

        current_combos = [
            {
                "name": (c.get("optionName1") or c.get("optionName2") or "").strip() or "(이름없음)",
                "price": c.get("price", 0),
                "stock": c.get("stockQuantity"),
                "usable": c.get("usable", True),
            }
            for c in combinations
        ]

        new_price = apply_margin(master.price)

        return {
            "ok": True,
            "product_name": master.product_name or "-",
            "store_name": sp.naver_store.store_name,
            "wholesaler_name": master.wholesaler.name if master.wholesaler else "-",
            "current_sale_price": origin.get("salePrice"),
            "current_combinations": current_combos,
            "combo_count": len(current_combos),
            "wholesale_price": master.price,
            "new_sale_price": new_price,
        }

    except Exception as e:
        logger.error(f"[mismatch] 미리보기 실패: {e}")
        return {"ok": False, "error": str(e)}, 500


@store_bp.route("/option-mismatch/<int:mismatch_id>/resolve", methods=["POST"])
@login_required
def resolve_option_mismatch(mismatch_id):
    """옵션 자체를 제거하고 도매가 기준 가격을 적용 → resolved.
    도매처가 단품인데 스마트스토어에 옵션이 있는 케이스를 도매처 기준으로 원복."""
    from store.naver.products import get_origin_product, update_origin_product
    from store.naver import update_price as _naver_update_price
    from app.settings import apply_margin

    m = StoreOptionMismatch.query.get_or_404(mismatch_id)
    sp = m.store_product
    master = sp.master if sp else None

    if not sp or not master or not sp.origin_product_no:
        return {"ok": False, "error": "상품 정보 없음"}, 400
    if not sp.naver_store:
        return {"ok": False, "error": "스토어 정보 없음"}, 400

    try:
        client_id = sp.naver_store.client_id
        client_secret = sp.naver_store.client_secret

        product_data = get_origin_product(sp.origin_product_no, client_id, client_secret)
        origin = product_data.get("originProduct", {})
        detail = origin.setdefault("detailAttribute", {})
        option_info = detail.get("optionInfo", {})
        combinations = option_info.get("optionCombinations", [])

        new_price = apply_margin(master.price)

        if combinations:
            # Step 1: 옵션 제거 — optionCombinations 비우고 그룹명도 정리
            detail["optionInfo"] = {"optionCombinations": []}
            payload = {
                "originProduct": origin,
                "smartstoreChannelProduct": product_data.get("smartstoreChannelProduct", {}),
            }
            update_origin_product(sp.origin_product_no, payload, client_id, client_secret)
            logger.info(
                f"[mismatch] Step1: 옵션 제거 (store_product_id={sp.id}, "
                f"removed_count={len(combinations)})"
            )

        # Step 2: 도매가 기준 새 salePrice 적용 (옵션 비웠으니 안전)
        _naver_update_price(sp.origin_product_no, new_price, client_id=client_id, client_secret=client_secret)
        logger.info(f"[mismatch] Step2: 가격 반영 (store_product_id={sp.id}, price={new_price})")

        # 단품 상태로 store_products 캐시 정리
        sp.sale_price = new_price
        sp.option_list_price = new_price
        sp.option_discount_amount = None
        sp.applied_option_diffs = None
        sp.applied_options_text = None
        sp.applied_option_base_price = master.price

        m.status = "resolved"
        m.resolved_at = kst_now()
        db.session.commit()

        logger.info(
            f"[mismatch] 옵션 제거 완료: store_product_id={sp.id}, "
            f"removed_count={len(combinations)}, price={new_price}"
        )
        return {"ok": True, "removed_count": len(combinations), "new_price": new_price}

    except Exception as e:
        db.session.rollback()
        logger.error(f"[mismatch] 옵션 제거 실패: {e}")
        return {"ok": False, "error": str(e)}, 500


@store_bp.route("/option-mismatch/<int:mismatch_id>/ignore", methods=["POST"])
@login_required
def ignore_option_mismatch(mismatch_id):
    """이 불일치를 무시 처리 (이후 동기화에서도 다시 생성되지 않음)"""
    m = StoreOptionMismatch.query.get_or_404(mismatch_id)
    m.status = "ignored"
    m.resolved_at = kst_now()
    db.session.commit()
    return {"ok": True}


@store_bp.route("/option-mismatch/detect", methods=["POST"])
@login_required
def trigger_detect_mismatch():
    """수동 감지 실행 — 백그라운드 스레드로 시작."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from flask import current_app

    _kst = ZoneInfo("Asia/Seoul")

    with _detect_mismatch_lock:
        if _detect_mismatch_status["running"]:
            return {"ok": False, "started": False, "reason": "이미 감지가 진행 중입니다."}, 409
        _detect_mismatch_status.update({
            "running": True,
            "started_at": datetime.now(_kst).strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": None,
            "current_store": "",
            "store_progress": "",
            "items_processed": 0,
            "items_total": 0,
            "created": 0,
            "updated": 0,
            "error": None,
        })

    flask_app = current_app._get_current_object()

    def _cb(stage, **kwargs):
        with _detect_mismatch_lock:
            if stage == "store_start":
                _detect_mismatch_status.update({
                    "current_store": kwargs.get("store_name", ""),
                    "store_progress": f"{kwargs.get('store_idx')}/{kwargs.get('total_stores')}",
                    "items_processed": 0,
                    "items_total": kwargs.get("items_total", 0),
                })
            elif stage == "item_progress":
                _detect_mismatch_status.update({
                    "items_processed": kwargs.get("items_processed", 0),
                    "created": kwargs.get("created", 0),
                    "updated": kwargs.get("updated", 0),
                })
            elif stage == "store_end":
                _detect_mismatch_status["items_processed"] = kwargs.get("items_processed", 0)
            elif stage == "all_done":
                _detect_mismatch_status["created"] = kwargs.get("total_created", 0)
                _detect_mismatch_status["updated"] = kwargs.get("total_updated", 0)

    def _runner():
        try:
            detect_option_mismatches(flask_app=flask_app, progress_cb=_cb)
        except Exception as e:
            logger.exception("[mismatch] 백그라운드 감지 실패")
            with _detect_mismatch_lock:
                _detect_mismatch_status["error"] = str(e)
        finally:
            with _detect_mismatch_lock:
                _detect_mismatch_status["running"] = False
                _detect_mismatch_status["finished_at"] = datetime.now(_kst).strftime("%Y-%m-%d %H:%M:%S")

    threading.Thread(target=_runner, daemon=True).start()
    return {"ok": True, "started": True}, 202


@store_bp.route("/option-mismatch/detect/status")
@login_required
def detect_mismatch_status():
    """현재 감지 진행 상태 조회"""
    with _detect_mismatch_lock:
        return dict(_detect_mismatch_status)


# ── 관리 페이지 ──────────────────────────────────────────

@store_bp.route("/stores")
@login_required
def stores_page():
    stores = NaverStore.query.order_by(NaverStore.created_at).all()
    return render_template("stores.html", stores=stores)


@store_bp.route("/stores/add", methods=["POST"])
@login_required
def add_store():
    store_name = request.form.get("store_name", "").strip()
    client_id = request.form.get("client_id", "").strip()
    client_secret = request.form.get("client_secret", "").strip()

    if not store_name or not client_id or not client_secret:
        flash("모든 항목을 입력해주세요.", "error")
        return redirect(url_for("store.stores_page"))

    db.session.add(NaverStore(
        store_name=store_name,
        client_id=client_id,
        client_secret=client_secret,
    ))
    db.session.commit()
    flash(f"'{store_name}' 스토어가 추가됐습니다.", "success")
    return redirect(url_for("store.stores_page"))


@store_bp.route("/stores/<int:store_id>/toggle", methods=["POST"])
@login_required
def toggle_store(store_id):
    store = NaverStore.query.get_or_404(store_id)
    store.is_active = not store.is_active
    db.session.commit()
    return redirect(url_for("store.stores_page"))


@store_bp.route("/stores/<int:store_id>/delete", methods=["POST"])
@login_required
def delete_store(store_id):
    store = NaverStore.query.get_or_404(store_id)
    db.session.delete(store)
    db.session.commit()
    flash(f"'{store.store_name}' 스토어가 삭제됐습니다.", "success")
    return redirect(url_for("store.stores_page"))


@store_bp.route("/stores/<int:store_id>/edit", methods=["POST"])
@login_required
def edit_store(store_id):
    store = NaverStore.query.get_or_404(store_id)
    store_name = request.form.get("store_name", "").strip()
    client_id = request.form.get("client_id", "").strip()
    client_secret = request.form.get("client_secret", "").strip()

    if not store_name or not client_id:
        flash("스토어 이름과 애플리케이션 ID는 필수입니다.", "error")
        return redirect(url_for("store.stores_page"))

    store.store_name = store_name
    store.client_id = client_id
    if client_secret:
        store.client_secret = client_secret
    db.session.commit()
    flash(f"'{store_name}' 스토어가 수정됐습니다.", "success")
    return redirect(url_for("store.stores_page"))
