import os
import logging
import requests

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("[telegram] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 미설정 - 알림 스킵")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.warning(f"[telegram] 전송 실패: {e}")
        return False


def notify_success(wholesaler: str, item_count: int, run_time: str):
    text = (
        f"✅ [{wholesaler}] 수집 완료\n"
        f"시각: {run_time}\n"
        f"총 상품 수: {item_count:,}개"
    )
    send_message(text)


def notify_changes(wholesaler: str, item_count: int, run_time: str, changes: dict | None):
    lines = [
        f"✅ [{wholesaler}] 수집 완료",
        f"🕐 {run_time}",
        f"📦 총 상품: {item_count:,}개",
        "─────────────────",
        "📊 변경사항",
    ]

    if changes is None:
        lines.append("첫 수집 — 비교 데이터 없음")
    else:
        labels = [
            ("신규",     "🆕 신규상품"),
            ("재입고",   "🔄 재입고"),
            ("가격변동", "💰 가격변동"),
            ("상품명변경","✏️ 상품명변경"),
            ("이미지변경","🖼 이미지변경"),
            ("품절단종", "❌ 품절/단종"),
            ("삭제",     "🗑 삭제예정"),
        ]
        for key, label in labels:
            count = changes.get(key, 0)
            lines.append(f"{label}: {count:,}개")

    send_message("\n".join(lines))


def notify_failure(wholesaler: str, error: str, run_time: str):
    text = (
        f"❌ [{wholesaler}] 수집 실패\n"
        f"시각: {run_time}\n"
        f"오류: {error}"
    )
    send_message(text)


def _fmt_elapsed(seconds: int) -> str:
    if seconds is None or seconds < 0:
        return "-"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}시간 {m}분 {s}초"
    if m > 0:
        return f"{m}분 {s}초"
    return f"{s}초"


_STATUS_EMOJI = {"success": "✅", "failed": "❌", "skipped": "⏭", "cancelled": "🛑"}
_STATUS_LABEL = {"success": "성공", "failed": "실패", "skipped": "스킵", "cancelled": "취소"}


def notify_wholesaler_done(
    name: str,
    status: str,
    started_at: str,
    finished_at: str,
    elapsed_seconds: int,
    stats: dict | None = None,
    error: str | None = None,
):
    emoji = _STATUS_EMOJI.get(status, "ℹ️")
    label = _STATUS_LABEL.get(status, status)

    lines = [
        f"{emoji} [{name}] 수집 {label}",
        f"🕐 시작: {started_at}",
        f"🕑 종료: {finished_at}",
        f"⏱ 소요: {_fmt_elapsed(elapsed_seconds)}",
    ]

    if status == "success" and stats:
        total = stats.get("total_items", 0)
        lines.append(f"📦 총 상품: {total:,}개")
        master = stats.get("master_stats") or {}
        if master:
            rows = [
                ("new", "🆕 신규"),
                ("restocked", "🔄 재입고"),
                ("price_change", "💰 가격변동"),
                ("name_change", "✏️ 상품명"),
                ("image_change", "🖼 이미지"),
                ("missing", "❌ 품절/단종"),
                ("discontinued_candidate", "🗑 삭제후보"),
            ]
            lines.append("📊 변경사항")
            for key, lab in rows:
                lines.append(f"  {lab}: {master.get(key, 0):,}")

    if error:
        lines.append(f"⚠️ 오류: {str(error)[:300]}")

    send_message("\n".join(lines))


def notify_chain_final(
    chain_started_at: str,
    chain_finished_at: str,
    total_elapsed_seconds: int,
    slot_results: list,
    store_sync_stats: dict | None = None,
    match_stats: dict | None = None,
):
    lines = [
        "🏁 체인 수집 완료",
        f"🕐 시작: {chain_started_at}",
        f"🕑 종료: {chain_finished_at}",
        f"⏱ 총 소요: {_fmt_elapsed(total_elapsed_seconds)}",
        "─────────────────",
        "📋 슬롯 결과",
    ]
    for r in slot_results or []:
        emoji = _STATUS_EMOJI.get(r.get("status"), "•")
        lines.append(f"{emoji} {r.get('name')}: {_STATUS_LABEL.get(r.get('status'), r.get('status'))} ({r.get('total_items', 0):,}건)")

    if store_sync_stats is not None:
        lines.append("─────────────────")
        if "error" in store_sync_stats:
            lines.append(f"🏪 스마트스토어 재수집 실패: {store_sync_stats['error'][:200]}")
        else:
            lines.append("🏪 스마트스토어 재수집")
            lines.append(f"  신규 {store_sync_stats.get('created', 0):,} / 갱신 {store_sync_stats.get('updated', 0):,}")
            lines.append(f"  매칭 {store_sync_stats.get('matched', 0):,} / 미매칭 {store_sync_stats.get('unmatched', 0):,} / 마감 {store_sync_stats.get('closed', 0):,}")

    if match_stats:
        lines.append("─────────────────")
        lines.append("🎯 액션 시그널")
        for k, v in match_stats.items():
            if v:
                lines.append(f"  {k}: {v:,}")

    send_message("\n".join(lines))
