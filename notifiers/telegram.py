import os
import requests


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 미설정 - 알림 스킵")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[telegram] 전송 실패: {e}")
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
        f"시각: {run_time}",
        f"총 상품 수: {item_count:,}개",
        "",
        "📊 변경사항",
    ]

    if changes is None:
        lines.append("(첫 수집 - 비교 데이터 없음)")
    else:
        labels = {
            "신규": "🆕 신규상품",
            "삭제": "🗑 삭제",
            "재입고": "🔄 재입고",
            "품절단종": "❌ 품절/단종",
            "가격변동": "💰 가격변동",
            "이미지변경": "🖼 이미지변경",
            "상품명변경": "✏️ 상품명변경",
        }
        has_changes = False
        for key, label in labels.items():
            count = changes.get(key, 0)
            if count > 0:
                lines.append(f"{label}: {count:,}개")
                has_changes = True
        if not has_changes:
            lines.append("변경사항 없음")

    send_message("\n".join(lines))


def notify_failure(wholesaler: str, error: str, run_time: str):
    text = (
        f"❌ [{wholesaler}] 수집 실패\n"
        f"시각: {run_time}\n"
        f"오류: {error}"
    )
    send_message(text)
