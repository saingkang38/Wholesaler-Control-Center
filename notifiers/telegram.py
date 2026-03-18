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
        f"수집 상품 수: {item_count:,}개"
    )
    send_message(text)


def notify_failure(wholesaler: str, error: str, run_time: str):
    text = (
        f"❌ [{wholesaler}] 수집 실패\n"
        f"시각: {run_time}\n"
        f"오류: {error}"
    )
    send_message(text)
