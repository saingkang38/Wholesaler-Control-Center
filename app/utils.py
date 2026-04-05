from datetime import datetime
from zoneinfo import ZoneInfo


def kst_now() -> datetime:
    """한국 시간(KST, UTC+9) 기준 현재 시각 반환 (tzinfo 없는 naive datetime)."""
    return datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
