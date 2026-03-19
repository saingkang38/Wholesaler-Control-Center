import json
from pathlib import Path

SNAPSHOT_FILENAME = "ownerclan_snapshot.json"
OUT_STATUSES = ("out_of_stock", "discontinued")


def load_snapshot(downloads_dir: str) -> dict:
    path = Path(downloads_dir) / SNAPSHOT_FILENAME
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_snapshot(items: list, downloads_dir: str):
    snapshot = {}
    for item in items:
        code = item.get("source_product_code")
        if code:
            snapshot[code] = {
                "price": item.get("price"),
                "status": item.get("status"),
                "image_url": item.get("image_url"),
                "product_name": item.get("product_name"),
            }
    path = Path(downloads_dir) / SNAPSHOT_FILENAME
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
    print(f"[comparator] 스냅샷 저장 완료: {len(snapshot)}개")


def compare(old_snapshot: dict, new_items: list) -> dict | None:
    if not old_snapshot:
        print("[comparator] 이전 스냅샷 없음 - 비교 생략")
        return None

    new_map = {
        item["source_product_code"]: item
        for item in new_items
        if item.get("source_product_code")
    }

    old_codes = set(old_snapshot.keys())
    new_codes = set(new_map.keys())

    신규 = new_codes - old_codes
    삭제 = old_codes - new_codes
    공통 = old_codes & new_codes

    재입고 = []
    품절단종 = []
    가격변동 = []
    이미지변경 = []
    상품명변경 = []

    for code in 공통:
        old = old_snapshot[code]
        new = new_map[code]

        old_status = old.get("status", "active")
        new_status = new.get("status", "active")

        if old_status in OUT_STATUSES and new_status == "active":
            재입고.append(code)
        elif old_status == "active" and new_status in OUT_STATUSES:
            품절단종.append(code)

        if old.get("price") != new.get("price"):
            가격변동.append(code)

        old_img = old.get("image_url")
        new_img = new.get("image_url")
        if old_img and new_img and old_img != new_img:
            이미지변경.append(code)

        old_name = old.get("product_name")
        new_name = new.get("product_name")
        if old_name and new_name and old_name != new_name:
            상품명변경.append(code)

    result = {
        "신규": len(신규),
        "삭제": len(삭제),
        "재입고": len(재입고),
        "품절단종": len(품절단종),
        "가격변동": len(가격변동),
        "이미지변경": len(이미지변경),
        "상품명변경": len(상품명변경),
    }
    print(f"[comparator] 비교 완료: {result}")
    return result
