"""필수값 누락 분류 + 사용자 입력 주입.

Naver PUT 시 카테고리별 필수값(productInfoProvidedNotice, unitCapacity 등)이
빠지면 invalidInputs로 거부된다. 자동 보강은 데이터 정확도 위험이 있어
운영자가 직접 채우도록 분류만 하고 입력 폼에 노출한다.

화이트리스트(KNOWN_REQUIRED_FIELDS)에 등록된 필드만 awaiting_input으로 분기.
나머지는 기존 failed 흐름 유지 (호출부에서 빈 리스트면 raise).

키 정확성: 실제로 어드민에서 정상 등록된 상품(channel=7282006103)의
GET 응답을 떠 확인한 결과를 반영. 추측 키는 모두 실키로 교체.
- consumptionDate(date) → consumptionDateText(자유 텍스트)
- unitCapacityRefSize → unitCapacity (부모와 같은 이름이지만 점경로상 명확)
- unitCapacitySize → totalCapacityValue
- unitCapacityType("GRAM") → indicationUnit("g") (소문자 enum)
"""
from __future__ import annotations


# Naver invalidInputs로 알려주는 필드명(키) → 실제 PUT 본문에 들어갈 경로(target_path).
# Naver는 invalidInputs로 'consumptionDate' 누락이라 알리지만, 직접입력 모드는
# 'consumptionDateText'에 자유 텍스트를 넣어야 통과한다. 이 mismatch를 매핑으로 흡수.
KNOWN_REQUIRED_FIELDS: dict[str, dict] = {
    # 소비기한 — Naver 카테고리에 따라 productInfoProvidedNotice 하위 키가 달라진다.
    # 두 카테고리 모두 invalidInputs는 '...consumptionDate' 키로 알려주지만,
    # 직접입력 모드 페이로드는 '...consumptionDateText'에 들어가야 함.
    "originProduct.detailAttribute.productInfoProvidedNotice.generalFood.consumptionDate": {
        "kind": "text",
        "label": "소비기한 또는 품질유지기한 (직접입력)",
        "hint": "예: 제조일로부터 24개월 / 별도표기 / 2026-12-31까지",
        "target_path": "originProduct.detailAttribute.productInfoProvidedNotice.generalFood.consumptionDateText",
    },
    "originProduct.detailAttribute.productInfoProvidedNotice.food.consumptionDate": {
        "kind": "text",
        "label": "소비기한 또는 품질유지기한 (직접입력)",
        "hint": "예: 제조일로부터 24개월 / 별도표기 / 2026-12-31까지",
        "target_path": "originProduct.detailAttribute.productInfoProvidedNotice.food.consumptionDateText",
    },

    # 가격표시제 단위가격 — Naver는 unitPriceYn만 invalidInputs로 알리지만
    # 실제로는 표시단위/총용량/단위까지 함께 보내야 통과. 진짜 키는 다음과 같다.
    "originProduct.detailAttribute.unitCapacity.unitPriceYn": {
        "kind": "yn",
        "label": "가격표시제 단위가격 사용여부",
        "hint": "Y(사용) / N(미사용) — Y 선택 시 아래 표시단위/총 용량/단위까지 함께 채워주세요",
        "co_required": [
            "originProduct.detailAttribute.unitCapacity.unitCapacity",
            "originProduct.detailAttribute.unitCapacity.totalCapacityValue",
            "originProduct.detailAttribute.unitCapacity.indicationUnit",
        ],
    },
    # 표시단위 수치 — 부모 객체 이름과 키 이름이 같아 보이지만 점경로상 unique
    "originProduct.detailAttribute.unitCapacity.unitCapacity": {
        "kind": "number",
        "label": "표시단위 수치",
        "hint": "예: 100 (100g당 가격이면 100)",
    },
    "originProduct.detailAttribute.unitCapacity.totalCapacityValue": {
        "kind": "number",
        "label": "총 용량 수치",
        "hint": "예: 200 (200g 용량이면 200)",
    },
    "originProduct.detailAttribute.unitCapacity.indicationUnit": {
        "kind": "select",
        "label": "단위 (소문자)",
        "hint": "표시단위와 총 용량의 단위 (둘이 같음)",
        "options": [
            {"value": "g",  "label": "g (그램)"},
            {"value": "kg", "label": "kg (킬로그램)"},
            {"value": "ml", "label": "ml (밀리리터)"},
            {"value": "L",  "label": "L (리터)"},
            {"value": "m",  "label": "m (미터)"},
            {"value": "cm", "label": "cm (센티미터)"},
            {"value": "ea", "label": "개"},
        ],
    },
}


def _make_field_entry(name: str, message: str = "") -> dict | None:
    """KNOWN_REQUIRED_FIELDS의 메타를 모달 entry 형식으로 변환. 미등록 필드면 None."""
    meta = KNOWN_REQUIRED_FIELDS.get(name)
    if not meta:
        return None
    entry = {
        "name": name,
        "message": message or "",
        "kind": meta["kind"],
        "label": meta["label"],
        "hint": meta["hint"],
    }
    if "options" in meta:
        entry["options"] = meta["options"]
    return entry


def classify_invalid_inputs(invalids: list) -> list[dict]:
    """invalidInputs 리스트에서 KNOWN_REQUIRED_FIELDS와 매치되는 항목만 메타와 함께 반환.

    co_required 메타가 있으면 연관 필드도 자동으로 함께 노출(Naver가 invalidInputs에
    1개 필드만 알려도 운영자가 한 번에 입력해서 PUT 1회로 처리되도록).

    매치 0건이면 빈 리스트 → 호출부는 기존 failed 흐름 유지.
    """
    matched: list[dict] = []
    seen: set[str] = set()
    for inv in invalids or []:
        name = (inv.get("name") or "").strip()
        if not name or name in seen:
            continue
        entry = _make_field_entry(name, inv.get("message") or "")
        if not entry:
            continue
        seen.add(name)
        matched.append(entry)

        # co_required 연관 필드도 함께 추가 (보조 필드 — 빈 값 허용)
        meta = KNOWN_REQUIRED_FIELDS.get(name) or {}
        for co_name in meta.get("co_required") or []:
            if co_name in seen:
                continue
            co_entry = _make_field_entry(co_name)
            if not co_entry:
                continue
            co_entry["optional"] = True  # 빈 값 허용 표시 (라우트/UI에서 검증 완화)
            seen.add(co_name)
            matched.append(co_entry)
    return matched


def _coerce_value(raw_value, kind: str):
    """KNOWN_REQUIRED_FIELDS의 kind에 맞춰 사용자 입력 문자열을 Naver가 기대하는 타입으로 변환.

    - "yn": Naver는 boolean을 기대. "Y"/"true"/"1" → True, 그 외 → False.
    - "number": 정수면 int, 아니면 float. "100" → 100.
    - "select" / "text" 등: 문자열 그대로.
    """
    if kind == "yn":
        return str(raw_value).strip().upper() in ("Y", "TRUE", "1")
    if kind == "number":
        s = str(raw_value).strip()
        try:
            if "." in s:
                return float(s)
            return int(s)
        except ValueError:
            return raw_value  # 빈 값/포맷 오류는 라우트에서 차단됐어야 함 — 폴백
    return raw_value


# Naver API에서 boolean 타입을 기대하지만 GET 응답에 "Y"/"N" 문자열로 박혀 들어오는
# 일이 있는 필드 화이트리스트. PUT 직전에 자동으로 boolean 정규화하기 위한 명시 등록.
# 발견되는 새 필드는 여기에 추가하면 됨.
NAVER_BOOLEAN_PATHS: frozenset = frozenset({
    "originProduct.detailAttribute.unitCapacity.unitPriceYn",
    "originProduct.detailAttribute.productInfoProvidedNotice.generalFood.geneticallyModified",
    "originProduct.detailAttribute.productInfoProvidedNotice.generalFood.importDeclarationCheck",
    "originProduct.detailAttribute.productInfoProvidedNotice.food.geneticallyModified",
    "originProduct.detailAttribute.productInfoProvidedNotice.food.importDeclarationCheck",
})


def coerce_payload_booleans(payload: dict) -> tuple[int, list[str]]:
    """PUT 페이로드를 walk하며 화이트리스트 boolean 필드의 'Y'/'N'/'TRUE'/'FALSE' 문자열을
    boolean(True/False)으로 정규화.

    화이트리스트(NAVER_BOOLEAN_PATHS) 매치된 필드만 변환한다.

    Yn 접미사 패턴은 위험 — Naver의 일부 필드는 이름이 'Yn'으로 끝나지만 실제로는
    enum 타입(예: kcCertifiedProductExclusionYn: KcCertificationExclusionType, "TRUE"/"FALSE"
    문자열을 enum 값으로 받음). boolean으로 변환하면 deserialize 실패하므로 절대 자동
    변환 안 함. 새 진짜 boolean 필드는 NAVER_BOOLEAN_PATHS에 명시 추가.

    문자열이 아닌 값은 건드리지 않음.
    반환: (변환 개수, 변환된 경로 리스트)
    """
    converted = 0
    converted_paths: list[str] = []

    def _walk(node, path: str) -> None:
        nonlocal converted
        if isinstance(node, dict):
            for k, v in list(node.items()):
                child_path = f"{path}.{k}" if path else k
                if isinstance(v, str) and child_path in NAVER_BOOLEAN_PATHS:
                    norm = v.strip().upper()
                    if norm in ("Y", "TRUE", "1"):
                        node[k] = True
                        converted += 1
                        converted_paths.append(child_path)
                    elif norm in ("N", "FALSE", "0"):
                        node[k] = False
                        converted += 1
                        converted_paths.append(child_path)
                else:
                    _walk(v, child_path)
        elif isinstance(node, list):
            for i, item in enumerate(node):
                _walk(item, f"{path}[{i}]")

    _walk(payload, "")
    return converted, converted_paths


def apply_user_input(payload: dict, user_values: dict[str, str]) -> None:
    """점-경로 문자열로 payload(=PUT 본문)에 사용자 입력 in-place 주입.

    KNOWN_REQUIRED_FIELDS의 kind에 맞춰 타입 변환(예: "yn" → boolean), 그리고
    target_path 메타가 있으면 그 경로로 라우팅(예: consumptionDate → consumptionDateText).
    중간 dict가 없으면 자동 생성. payload는 {"originProduct": {...}} 형태를 기대.
    """
    for path, raw_value in (user_values or {}).items():
        if not path or raw_value is None:
            continue
        meta = KNOWN_REQUIRED_FIELDS.get(path) or {}
        value = _coerce_value(raw_value, meta.get("kind", "text"))
        target_path = meta.get("target_path") or path
        keys = target_path.split(".")
        node = payload
        for k in keys[:-1]:
            nxt = node.get(k)
            if not isinstance(nxt, dict):
                nxt = {}
                node[k] = nxt
            node = nxt
        node[keys[-1]] = value
