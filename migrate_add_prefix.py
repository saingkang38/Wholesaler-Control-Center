"""
도매처 prefix 마이그레이션
- wholesalers 테이블에 prefix 컬럼 추가
- 전 도매처 prefix 값 설정 (dometopia 없으면 INSERT)
- master_products.supplier_product_code 앞에 prefix 붙이기 (idempotent)
- 이중 prefix 제거 (이전 마이그레이션으로 생긴 중복 정리)
- normalized_products.source_product_code 앞에 prefix 붙이기 (테이블 없으면 skip)
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "instance" / "wholesaler.db"

# 이전 마이그레이션에서 사용된 old prefix → 이중 적용된 경우 제거
# (wholesaler_code, new_prefix, old_prefix)
DOUBLE_PREFIX_FIXES = [
    ("mro3",     "mro_",  "3mro_"),
    ("zentrade",  "zt_",   "zen_"),
    ("feelwoo",   "fwc_",  "fw_"),
    ("sikjaje",   "sik_",  "sj_"),
]

PREFIX_MAP = {
    "dometopia":  "doto_",
    "ownerclan":  "on_",
    "ds1008":     "ds_",
    "mro3":       "mro_",
    "zentrade":   "zt_",
    "chingudome": "79_",
    "feelwoo":    "fwc_",
    "hitdesign":  "hit_",
    "jtckorea":   "jtc_",
    "sikjaje":    "sik_",
    "onch3":      "onch_",
    "metaldiy":   "cm_",
}

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# 1. prefix 컬럼 추가 (이미 있으면 skip)
try:
    cur.execute("ALTER TABLE wholesalers ADD COLUMN prefix VARCHAR(20)")
    print("[1] wholesalers.prefix 컬럼 추가됨")
except sqlite3.OperationalError:
    print("[1] wholesalers.prefix 컬럼 이미 존재 - skip")

# 2. 각 wholesaler prefix UPDATE
for code, prefix in PREFIX_MAP.items():
    cur.execute("UPDATE wholesalers SET prefix = ? WHERE code = ?", (prefix, code))
    print(f"[2] {code} → {prefix}")

con.commit()

# 3. dometopia 없으면 INSERT
row = cur.execute("SELECT id FROM wholesalers WHERE code = 'dometopia'").fetchone()
if not row:
    cur.execute(
        "INSERT INTO wholesalers (code, name, site_url, prefix) VALUES (?, ?, ?, ?)",
        ("dometopia", "도매토피아", "https://www.dometopia.com", "doto_"),
    )
    con.commit()
    print("[3] dometopia 도매처 INSERT 완료")
else:
    print("[3] dometopia 이미 존재 - skip")

# 4. master_products.supplier_product_code - prefix 없는 것에만 붙이기
rows = cur.execute("""
    SELECT mp.id, mp.supplier_product_code, w.prefix
    FROM master_products mp
    JOIN wholesalers w ON mp.wholesaler_id = w.id
    WHERE w.prefix IS NOT NULL
""").fetchall()

updated = 0
for row_id, code, prefix in rows:
    if not code.startswith(prefix):
        cur.execute(
            "UPDATE master_products SET supplier_product_code = ? WHERE id = ?",
            (f"{prefix}{code}", row_id),
        )
        updated += 1
        if updated % 500 == 0:
            con.commit()

con.commit()
print(f"[4] master_products 업데이트: {updated}건")

# 5. 이중 prefix 제거 (master_products)
def fix_double_prefix(cur, table, col, fixes):
    total = 0
    for wcode, new_pfx, old_pfx in fixes:
        wid = cur.execute("SELECT id FROM wholesalers WHERE code=?", (wcode,)).fetchone()
        if not wid:
            continue
        wid = wid[0]
        bad_pfx = new_pfx + old_pfx
        rows = cur.execute(
            f"SELECT id, {col} FROM {table} WHERE wholesaler_id=? AND {col} LIKE ?",
            (wid, bad_pfx + "%"),
        ).fetchall()
        for rid, code in rows:
            fixed = new_pfx + code[len(bad_pfx):]
            cur.execute(f"UPDATE {table} SET {col}=? WHERE id=?", (fixed, rid))
            total += 1
            if total % 500 == 0:
                cur.connection.commit()
    cur.connection.commit()
    return total

fixed = fix_double_prefix(cur, "master_products", "supplier_product_code", DOUBLE_PREFIX_FIXES)
print(f"[5] master_products 이중 prefix 제거: {fixed}건")

# 6. normalized_products - 테이블 없으면 graceful skip
tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
if "normalized_products" not in tables:
    print("[6] normalized_products 테이블 없음 - skip")
else:
    rows = cur.execute("""
        SELECT np.id, np.source_product_code, w.prefix
        FROM normalized_products np
        JOIN wholesalers w ON np.wholesaler_id = w.id
        WHERE w.prefix IS NOT NULL
    """).fetchall()

    updated = 0
    for row_id, code, prefix in rows:
        if not code.startswith(prefix):
            cur.execute(
                "UPDATE normalized_products SET source_product_code = ? WHERE id = ?",
                (f"{prefix}{code}", row_id),
            )
            updated += 1
            if updated % 500 == 0:
                con.commit()

    con.commit()
    print(f"[6] normalized_products 업데이트: {updated}건")

    fixed = fix_double_prefix(cur, "normalized_products", "source_product_code", DOUBLE_PREFIX_FIXES)
    print(f"[7] normalized_products 이중 prefix 제거: {fixed}건")

con.close()
print("\n마이그레이션 완료.")
print("검증: python -c \"import sqlite3; con=sqlite3.connect('instance/wholesaler.db'); print(con.execute('SELECT supplier_product_code FROM master_products LIMIT 10').fetchall())\"")
