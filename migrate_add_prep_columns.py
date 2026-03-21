"""
SQLite ALTER TABLE: master_products 가공 컬럼 추가
실행: python migrate_add_prep_columns.py
"""
import sqlite3
import os

DB_PATH = os.getenv("DATABASE_URL", "instance/wholesaler.db").replace("sqlite:///", "")

COLUMNS = [
    ("product_url",   "TEXT"),
    ("edited_name",   "TEXT"),
    ("category_id",   "TEXT"),
    ("is_prep_ready", "INTEGER DEFAULT 0"),
]

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(master_products)")
    existing = {row[1] for row in cur.fetchall()}

    for col_name, col_type in COLUMNS:
        if col_name not in existing:
            sql = f"ALTER TABLE master_products ADD COLUMN {col_name} {col_type}"
            cur.execute(sql)
            print(f"Added: {col_name}")
        else:
            print(f"Skip (already exists): {col_name}")

    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    migrate()
