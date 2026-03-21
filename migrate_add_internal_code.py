from dotenv import load_dotenv
load_dotenv()

from app import create_app
app = create_app()

with app.app_context():
    from app.infrastructure import db
    with db.engine.connect() as conn:
        conn.execute(db.text("ALTER TABLE master_products ADD COLUMN internal_code VARCHAR(128)"))
        conn.commit()
    print("완료: internal_code 컬럼 추가됨")
