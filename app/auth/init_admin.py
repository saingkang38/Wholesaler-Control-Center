import os
from app.auth.models import User
from app.infrastructure import db

def create_initial_admin():
    username = os.getenv("ADMIN_INIT_USERNAME")
    password = os.getenv("ADMIN_INIT_PASSWORD")

    if not username or not password:
        return

    existing = User.query.filter_by(username=username).first()
    if existing:
        return

    admin = User(username=username, role="admin")
    admin.set_password(password)
    db.session.add(admin)
    db.session.commit()
    print(f"[초기화] 관리자 계정 생성 완료: {username}")