from flask import Flask
import os

def create_app():
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "fallback-dev-key")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///wholesaler.db")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "connect_args": {"timeout": 60},
        "pool_pre_ping": True,
    }

    from app.infrastructure import db
    db.init_app(app)

    from flask_login import LoginManager
    login_manager = LoginManager()
    login_manager.login_view = "auth.login"
    login_manager.init_app(app)

    from app.auth.models import User

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    from app.auth import auth_bp
    from app.dashboard import dashboard_bp
    from app.wholesalers import wholesalers_bp
    from app.execution_logs import execution_logs_bp
    from app.collections import collections_bp
    from app.normalization import normalization_bp
    from app.master import master_bp
    from app.master.models import MasterProduct, ProductEvent  # noqa: F401 - 테이블 생성용
    from app.store import store_bp
    from app.store import routes as _store_routes  # noqa: F401 — blueprint에 라우트 등록
    from app.store.models import StoreProduct, NaverStore, ProductExclusion, SyncLog  # noqa: F401 - 테이블 생성용
    from app.actions import actions_bp
    from app.actions.models import ActionSignal  # noqa: F401 - 테이블 생성용
    from app.settings import settings_bp
    from app.settings.models import MarginRule  # noqa: F401 - 테이블 생성용
    # PrepSetting, SmartStoreSetting 삭제됨

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(wholesalers_bp)
    app.register_blueprint(execution_logs_bp)
    app.register_blueprint(collections_bp)
    app.register_blueprint(normalization_bp)
    app.register_blueprint(master_bp)
    app.register_blueprint(store_bp)
    app.register_blueprint(actions_bp)
    app.register_blueprint(settings_bp)

    with app.app_context():
        db.create_all()
        # 신규 컬럼 마이그레이션 (기존 DB에 없으면 추가)
        import logging as _logging
        from sqlalchemy import text
        from sqlalchemy.exc import OperationalError as _OpError
        migrations = [
            "ALTER TABLE master_products ADD COLUMN detail_description TEXT",
            "ALTER TABLE master_products ADD COLUMN product_url TEXT",
            "ALTER TABLE master_products ADD COLUMN edited_name TEXT",
            "ALTER TABLE master_products ADD COLUMN category_id INTEGER",
            "ALTER TABLE master_products ADD COLUMN is_prep_ready INTEGER DEFAULT 0",
            "ALTER TABLE master_products ADD COLUMN current_status TEXT",
            "ALTER TABLE master_products ADD COLUMN first_seen_date TEXT",
            "ALTER TABLE master_products ADD COLUMN last_seen_date TEXT",
            "ALTER TABLE master_products ADD COLUMN missing_days INTEGER DEFAULT 0",
            "ALTER TABLE master_products ADD COLUMN last_status_change_date TEXT",
            "ALTER TABLE master_products ADD COLUMN discontinued_flag INTEGER DEFAULT 0",
            "ALTER TABLE wholesalers ADD COLUMN prefix TEXT",
            "ALTER TABLE wholesalers ADD COLUMN notes TEXT",
            "ALTER TABLE master_products ADD COLUMN origin VARCHAR(128)",
            "ALTER TABLE master_products ADD COLUMN shipping_fee INTEGER",
            "ALTER TABLE master_products ADD COLUMN shipping_condition VARCHAR(256)",
        ]
        for sql in migrations:
            try:
                with db.engine.connect() as conn:
                    conn.execute(text(sql))
                    conn.commit()
            except _OpError:
                pass  # column already exists — 정상적으로 무시
            except Exception as _e:
                _logging.getLogger(__name__).warning(f"마이그레이션 실패: {_e}")
        from app.auth.init_admin import create_initial_admin
        from app.wholesalers import (
            get_or_create_ownerclan, get_or_create_jtckorea, get_or_create_metaldiy,
            get_or_create_ds1008, get_or_create_hitdesign, get_or_create_mro3,
            get_or_create_feelwoo, get_or_create_sikjaje, get_or_create_onch3,
            get_or_create_chingudome, get_or_create_zentrade, get_or_create_dometopia,
        )
        create_initial_admin()
        for init_fn in [
            get_or_create_ownerclan, get_or_create_jtckorea, get_or_create_metaldiy,
            get_or_create_ds1008, get_or_create_hitdesign, get_or_create_mro3,
            get_or_create_feelwoo, get_or_create_sikjaje, get_or_create_onch3,
            get_or_create_chingudome, get_or_create_zentrade, get_or_create_dometopia,
        ]:
            try:
                init_fn()
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"도매처 초기화 실패: {e}")

    return app