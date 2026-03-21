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
    from app.suppliers import suppliers_bp
    from app.master import master_bp
    from app.master.models import MasterProduct, ProductEvent  # noqa: F401 - 테이블 생성용
    from app.store import store_bp
    from app.store.models import StoreProduct, NaverStore, ProductExclusion, SyncLog, BulkRegisterJob, BulkRegisterItem  # noqa: F401 - 테이블 생성용
    from app.actions import actions_bp
    from app.actions.models import ActionSignal  # noqa: F401 - 테이블 생성용
    from app.settings import settings_bp
    from app.settings.models import MarginRule  # noqa: F401 - 테이블 생성용
    from app.exporter import exporter_bp
    from app.orders import orders_bp
    from app.inquiries import inquiries_bp
    from app.settlement import settlement_bp
    from app.group_products import group_products_bp
    from app.product_register import product_register_bp
    from app.product_prep import product_prep_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(wholesalers_bp)
    app.register_blueprint(execution_logs_bp)
    app.register_blueprint(collections_bp)
    app.register_blueprint(normalization_bp)
    app.register_blueprint(suppliers_bp)
    app.register_blueprint(master_bp)
    app.register_blueprint(store_bp)
    app.register_blueprint(actions_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(exporter_bp)
    app.register_blueprint(orders_bp)
    app.register_blueprint(inquiries_bp)
    app.register_blueprint(settlement_bp)
    app.register_blueprint(group_products_bp)
    app.register_blueprint(product_register_bp)
    app.register_blueprint(product_prep_bp)

    with app.app_context():
        db.create_all()
        from app.auth.init_admin import create_initial_admin
        from app.wholesalers import get_or_create_ownerclan, get_or_create_jtckorea, get_or_create_metaldiy, get_or_create_ds1008, get_or_create_hitdesign, get_or_create_mro3, get_or_create_feelwoo, get_or_create_sikjaje, get_or_create_onch3
        create_initial_admin()
        get_or_create_ownerclan()
        get_or_create_jtckorea()
        get_or_create_metaldiy()
        get_or_create_ds1008()
        get_or_create_hitdesign()
        get_or_create_mro3()
        get_or_create_feelwoo()
        get_or_create_sikjaje()
        get_or_create_onch3()

    return app