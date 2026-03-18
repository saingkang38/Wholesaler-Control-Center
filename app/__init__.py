from flask import Flask
import os

def create_app():
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "fallback-dev-key")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///wholesaler.db")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

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

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(wholesalers_bp)
    app.register_blueprint(execution_logs_bp)
    app.register_blueprint(collections_bp)
    app.register_blueprint(normalization_bp)
    app.register_blueprint(suppliers_bp)

    with app.app_context():
        db.create_all()
        from app.auth.init_admin import create_initial_admin
        from app.wholesalers import get_or_create_ownerclan
        create_initial_admin()
        get_or_create_ownerclan()

    return app