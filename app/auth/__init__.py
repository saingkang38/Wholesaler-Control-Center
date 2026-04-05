from flask import Blueprint, render_template, redirect, url_for, request
from flask_login import login_user, logout_user, login_required
from app.auth.models import User
from app.infrastructure import db
from app.utils import kst_now

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user = User.query.filter_by(username=username, is_active=True).first()

        if user and user.check_password(password):
            login_user(user)
            user.last_login_at = kst_now()
            db.session.commit()
            return redirect(url_for("dashboard.index"))

        return render_template("login.html", error="아이디 또는 비밀번호가 올바르지 않습니다.")

    return render_template("login.html")

@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))