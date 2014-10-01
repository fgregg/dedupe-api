# -*- coding: utf-8 -*-
from flask import session as flask_session, redirect, url_for, request, Blueprint, \
    render_template, abort, flash, make_response
from functools import wraps
from flask_login import login_required, login_user, logout_user, LoginManager
from flask_wtf import Form
from flask_wtf.csrf import CsrfProtect
from wtforms import TextField, PasswordField
from wtforms.validators import DataRequired, Email
from api.database import session as db_session
from api.models import User
import os
import json
from uuid import uuid4

auth = Blueprint('auth', __name__)

login_manager = LoginManager()

csrf = CsrfProtect()

class LoginForm(Form):
    email = TextField('email', validators=[DataRequired(), Email()])
    password = PasswordField('password', validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        Form.__init__(self, *args, **kwargs)
        self.user = None

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False

        user = db_session.query(User)\
            .filter(User.email == self.email.data).first()
        if user is None:
            self.email.errors.append('Email address is not registered')
            return False

        if not user.check_password(user.name, self.password.data):
            self.password.errors.append('Password is not valid')
            return False

        self.user = user
        return True

class ResetPasswordForm(Form):
    old_password = PasswordField('old_password', validators=[DataRequired()])
    new_password = PasswordField('new_password', validators=[DataRequired()])

def check_roles(roles=[]):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            user = db_session.query(User).get(flask_session['user_id'])
            user_roles = set([r.name for r in user.roles])
            rs = set(roles)
            if user_roles.issubset(rs):
                return f(*args, **kwargs)
            else:
                flash('Sorry, you don\'t have access to that page')
                return redirect(url_for('trainer.index'))
        return decorated
    return decorator

def check_api_key():
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            api_key = None
            resp = {
                'status': 'ok',
                'message': ''
            }
            status_code = 200
            if flask_session.get('user_id'):
                api_key = flask_session['user_id']
            else:
                api_key = request.args.get('api_key')
            if not api_key:
                resp['status'] = 'error'
                resp['message'] = "'api_key' is a required parameter"
                status_code = 401
                response = make_response(json.dumps(resp), status_code)
                response.headers['Content-Type'] = 'application/json'
                return response
            return f(*args, **kwargs)
        return decorated
    return decorator

@login_manager.user_loader
def load_user(userid):
    return db_session.query(User).get(userid)

@auth.route('/login/', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = form.user
        login_user(user)
        return redirect(request.args.get('next') or url_for('manager.index'))
    email = form.email.data
    return render_template('login.html', form=form, email=email)

@auth.route('/logout/')
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

