from flask import session, redirect, url_for, request, Blueprint, \
    render_template, abort, flash
from flask_login import login_required, login_user, logout_user, LoginManager
from flask_wtf import Form
from flask_wtf.csrf import CsrfProtect
from wtforms import TextField, PasswordField
from wtforms.validators import DataRequired, Email
from api.database import session as db_session
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

class AddUserForm(Form):
    name = TextField('name', validators=[DataRequired()])
    email = TextField('email', validators=[DataRequired(), Email()])
    password = PasswordField('password', validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        Form.__init__(self, *args, **kwargs)
        self.user = None

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False

        existing_name = db_session.query(User)\
            .filter(User.name == self.name.data).first()
        if existing_name:
            self.name.errors.append('Name is already registered')
            return False

        existing_email = db_session.query(User)\
            .filter(User.email == self.email.data).first()
        if existing_email:
            self.email.errors.append('Email address is already registered')
            return False
        
        return True

class ResetPasswordForm(Form):
    old_password = PasswordField('old_password', validators=[DataRequired()])
    new_password = PasswordField('new_password', validators=[DataRequired()])

@login_manager.user_loader
def load_user(userid):
    return db_session.query(User).get(userid)

@auth.route('/login/', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = form.user
        login_user(user)
        return redirect(request.args.get('next') or url_for('auth.api_user_list'))
    email = form.email.data
    return render_template('login.html', form=form, email=email)

@auth.route('/logout/')
def logout():
    logout_user()
    return redirect(url_for('auth.api_user_list'))

@auth.route('/add-api-user/', methods=['GET', 'POST'])
@login_required
def add_api_user():
    form = AddUserForm()
    if form.validate_on_submit():
        user_info = {
            'name': form.name.data,
            'email': form.email.data,
        }
        user = ApiUser(**user_info)
        db.session.add(user)
        db.session.commit()
        flash('User %s added' % user.name)
        return redirect(url_for('auth.api_user_list'))
    return render_template('add_api_user.html', form=form)

@auth.route('/')
@login_required
def api_user_list():
    users = db.session.query(ApiUser).all()
    return render_template('api_user_list.html', users=users)

@auth.route('/sessions/<api_key>/')
def user_sessions(api_key):
    user = db.session.query(ApiUser).get(api_key)
    return render_template('user_sessions.html', user=user)
