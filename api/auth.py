from flask import session, redirect, url_for, request, Blueprint, \
    render_template, abort, flash
from flask_security import Security, login_required
from flask_wtf import Form
from flask_wtf.csrf import CsrfProtect
from wtforms import TextField, validators
from api.database import db, User
import os
import json
from uuid import uuid4

auth = Blueprint('auth', __name__)

security = Security()

csrf = CsrfProtect()

class AddUser(Form):
    name = TextField('name', [validators.DataRequired()])
    email = TextField('email', 
        [validators.DataRequired(), validators.Email()])

    def __init__(self, *args, **kwargs):
        Form.__init__(self, *args, **kwargs)

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False
        
        existing_name = db.session.query(User)\
            .filter(User.name == self.name.data).first()
        if existing_name:
            self.name.errors.append('Name is already registered')
            return False
        
        existing_email = db.session.query(User)\
            .filter(User.email == self.email.data).first()
        if existing_email:
            self.email.errors.appedn('Email is already registered')
            return False
        
        return True

@auth.route('/add-api-user/', methods=['GET', 'POST'])
@login_required
def add_api_user():
    form = AddUser()
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
