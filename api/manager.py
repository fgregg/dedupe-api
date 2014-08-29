from flask import Blueprint, request, session as flask_session, \
    render_template, make_response, flash, redirect, url_for
from api.database import session as db_session, engine, Base
from api.models import User, Role, DedupeSession
from api.auth import login_required, check_roles
from api.dedupe_utils import preProcess, get_engine
from flask_wtf import Form
from wtforms import TextField, PasswordField
from wtforms.ext.sqlalchemy.fields import QuerySelectMultipleField
from wtforms.validators import DataRequired, Email
from sqlalchemy import Table, and_
from sqlalchemy.sql import select
from itertools import groupby
from operator import itemgetter
import json
from cPickle import loads
from dedupe.convenience import canonicalize

manager = Blueprint('manager', __name__)

def role_choices():
    return Role.query.all()

class AddUserForm(Form):
    name = TextField('name', validators=[DataRequired()])
    email = TextField('email', validators=[DataRequired(), Email()])
    roles = QuerySelectMultipleField('roles', query_factory=role_choices, 
                                validators=[DataRequired()])
    password = PasswordField('password', validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        Form.__init__(self, *args, **kwargs)

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

@manager.route('/add-user/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def add_user():
    form = AddUserForm()
    user = db_session.query(User).get(flask_session['user_id'])
    if form.validate_on_submit():
        user_info = {
            'name': form.name.data,
            'email': form.email.data,
            'password': form.password.data,
        }
        user = User(**user_info)
        db_session.add(user)
        db_session.commit()
        user.roles = form.roles.data
        db_session.add(user)
        db_session.commit()
        flash('User %s added' % user.name)
        return redirect(url_for('manager.user_list'))
    return render_template('add_user.html', form=form, user=user)

@manager.route('/user-list/')
@login_required
@check_roles(roles=['admin'])
def user_list():
    users = db_session.query(User).all()
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('user_list.html', users=users, user=user)

@manager.route('/review-list/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def review():
    return render_template('review-list.html')

@manager.route('/session-review/<session_id>/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def session_review(session_id):
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('session-review.html', session_id=session_id, user=user)
