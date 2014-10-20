from flask import Blueprint, request, session as flask_session, \
    render_template, make_response, flash, redirect, url_for
from api.database import app_session as db_session, Base
from api.models import User, Role, DedupeSession, Group
from api.auth import login_required, check_roles
from api.utils.helpers import preProcess
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
from csvkit.unicsv import UnicodeCSVReader

manager = Blueprint('manager', __name__)

def role_choices():
    return db_session.query(Role).all()

def group_choices():
    return db_session.query(Group).all()

class AddUserForm(Form):
    name = TextField('name', validators=[DataRequired()])
    email = TextField('email', validators=[DataRequired(), Email()])
    roles = QuerySelectMultipleField('roles', query_factory=role_choices, 
                                validators=[DataRequired()])
    groups = QuerySelectMultipleField('groups', query_factory=group_choices, 
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

@manager.route('/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def index():
    user = db_session.query(User).get(flask_session['user_id'])
    roles = [r.name for r in user.roles]
    return render_template('index.html', user=user, roles=roles)

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
        user.groups = form.groups.data
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

@manager.route('/session-review/<session_id>/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def session_review(session_id):
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('session-review.html', session_id=session_id, user=user)

@manager.route('/match-demo/<session_id>/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def match_demo(session_id):
    user = db_session.query(User).get(flask_session['user_id'])
    sess = db_session.query(DedupeSession).get(session_id)
    return render_template('match-demo.html', sess=sess, user=user)

@manager.route('/bulk-match-demo/<session_id>/', methods=['GET', 'POST'])
@login_required
def bulk_match(session_id):
    user = db_session.query(User).get(flask_session['user_id'])
    sess = db_session.query(DedupeSession).get(session_id)
    context = {
        'user': user,
        'sess': sess
    }
    if request.method == 'POST':
        try:
            upload = request.files.values()[0]
        except IndexError:
            upload = None
            flash('File upload is required')
        if upload:
            context['field_defs'] = [f['field'] for f in json.loads(sess.field_defs)]
            reader = UnicodeCSVReader(upload)
            context['header'] = reader.next()
            upload.seek(0)
            flask_session['bulk_match_upload'] = upload.read()
            flask_session['bulk_match_filename'] = upload.filename
    return render_template('bulk-match.html', **context)
