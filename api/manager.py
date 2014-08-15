from flask import Blueprint, request, session as flask_session, \
    render_template, make_response
from api.database import session as db_session, DEFAULT_ROLES
from api.models import User, Role
from api.auth import login_required, check_roles
from flask_wtf import Form
from wtforms import TextField, PasswordField, SelectMultipleField
from wtforms.validators import DataRequired, Email
from sqlalchemy import Table
from api.database import session as db_session, engine, Base
from api.models import DedupeSession
from itertools import groupby
from operator import itemgetter
import json
from cPickle import loads

manager = Blueprint('manager', __name__)

ROLE_CHOICES = [(r['name'], r['description'],) for r in DEFAULT_ROLES]

class AddUserForm(Form):
    name = TextField('name', validators=[DataRequired()])
    email = TextField('email', validators=[DataRequired(), Email()])
    roles = SelectMultipleField('roles', choices=ROLE_CHOICES, 
                                validators=[DataRequired()])
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

@manager.route('/add-user/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def add_user():
    form = AddUserForm()
    if form.validate_on_submit():
        roles = []
        for role in roles:
            r = db_session.query(Role).filter(Role.name == role).first()
            roles.append(r)
        user_info = {
            'name': form.name.data,
            'email': form.email.data,
            'roles': roles,
        }
        user = User(**user_info)
        db_session.add(user)
        db_session.commit()
        flash('User %s added' % user.name)
        return redirect(url_for('auth.user_list'))
    roles = db_session.query(Role).all()
    return render_template('add_user.html', form=form, roles=roles)

@manager.route('/user-list/')
@login_required
@check_roles(roles=['admin'])
def user_list():
    users = db_session.query(User).all()
    return render_template('user_list.html', users=users)

@manager.route('/review-queue/<session_id>/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def review_queue(session_id):
    user = db_session.query(User).get(flask_session['user_id'])
    if session_id not in [s.id for s in user.sessions]:
        return abort(401)
    sess = db_session.query(DedupeSession).get(session_id)
    field_defs = [f['field'] for f in json.loads(sess.field_defs)]
    raw_table = Table('raw_%s' % session_id, Base.metadata, 
        autoload=True, autoload_with=engine)
    entity_table = Table('entity_%s' % session_id, Base.metadata,
        autoload=True, autoload_with=engine)
    cols = [getattr(raw_table.c, f) for f in field_defs]
    cols.append(raw_table.c.record_id)
    q = db_session.query(entity_table, *cols)
    fields = [f['name'] for f in q.column_descriptions]
    clusters = q.filter(raw_table.c.record_id == entity_table.c.record_id)\
        .filter(entity_table.c.clustered == False)\
        .order_by(entity_table.c.group_id)\
        .all()
    clusters_d = []
    for cluster in clusters:
        d = {}
        for k,v in zip(fields, cluster):
            d[k] = v
        clusters_d.append(d)
    grouped = {}
    for k,g in groupby(clusters_d, key=itemgetter('group_id')):
        grouped[k] = list(g)
    context = {
        'user': user, 
        'grouped': grouped,
        'session_id': session_id
    }
    return render_template('review-queue.html', **context)

@manager.route('/mark-cluster/<session_id>/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def mark_cluster(session_id):
    user = db_session.query(User).get(flask_session['user_id'])
    if session_id not in [s.id for s in user.sessions]:
        return abort(401)
    entity_table = Table('entity_%s' % session_id, Base.metadata,
        autoload=True, autoload_with=engine)
    conn = engine.contextual_connect()
    group_id = request.args.get('group_id')
    update = {}
    if request.args.get('action') == 'yes':
        update['clustered'] = True
        upd = entity_table.update()\
            .where(entity_table.c.group_id == group_id)\
            .values(clustered=True)
        conn.execute(upd)
    elif request.args.get('actions') == 'no':
        rows = group.all()
        dels = entity_table.delete()\
            .where(entity_table.c.group_id == group_id)
        conn.execute(dels)
    resp = make_response(json.dumps({}))
    resp.headers['Content-Type'] = 'application/json'
    return resp

