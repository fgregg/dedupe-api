from flask import Blueprint, request, session as flask_session, \
    render_template, make_response, flash, redirect, url_for, current_app
from flask_login import current_user
from functools import wraps
from api.database import app_session as db_session, Base
from api.models import User, Role, DedupeSession, Group, WorkTable
from api.auth import login_required, check_roles, check_sessions
from api.utils.helpers import preProcess, STATUS_LIST
from api.utils.delayed_tasks import cleanupTables, reDedupeRaw, \
    reDedupeCanon, trainDedupe
from flask_wtf import Form
from wtforms import TextField, PasswordField
from wtforms.ext.sqlalchemy.fields import QuerySelectMultipleField
from wtforms.validators import DataRequired, Email
from sqlalchemy import Table, and_, text, MetaData
from sqlalchemy.sql import select
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from itertools import groupby
from operator import itemgetter
import json
from cPickle import loads
from dedupe.convenience import canonicalize
from csvkit.unicsv import UnicodeCSVReader
import dedupe
from cStringIO import StringIO
from datetime import datetime

admin = Blueprint('admin', __name__)

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

@admin.route('/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def index():
    status = request.args.get('status')
    if status is None:
        status = 'in-progress'
    return render_template('index.html', status=status)

@admin.route('/add-user/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def add_user():
    form = AddUserForm()
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
        return redirect(url_for('admin.user_list'))
    return render_template('add_user.html', form=form)

@admin.route('/user-list/')
@login_required
@check_roles(roles=['admin'])
def user_list():
    users = db_session.query(User).all()
    return render_template('user_list.html', users=users)

@admin.route('/session-admin/')
@login_required
@check_sessions()
def session_admin():
    
    session_id = flask_session['session_id']

    dedupe_session = db_session.query(DedupeSession).get(session_id)
    db_session.refresh(dedupe_session)
    predicates = None
    session_info = {}
    training_data = None
    status_info = dedupe_session.as_dict()['status_info']
    if dedupe_session.field_defs:
        field_defs = json.loads(dedupe_session.field_defs)
        for fd in field_defs:
            try:
                session_info[fd['field']]['types'].append(fd['type'])
                session_info[fd['field']]['has_missing'] = fd.get('has_missing', '')
                session_info[fd['field']]['children'] = []
            except KeyError:
                session_info[fd['field']] = {
                                              'types': [fd['type']],
                                              'has_missing': fd.get('has_missing', ''),
                                            }
                session_info[fd['field']]['children'] = []
    if dedupe_session.settings_file:
        dd = dedupe.StaticDedupe(StringIO(dedupe_session.settings_file))
        for field in dd.data_model.primary_fields:
            name, ftype = field.field, field.type
            if ftype in ['Categorical', 'Address', 'Set']:
                children = []
                for f in field.higher_vars:
                    children.append((f.name, f.type, f.has_missing, f.weight,) )
                session_info[name]['children'] = children
            try:
                session_info[name]['learned_weight'] = field.weight
            except KeyError: # pragma: no cover
                session_info[name] = {'learned_weight': field.weight}
        predicates = dd.predicates
    if dedupe_session.training_data:
        td = json.loads(dedupe_session.training_data)
        training_data = {'distinct': [], 'match': []}
        for left, right in td['distinct']:
            keys = left.keys()
            pair = []
            for key in keys:
                d = {
                    'field': key,
                    'left': left[key],
                    'right': right[key]
                }
                pair.append(d)
            training_data['distinct'].append(pair)
        for left, right in td['match']:
            keys = left.keys()
            pair = []
            for key in keys:
                d = {
                    'field': key,
                    'left': left[key],
                    'right': right[key]
                }
                pair.append(d)
            training_data['match'].append(pair)
    return render_template('session-admin.html', 
                            dedupe_session=dedupe_session, 
                            session_info=session_info, 
                            predicates=predicates,
                            training_data=training_data,
                            status_info=status_info)

@admin.route('/training-data/')
@login_required
@check_sessions()
def training_data():

    session_id = flask_session['session_id']
    data = db_session.query(DedupeSession).get(session_id)
    training_data = data.training_data
    
    resp = make_response(training_data, 200)
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_training.json' % data.id
    return resp

@admin.route('/settings-file/')
@login_required
@check_sessions()
def settings_file():
    session_id = flask_session['session_id']
    data = db_session.query(DedupeSession).get(session_id)
    settings_file = data.settings_file
    resp = make_response(settings_file, 200)
    resp.headers['Content-Disposition'] = 'attachment; filename=%s.dedupe_settings' % data.id
    return resp

@admin.route('/field-definitions/')
@login_required
@check_sessions()
def field_definitions():
    session_id = flask_session['session_id']
    data = db_session.query(DedupeSession).get(session_id)
    field_defs = data.field_defs
    
    resp = make_response(field_defs, 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@admin.route('/delete-data-model/')
@login_required
@check_sessions()
def delete_data_model():

    session_id = flask_session['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    dedupe_session.field_defs = None
    dedupe_session.settings_file = None
    dedupe_session.gaz_settings_file = None
    dedupe_session.status = 'dataset uploaded'
    db_session.add(dedupe_session)
    db_session.commit()
    tables = [
        'entity_{0}',
        'block_{0}',
        'plural_block_{0}',
        'covered_{0}',
        'plural_key_{0}',
        'small_cov_{0}',
    ]
    engine = db_session.bind
    for table in tables: # pragma: no cover
        with engine.begin() as conn:
            table_name = table.format(session_id)
            conn.execute('DROP TABLE IF EXISTS "{0}" CASCADE'.format(table_name))
    resp = {
        'status': 'ok',
        'message': 'Data model for session {0} deleted'.format(session_id)
    }
    status_code = 200

    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@admin.route('/delete-session/')
@login_required
@check_sessions()
def delete_session():

    session_id = flask_session['session_id']
    data = db_session.query(DedupeSession).get(session_id)
    db_session.delete(data)
    db_session.commit()
    tables = [
        'entity_{0}',
        'entity_{0}_cr',
        'raw_{0}',
        'processed_{0}',
        'processed_{0}_cr',
        'block_{0}',
        'block_{0}_cr',
        'plural_block_{0}',
        'plural_block_{0}_cr',
        'cr_{0}',
        'covered_{0}',
        'covered_{0}_cr',
        'plural_key_{0}',
        'plural_key_{0}_cr',
        'small_cov_{0}',
        'small_cov_{0}_cr',
        'canon_{0}',
        'exact_match_{0}',
        'match_blocks_{0}',
    ]
    cleanupTables.delay(session_id, tables=tables)
    resp = make_response(json.dumps({'session_id': session_id, 'status': 'ok'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@admin.route('/session-list/')
@login_required
def review():
    
    sess_id = request.args.get('session_id')
    status = request.args.get('status')

    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    all_sessions = []
    sessions = db_session.query(DedupeSession.id)\
        .filter(DedupeSession.group.has(
            Group.id.in_([i.id for i in current_user.groups])))\
        .order_by(DedupeSession.date_updated.desc())\
        .all()
    engine = db_session.bind
    sel = ''' 
        SELECT 
            d.id,
            d.name,
            d.description,
            d.filename,
            d.date_added,
            d.date_updated,
            d.status,
            d.record_count,
            d.entity_count,
            d.review_count,
            d.processing,
            d.field_defs,
            w.value AS last_work_status
        FROM dedupe_session AS d
        LEFT JOIN (
            SELECT value, session_id
            FROM work_table
            ORDER BY updated DESC
            LIMIT 1
        ) AS w
        ON d.id = w.session_id
        WHERE 1=1
    '''
    qargs = {}
    if sess_id:
        if sess_id in [s.id for s in sessions]:
            sel = text('{0} AND d.id = :sess_id'.format(sel))
            qargs['sess_id'] = sess_id
    if status:
        if status == 'canonical':
            sel = text("{0} AND d.status = 'canonical'".format(sel))
        elif status == 'in-progress':
            sel = text("{0} AND d.status != 'canonical'".format(sel))
    for row in engine.execute(sel, **qargs):
        d = dict(zip(row.keys(), row.values()))
        if row.date_added:
            d['date_added'] = row.date_added.isoformat()
        if row.date_updated:
            d['date_updated'] = row.date_added.isoformat()
        if row.field_defs:
            d['field_defs'] = json.loads(unicode(row.field_defs))
        d['status_info'] = [i.copy() for i in STATUS_LIST if i['machine_name'] == row.status][0]
        d['status_info']['next_step_url'] = d['status_info']['next_step_url'].format(row.id)
        if row.last_work_status:
            d['last_work_status'] = unicode(row.last_work_status)
        all_sessions.append(d)

    resp['objects'] = all_sessions
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@admin.route('/dump-entity-map/')
@login_required
@check_sessions()
def entity_map_dump():

    session_id = flask_session['session_id']
    outp = StringIO()
    copy = """ 
        COPY (
          SELECT 
            e.entity_id, 
            r.* 
          FROM \"raw_{0}\" AS r
          LEFT JOIN \"entity_{0}\" AS e
            ON r.record_id = e.record_id
          WHERE e.clustered = TRUE
          ORDER BY e.entity_id NULLS LAST
        ) TO STDOUT WITH CSV HEADER DELIMITER ','
    """.format(session_id)
    engine = db_session.bind
    conn = engine.raw_connection()
    curs = conn.cursor()
    curs.copy_expert(copy, outp)
    outp.seek(0)
    resp = make_response(outp.getvalue())
    resp.headers['Content-Type'] = 'text/csv'
    filedate = datetime.now().strftime('%Y-%m-%d')
    resp.headers['Content-Disposition'] = 'attachment; filename=entity_map_{0}.csv'.format(filedate)
    return resp

@admin.route('/rewind/')
@login_required
@check_sessions()
def rewind():
    session_id = flask_session['session_id']
    step = request.args.get('step')
    threshold = request.args.get('threshold')
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    dedupe_session.processing = True
    db_session.add(dedupe_session)
    db_session.commit()
    if step == 'first':
        reDedupeRaw.delay(session_id, threshold=float(threshold))
    if step == 'second':
        reDedupeCanon.delay(session_id, threshold=float(threshold))
    response = make_response(json.dumps({'status': 'ok'}))
    response.headers['Content-Type'] = 'application/json'
    return response

@admin.route('/clear-error/')
@login_required
@check_sessions()
def clear_error():
    work_id = request.args['work_id']
    work = db_session.query(WorkTable).get(work_id)
    work.cleared = True
    db_session.add(work)
    db_session.commit()
    response = make_response(json.dumps({'status': 'ok'}))
    response.headers['Content-Type'] = 'application/json'
    return response

@admin.route('/add-bulk-training/', methods=['POST'])
@login_required
@check_sessions()
def add_bulk_training():
    session_id = flask_session['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    replace = request.form.get('replace', False)
    td = json.load(request.files['input_file'])
    if dedupe_session.training_data:
        if not replace:
            old_training = json.loads(dedupe_session.training_data)
            td['distinct'].extend([pair for pair in old_training['distinct']])
            td['match'].extend([pair for pair in old_training['match']])
    dedupe_session.training_data = json.dumps(td)
    db_session.add(dedupe_session)
    db_session.commit()
    r = {
        'status': 'ok', 
        'message': 'Added {0} distinct and {1} matches'\
                .format(len(td['distinct']), len(td['match']))
    }
    return redirect(url_for('admin.session_admin'))


@admin.route('/get-entity-records/', methods=['GET'])
@login_required
@check_sessions()
def get_entity_records():
    session_id = flask_session['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    field_names = set([f['field'] for f in json.loads(dedupe_session.field_defs)])
    fields = ', '.join(['r.{0}'.format(f) for f in field_names])
    entity_id = request.args.get('entity_id')
    sel = ''' 
        SELECT {0}
        FROM "raw_{1}" AS r
        JOIN "entity_{1}" AS e
          ON r.record_id = e.record_id
        WHERE e.entity_id = :entity_id
    '''.format(fields, session_id)
    engine = db_session.bind
    records = [dict(zip(r.keys(), r.values())) \
            for r in engine.execute(text(sel), entity_id=entity_id)]
    response = make_response(json.dumps({'status': 'ok', 'records': records}))
    response.headers['Content-Type'] = 'application/json'
    return response

@admin.route('/entity-browser/', methods=['POST', 'GET'])
@login_required
@check_sessions()
def entity_browser():
    session_id = flask_session['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    field_names = set([f['field'] for f in json.loads(dedupe_session.field_defs)])
    sel = ''' 
        SELECT * FROM "browser_{0}" LIMIT 100
    '''.format(session_id)
    if request.args.get('page'):
        page = int(request.args['page'])
        offset = (page - 1) * 100
        sel = '{0} OFFSET {1}'.format(sel, offset)
    engine = db_session.bind
    entities = list(engine.execute(sel))
    page_count = int(round(dedupe_session.entity_count, -2) / 100)
    return render_template('entity-browser.html', 
                           dedupe_session=dedupe_session,
                           entities=entities, 
                           fields=list(field_names), 
                           page_count=page_count)

@admin.route('/entity-detail/', methods=['POST', 'GET'])
@login_required
@check_sessions()
def entity_detail():
    session_id = flask_session['session_id']
    entity_id = request.args.get('entity_id')
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    model_fields = [f['field'] for f in json.loads(dedupe_session.field_defs)]
    sel = ''' 
      SELECT 
        e.entity_id,
        e.reviewer,
        e.date_added,
        e.last_update,
        e.match_type,
        e.target_record_id,
        e.confidence,
        r.*
      FROM "entity_{0}" AS e
      JOIN "raw_{0}" AS r
        ON e.record_id = r.record_id
      WHERE e.entity_id = :entity_id
    '''.format(session_id)
    engine = db_session.bind
    records = list(engine.execute(text(sel), entity_id=entity_id))
    meta = MetaData()
    raw_table = Table('raw_{0}'.format(session_id), meta, 
        autoload=True, autoload_with=engine)
    raw_fields = raw_table.columns.keys()
    entity_fields = [
        'reviewer', 
        'date_added', 
        'last_update', 
        'match_type', 
        'target_record_id', 
        'confidence'
    ]
    return render_template('entity-detail.html',
                           model_fields=model_fields,
                           raw_fields=raw_fields,
                           records=records,
                           entity_fields=entity_fields,
                           entity_id=entity_id,
                           dedupe_session=dedupe_session)

@admin.route('/edit-model/', methods=['POST', 'GET'])
@login_required
@check_sessions()
def edit_model():
    session_id = flask_session['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    field_types = [
        "String",
        "Address",
        "Price",
        "ShortString",
        "Text",
        "LatLong",
        "Set",
        "Exact",
        "Exists",
        "Categorical",
    ]
    if request.method == 'POST':
        field_defs = []
        form = {}
        for k in request.form.keys():
            if k != 'csrf_token':
                form[k] = request.form.getlist(k)
        ftypes = sorted(form.items())
        for k,g in groupby(ftypes, key=lambda x: x[0].rsplit('_', 1)[0]):
            vals = list(g)
            fs = []
            for field, val in vals:
                fs.extend([{'field': k, 'type': val[i]} \
                    for i in range(len(val)) if field.endswith('type')])
            field_defs.extend(fs)
        engine = db_session.bind
        with engine.begin() as conn:
            conn.execute(text(''' 
                UPDATE dedupe_session SET
                    field_defs = :field_defs
                WHERE id = :id
            '''), field_defs=json.dumps(field_defs), id=dedupe_session.id)
        flash('Model updated!')
        dedupe_session.processing = True
        db_session.add(dedupe_session)
        db_session.commit()
        trainDedupe.delay(session_id)
        return redirect(url_for('admin.index'))
    return render_template('edit-model.html',
                           dedupe_session=dedupe_session,
                           model=json.loads(dedupe_session.field_defs),
                           field_types=field_types)

