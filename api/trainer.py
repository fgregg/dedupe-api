from flask import request, make_response, render_template, \
    session as flask_session, redirect, url_for, send_from_directory, jsonify,\
    Blueprint, current_app
from flask_login import current_user
from werkzeug import secure_filename
import time
from datetime import datetime, timedelta
import json
import cPickle
import re
import os
import copy
import time
from itertools import groupby
from dedupe.serializer import _to_json, dedupe_decoder
import dedupe
from api.utils.delayed_tasks import dedupeRaw, initializeSession, \
    initializeModel
from api.utils.db_functions import writeRawTable
from api.utils.helpers import getDistinct, slugify, STATUS_LIST, \
    updateTraining, convertTraining
from api.models import DedupeSession, User, Group, WorkTable
from api.database import app_session as db_session, init_engine
from api.auth import check_roles, csrf, login_required, check_sessions
from sqlalchemy.exc import OperationalError, NoSuchTableError
from sqlalchemy import Table, MetaData, text
from cStringIO import StringIO
from redis import Redis
from uuid import uuid4
import collections
from csvkit.unicsv import UnicodeCSVReader
from csvkit import convert
from unidecode import unidecode
from operator import itemgetter

redis = Redis()

ALLOWED_EXTENSIONS = set(['csv', 'xls', 'xlsx'])

trainer = Blueprint('trainer', __name__)

db_path = os.path.abspath(os.path.dirname(__file__))

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@trainer.route('/upload/', methods=['POST'])
@login_required
def upload():
    session_id = unicode(uuid4())
    f = request.files['input_file']
    flask_session['session_name'] = f.filename
    file_type = f.filename.rsplit('.')[1]
    u = StringIO(f.read())
    u.seek(0)
    if file_type != 'csv': # pragma: no cover
        file_format = convert.guess_format(flask_session['session_name'])
        u = StringIO(convert.convert(u, file_format))
    reader = UnicodeCSVReader(u)
    fieldnames = [slugify(unidecode(unicode(i))) for i in reader.next()]
    sample_values = [[] for i in range(len(fieldnames))]
    v = 0
    while v < 10:
        line = reader.next()
        for i in range(len(fieldnames)):
            sample_values[i].append(unidecode(unicode(line[i])))
        v += 1
    flask_session['fieldnames'] = fieldnames
    flask_session['sample_values'] = sample_values
    user_id = flask_session['user_id']
    user = db_session.query(User).get(user_id)
    group = user.groups[0]
    sess = DedupeSession(
        id=session_id,
        name=request.form.get('name'),
        description=request.form.get('description'),
        filename=f.filename,
        group=group,
        status=STATUS_LIST[0]['machine_name'])
    u.seek(0)
    with open('/tmp/%s_raw.csv' % session_id, 'wb') as s:
        s.write(u.getvalue())
    del u
    del reader
    sess.processing = True
    db_session.add(sess)
    db_session.commit()
    initializeSession.delay(session_id)
    flask_session['session_id'] = session_id
    return jsonify(ready=True, session_id=session_id)

@trainer.route('/new-session/', methods=['GET'])
@login_required
def new_session():
    error = None
    session_values = [
        'sample',
        'fieldnames',
        'session_name',
        'training_data',
        'current_pair',
        'counter',
        'deduper',
    ]
    for k in session_values:
        try:
            del flask_session[k]
        except KeyError:
            pass
    return make_response(render_template('dedupe_session/new-session.html', error=error))

@trainer.route('/select-fields/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
@check_sessions()
def select_fields():
    status_code = 200
    errors = []
    dedupe_session = db_session.query(DedupeSession.name, DedupeSession.id)\
            .filter(DedupeSession.id == flask_session['session_id'])\
            .first()
    fields = flask_session.get('fieldnames')
    sample_values = flask_session.get('sample_values')
    # If the fields are not in the session, that means that the user has come
    # here directly from the home page. We'll try to load them from the raw
    # table in the database but if that does not exist yet (which is possible)
    # then we'll redirect them to the home page.
    if request.args.get('session_id'):
        session_values = [
            'sample',
            'fieldnames',
            'session_name',
            'training_data',
            'current_pair',
            'counter',
            'deduper',
        ]
        for k in session_values:
            try:
                del flask_session[k]
            except KeyError:
                pass
    if not fields:
        meta = MetaData()
        engine = db_session.bind
        try:
            raw = Table('raw_{0}'.format(flask_session['session_id']), meta, 
                autoload=True, autoload_with=engine, keep_existing=True)
            rows = list(engine.execute(''' 
                SELECT * FROM "raw_{0}" LIMIT 10
                '''.format(flask_session['session_id'])))
            fields = [k for k in rows[0].keys() if k != 'record_id']
            sample_values = [[] for idx, val in enumerate(fields) if val != 'record_id']
            v = 0
            while v < 10:
                row = rows[v]
                for idx, field in enumerate(fields):
                    sample_values[idx].append(getattr(row, field))
                v += 1
            flask_session['sample_values'] = sample_values
            flask_session['fieldnames'] = fields
        except NoSuchTableError:
            return redirect(url_for('admin.index'))
    errors = db_session.query(WorkTable)\
            .filter(WorkTable.session_id == dedupe_session.id)\
            .filter(WorkTable.cleared == False)\
            .all()
    if request.method == 'POST':
        field_list = [r for r in request.form if r != 'csrf_token']
        flask_session['field_list'] = field_list
        if field_list:
            return redirect(url_for('trainer.select_field_types'))
        else:
            errors = ['You must select at least one field to compare on.']
            status_code = 400

    return render_template('dedupe_session/select_fields.html', 
                            errors=errors, 
                            fields=fields, 
                            sample_values=sample_values,
                            dedupe_session=dedupe_session)

@trainer.route('/select-field-types/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def select_field_types():
    dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])
    errors = db_session.query(WorkTable)\
            .filter(WorkTable.session_id == dedupe_session.id)\
            .filter(WorkTable.cleared == False)\
            .all()
    errors = [e.value for e in errors]
    field_list = flask_session['field_list']
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
        if not errors:
            dedupe_session.processing = True
            db_session.add(dedupe_session)
            db_session.commit()
            initializeModel.delay(dedupe_session.id)
        return redirect(url_for('trainer.training_run'))
    return render_template('dedupe_session/select_field_types.html', 
                           field_list=field_list, 
                           dedupe_session=dedupe_session,
                           errors=errors)

@trainer.route('/training-run/')
@login_required
@check_roles(roles=['admin'])
@check_sessions()
def training_run():
    dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])
    if dedupe_session.training_data:
        td = json.loads(dedupe_session.training_data)
        flask_session['counter'] = {
                'yes': len(td['match']),
                'no': len(td['distinct']),
                'unsure': 0
            }
    else:
        flask_session['counter'] = {
            'yes': 0,
            'no': 0 ,
            'unsure': 0,
        }
    errors = db_session.query(WorkTable)\
            .filter(WorkTable.session_id == dedupe_session.id)\
            .filter(WorkTable.cleared == False)\
            .all()
    if not errors:
        status_code = 200
        time.sleep(1)
        db_session.refresh(dedupe_session)
        field_defs = json.loads(dedupe_session.field_defs)
        if not dedupe_session.processing and dedupe_session.sample:
            sample = cPickle.loads(dedupe_session.sample)
            if sample[0][0].get('record_id'):
                deduper = dedupe.Dedupe(field_defs, data_sample=sample)
                flask_session['deduper'] = deduper
            else:
                dedupe_session.processing = True
                db_session.add(dedupe_session)
                db_session.commit()
                initializeModel.delay(dedupe_session.id)
    else:
        status_code = 500
    time.sleep(0.5)
    db_session.refresh(dedupe_session)
    return make_response(render_template(
                            'dedupe_session/training_run.html', 
                            errors=errors, 
                            dedupe_session=dedupe_session), status_code)

@trainer.route('/get-pair/')
@login_required
@check_roles(roles=['admin'])
def get_pair():
    deduper = flask_session['deduper']
    flask_session['last_interaction'] = datetime.now()
    fields = list(set([f[0] for f in deduper.data_model.field_comparators]))
    record_pair = deduper.uncertainPairs()[0]
    flask_session['current_pair'] = record_pair
    data = []
    left, right = record_pair
    for field in fields:
        d = {
            'field': field,
            'left': left[field],
            'right': right[field],
        }
        data.append(d)
    resp = make_response(json.dumps(data))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/mark-pair/')
@login_required
@check_roles(roles=['admin'])
def mark_pair():
    action = request.args['action']
    flask_session['last_interaction'] = datetime.now()
    counter = flask_session.get('counter')
    sess = db_session.query(DedupeSession).get(flask_session['session_id'])
    db_session.refresh(sess, ['training_data'])
    deduper = flask_session['deduper']

    # Attempt to cast the training input appropriately
    # TODO: Figure out LatLong type
    field_defs = json.loads(sess.field_defs)
    fds = {}
    for fd in field_defs:
        try:
            fds[fd['field']].append(fd['type'])
        except KeyError:
            fds[fd['field']] = [fd['type']]
    current_pair = flask_session['current_pair']
    left, right = current_pair
    record_ids = [left['record_id'], right['record_id']]
    if sess.training_data:
        labels = json.loads(sess.training_data)
    else:
        labels = {'distinct' : [], 'match' : []}
    if action == 'yes':
        updateTraining(sess.id, match_ids=record_ids)
        counter['yes'] += 1
        resp = {'counter': counter}
    elif action == 'no':
        updateTraining(sess.id, distinct_ids=record_ids)
        counter['no'] += 1
        resp = {'counter': counter}
    elif action == 'finish':
        sess.processing = True
        db_session.add(sess)
        db_session.commit()
        dedupeRaw.delay(flask_session['session_id'])
        resp = {'finished': True}
        flask_session['dedupe_start'] = time.time()
    else:
        counter['unsure'] += 1
        flask_session['counter'] = counter
        resp = {'counter': counter}
    db_session.refresh(sess, ['training_data'])
    labels = json.loads(sess.training_data)
    try:
        deduper.markPairs(labels)
    except TypeError:
        td = convertTraining(field_defs, labels)
        deduper.markPairs(td)
    if resp.get('finished'):
        del flask_session['deduper']
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/upload_formats/')
def upload_formats(): # pragma: no cover
    user_id = flask_session.get('user_id')
    user = None
    if user_id:
        user = db_session.query(User).get(flask_session['user_id'])
    return render_template("upload-formats.html", user=user)

@trainer.route('/about/')
def about(): # pragma: no cover
    user_id = flask_session.get('user_id')
    user = None
    if user_id:
        user = db_session.query(User).get(flask_session['user_id'])
    return render_template("about.html", user=user)
