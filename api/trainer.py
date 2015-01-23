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
from api.utils.helpers import getDistinct, slugify, STATUS_LIST
from api.models import DedupeSession, User, Group
from api.database import app_session as db_session, init_engine
from api.auth import check_roles, csrf, login_required, check_sessions
from sqlalchemy.exc import OperationalError, NoSuchTableError
from sqlalchemy import Table, MetaData
from cStringIO import StringIO
from redis import Redis
from api.queue import DelayedResult
from uuid import uuid4
import collections
from csvkit.unicsv import UnicodeCSVReader
from csvkit import convert

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
    fieldnames = [slugify(unicode(i)) for i in u.next().strip('\r\n').split(',')]
    flask_session['fieldnames'] = fieldnames
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
    db_session.add(sess)
    db_session.commit()
    u.seek(0)
    with open('/tmp/%s_raw.csv' % session_id, 'wb') as s:
        s.write(u.getvalue())
    del u
    flask_session['init_key'] = initializeSession.delay(session_id)
    flask_session['session_id'] = session_id
    return jsonify(ready=True)

@trainer.route('/get-init-status/<init_key>/')
@login_required
def get_init_status(init_key): # pragma: no cover
    rv = DelayedResult(init_key)
    if rv.return_value is None:
        return jsonify(ready=False)
    redis.delete(init_key)
    del flask_session['init_key']
    return jsonify(ready=True, **rv.return_value)

@trainer.route('/train-start/', methods=['GET'])
@login_required
def train():
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
    return make_response(render_template('dedupe_session/upload.html', error=error))

@trainer.route('/select-fields/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
@check_sessions()
def select_fields():
    status_code = 200
    error = None
    dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])
    fields = flask_session.get('fieldnames')
    meta = MetaData()
    engine = db_session.bind
    raw = Table('raw_{0}'.format(flask_session['session_id']), meta, 
        autoload=True, autoload_with=engine, keep_existing=True)
    fields = [r for r in raw.columns.keys() if r != 'record_id']
    flask_session['fieldnames'] = fields
    
    if request.method == 'POST':
        field_list = [r for r in request.form if r != 'csrf_token']
        flask_session['field_list'] = field_list
        if field_list:
            return redirect(url_for('trainer.select_field_types'))
        else:
            error = 'You must select at least one field to compare on.'
            status_code = 400

    return render_template('dedupe_session/select_fields.html', error=error, fields=fields, dedupe_session=dedupe_session)

@trainer.route('/select-field-types/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def select_field_types():
    dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])
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
            has_missing = False
            for ftype, val in vals:
                if ftype == '{0}_missing'.format(k):
                    has_missing = True
            fs = []
            for field, val in vals:
                fs.extend([{'field': k, 'type': val[i]} \
                    for i in range(len(val)) if field.endswith('type')])
            for f in fs:
                if has_missing:
                    f.update({'has_missing': True})
            field_defs.extend(fs)
        dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])
        dedupe_session.field_defs = json.dumps(field_defs)
        dedupe_session.status = 'model defined'
        db_session.add(dedupe_session)
        db_session.commit()
        flask_session['init_key'] = initializeModel.delay(dedupe_session.id).key
        return redirect(url_for('trainer.training_run'))
    return render_template('dedupe_session/select_field_types.html', field_list=field_list, dedupe_session=dedupe_session)

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
    error = None
    status_code = 200
    field_defs = json.loads(dedupe_session.field_defs)
    init_status = 'processing'
    if dedupe_session.sample:
        sample = cPickle.loads(dedupe_session.sample)
        deduper = dedupe.Dedupe(field_defs, data_sample=sample)
        flask_session['deduper'] = deduper
        init_status = 'finished'
    return make_response(render_template(
                            'dedupe_session/training_run.html', 
                            error=error, 
                            init_status=init_status, 
                            dedupe_session=dedupe_session), status_code)

@trainer.route('/get-pair/')
@login_required
@check_roles(roles=['admin'])
def get_pair():
    deduper = flask_session['deduper']
    flask_session['last_interaction'] = datetime.now()
    fields = list(set([f[0] for f in deduper.data_model.field_comparators]))
    #fields = deduper.data_model.field_comparators
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
    l_d = {}
    r_d = {}
    for k,v in left.items():
        if 'Price' in fds[k]:
            l_d[k] = float(v)
        else:
            l_d[k] = v
    for k,v in right.items():
        if 'Price' in fds[k]:
            r_d[k] = float(v)
        else:
            r_d[k] = v
    current_pair = [l_d, r_d]
    if sess.training_data:
        labels = json.loads(sess.training_data)
    else:
        labels = {'distinct' : [], 'match' : []}
    if action == 'yes':
        labels['match'].append(current_pair)
        counter['yes'] += 1
        resp = {'counter': counter}
    elif action == 'no':
        labels['distinct'].append(current_pair)
        counter['no'] += 1
        resp = {'counter': counter}
    elif action == 'finish':
        rv = dedupeRaw.delay(flask_session['session_id'])
        flask_session['deduper_key'] = rv.key
        resp = {'finished': True}
        flask_session['dedupe_start'] = time.time()
    else:
        counter['unsure'] += 1
        flask_session['counter'] = counter
        resp = {'counter': counter}
    sess.training_data = json.dumps(labels, default=_to_json)
    db_session.add(sess)
    db_session.commit()
    deduper.markPairs(labels)
    if resp.get('finished'):
        del flask_session['deduper']
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/working/')
@login_required
@check_roles(roles=['admin'])
def working():
    key = flask_session.get('deduper_key')
    if key is None:
        return jsonify(ready=False)
    rv = DelayedResult(key)
    if rv.return_value is None:
        return jsonify(ready=False)
    redis.delete(key)
    del flask_session['deduper_key']
    if flask_session.get('dedupe_start'):
        start = flask_session['dedupe_start']
        end = time.time()
    return jsonify(ready=True, result=rv.return_value)

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
