from flask import request, make_response, render_template, \
    session as flask_session, redirect, url_for, send_from_directory, jsonify,\
    Blueprint, current_app
from flask_login import current_user
from werkzeug import secure_filename
import time
from datetime import datetime, timedelta
import json
import re
import os
import copy
import time
from dedupe.serializer import _to_json, dedupe_decoder
import dedupe
from api.utils.delayed_tasks import dedupeRaw
from api.utils.dedupe_functions import DedupeFileError
from api.utils.db_functions import writeRawTable
from api.utils.helpers import makeDataDict, getDistinct, slugify
from api.models import DedupeSession, User, Group
from api.database import app_session as db_session
from api.auth import check_roles, csrf, login_required
from api.database import app_session
from sqlalchemy.exc import OperationalError, NoSuchTableError
from sqlalchemy import Table
from cStringIO import StringIO
import csv
from redis import Redis
from api.queue import DelayedResult
from uuid import uuid4
import collections

redis = Redis()

ALLOWED_EXTENSIONS = set(['csv', 'xls', 'xlsx'])

trainer = Blueprint('trainer', __name__)

db_path = os.path.abspath(os.path.dirname(__file__))

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@trainer.route('/train-start/', methods=['GET', 'POST'])
@login_required
def train():
    user = db_session.query(User).get(flask_session['user_id'])
    status_code = 200
    error = None
    flask_session['session_id'] = unicode(uuid4())
    session_values = [
        'dedupe_start',
        'sample',
        'sample_key',
        'fieldnames',
        'session_name',
        'last_interaction',
        'training_data',
        'current_pair',
        'field_defs',
        'counter',
    ]
    for k in session_values:
        try:
            del flask_session[k]
        except KeyError:
            pass
    if request.method == 'POST':
        f = request.files['input_file']
        conn_string = current_app.config['DB_CONN']
        table_name = 'raw_%s' % flask_session['session_id']
        primary_key = 'record_id'
        fieldnames = writeRawTable(session_id=flask_session['session_id'],
            filename=f.filename,
            file_obj=f)
        session_name = f.filename
        flask_session['last_interaction'] = datetime.now()
        flask_session['fieldnames'] = fieldnames
        old = datetime.now() - timedelta(seconds=60 * 30)
        if flask_session['last_interaction'] < old:
            del flask_session['deduper']
        flask_session['session_name'] = session_name
        # Add this session to the user's first group
        # Will need to revisit this when there are more groups
        group = user.groups[0]
        sess = DedupeSession(
            id=flask_session['session_id'], 
            name=session_name,
            group=group,
            conn_string=conn_string,
            table_name=table_name, 
            status='dataset uploaded')
        db_session.add(sess)
        db_session.commit()
        return redirect(url_for('trainer.select_fields'))
    return make_response(render_template('upload.html', error=error, user=user), status_code)

@trainer.route('/select_fields/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def select_fields():
    status_code = 200
    error = None
    session_name = flask_session['session_name']
    flask_session['last_interaction'] = datetime.now()
    fields = flask_session.get('fieldnames')
    if request.method == 'POST':
        field_list = [r for r in request.form if r != 'csrf_token']
        flask_session['field_list'] = field_list
        if field_list:
            return redirect(url_for('trainer.select_field_types'))
        else:
            error = 'You must select at least one field to compare on.'
            status_code = 500
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('select_fields.html', error=error, fields=fields, user=user)

@trainer.route('/select_field_types/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def select_field_types():
    user = db_session.query(User).get(flask_session['user_id'])
    sess = db_session.query(DedupeSession).get(flask_session['session_id'])
    field_list = flask_session['field_list']
    if request.method == 'POST':
        field_dict = {}
        for k,v in request.form.items():
            if k != 'csrf_token':
                field_name, form_field = k.split('_')
                if not field_dict.get(field_name):
                    field_dict[field_name] = {}
                if form_field == 'missing':
                    field_dict[field_name]['has_missing'] = True
                if form_field == 'type': 
                    field_dict[field_name]['type'] = v
        field_defs = []
        for k,v in field_dict.items():
            slug = slugify(unicode(k))
            d = {'field': slug}
            if v['type'] == 'Categorical':
                v['categories'] = getDistinct(slug,sess.id)
            d.update(v)
            field_defs.append(d)
        sess = db_session.query(DedupeSession).get(flask_session['session_id'])
        sess.field_defs = json.dumps(field_defs)
        db_session.add(sess)
        db_session.commit()
        return redirect(url_for('trainer.training_run'))
    return render_template('select_field_types.html', user=user, field_list=field_list)

@trainer.route('/training_run/')
@login_required
@check_roles(roles=['admin'])
def training_run():
    if request.args.get('session_id'):
        session_id = request.args['session_id']
        try:
            del flask_session['counter']
        except KeyError:
            pass
    elif flask_session.get('session_id'):
        session_id = flask_session['session_id']
    else:
        return redirect(url_for('trainer.train_start'))
    user = db_session.query(User).get(flask_session['user_id'])
    sess = db_session.query(DedupeSession)\
        .filter(DedupeSession.group.has(
            Group.id.in_([i.id for i in user.groups])))\
        .filter(DedupeSession.id == session_id)\
        .first()
    if not sess:
        error = "You don't have access to session '%s'" % session_id
        status_code = 401
    else:
        error = None
        status_code = 200
        field_defs = json.loads(sess.field_defs)
        deduper = dedupe.Dedupe(field_defs)
        data_d = makeDataDict(sess.id, sample=True)
        deduper.sample(data_d, sample_size=5000, blocked_proportion=1)
        flask_session['deduper'] = deduper
    return make_response(render_template('training_run.html', user=user, error=error), status_code)

@trainer.route('/get-pair/')
@login_required
@check_roles(roles=['admin'])
def get_pair():
    deduper = flask_session['deduper']
    flask_session['last_interaction'] = datetime.now()
    fields = [f[0] for f in deduper.data_model.field_comparators]
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
    if flask_session.get('counter'):
        counter = flask_session['counter']
    else:
        counter = {'yes': 0, 'no': 0, 'unsure': 0}
        sess = db_session.query(DedupeSession).get(flask_session['session_id'])
        sess.status = 'training started'
        db_session.add(sess)
        db_session.commit()
    if flask_session.get('training_data'):
        labels = flask_session['training_data']
    else:
        labels = {'distinct' : [], 'match' : []}
    deduper = flask_session['deduper']
    if action == 'yes':
        current_pair = flask_session['current_pair']
        labels['match'].append(current_pair)
        counter['yes'] += 1
        resp = {'counter': counter}
    elif action == 'no':
        current_pair = flask_session['current_pair']
        labels['distinct'].append(current_pair)
        counter['no'] += 1
        resp = {'counter': counter}
    elif action == 'finish':
        training_data = flask_session['training_data']
        sess = db_session.query(DedupeSession).get(flask_session['session_id'])
        if sess.training_data:
            td = json.loads(sess.training_data)
            td['distinct'].extend(training_data['distinct'])
            td['match'].extend(training_data['match'])
            sess.training_data = json.dumps(td, default=_to_json)
        else:
            sess.training_data = json.dumps(training_data, default=_to_json)
        sess.status = 'training complete'
        db_session.add(sess)
        db_session.commit()
        sample = deduper.data_sample
        rv = dedupeRaw.delay(flask_session['session_id'], sample)
        flask_session['deduper_key'] = rv.key
        resp = {'finished': True}
        flask_session['dedupe_start'] = time.time()
    else:
        counter['unsure'] += 1
        flask_session['counter'] = counter
        resp = {'counter': counter}
    deduper.markPairs(labels)
    flask_session['training_data'] = labels
    flask_session['counter'] = counter
    if resp.get('finished'):
        del flask_session['deduper']
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/dedupe_finished/')
@login_required
@check_roles(roles=['admin'])
def dedupe_finished():
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template("dedupe_finished.html", user=user)

@trainer.route('/dedupe_finished/checkscore.php')
def pong_score():
    print request.args
    resp = {}
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/about/')
def about():
    return render_template("about.html")

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
    if flask_session.get('adjust_start'):
        start = flask_session['adjust_start']
        end = time.time()
    return jsonify(ready=True, result=rv.return_value)

