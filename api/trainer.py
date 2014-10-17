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
from api.utils.delayed_tasks import dedupeit
from api.utils.dedupe_functions import DedupeFileError
from api.utils.db_functions import writeRawTable
from api.utils.helpers import makeDataDict
from api.models import DedupeSession, User
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
    flask_session['session_key'] = unicode(uuid4())
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
        table_name = 'raw_%s' % flask_session['session_key']
        primary_key = 'record_id'
        fieldnames = writeRawTable(session_key=flask_session['session_key'],
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
            id=flask_session['session_key'], 
            name=session_name,
            group=group,
            conn_string=conn_string,
            table_name=table_name)
        db_session.add(sess)
        db_session.commit()
        return redirect(url_for('trainer.select_fields'))
    return make_response(render_app_template('upload.html', error=error, user=user), status_code)

# from sqlalchemy.ext.declarative import declarative_base

# @csrf.exempt
# @trainer.route('/fetch-tables/', methods=['POST'])
# @login_required
# @check_roles(roles=['admin'])
# def fetch_tables():
#     conn_string = request.form['conn_string']
#     engine = getEngine(conn_string)
#     Rebase = declarative_base()
#     resp = {
#         'status': 'ok',
#         'table_names': [],
#     }
#     try:
#         Rebase.metadata.reflect(engine)
#         resp['table_names'] = Rebase.metadata.tables.keys()
#     except OperationalError, e:
#         print e.message
#         resp['status'] = 'error'
#         resp['message'] = e.message
#     resp = make_response(json.dumps(resp))
#     resp.headers['Content-Type'] = 'application/json'
#     return resp

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
        if field_list:
            training = True
            field_defs = []
            for field in field_list:
                field_defs.append({'field': field, 'type': 'String'})
            sess = db_session.query(DedupeSession).get(flask_session['session_key'])
            sess.field_defs = json.dumps(field_defs)
            db_session.add(sess)
            db_session.commit()
            deduper = dedupe.Dedupe(field_defs)
            data_d = makeDataDict(sess.id, table_name=sess.table_name)
            deduper.sample(data_d, sample_size=5000, rand_p=0)
            flask_session['deduper'] = deduper
            return redirect(url_for('trainer.training_run'))
        else:
            error = 'You must select at least one field to compare on.'
            status_code = 500
    user = db_session.query(User).get(flask_session['user_id'])
    return render_app_template('select_fields.html', error=error, fields=fields, user=user)

@trainer.route('/training_run/')
@login_required
@check_roles(roles=['admin'])
def training_run():
    user = db_session.query(User).get(flask_session['user_id'])
    return render_app_template('training_run.html', user=user)

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
        sess = db_session.query(DedupeSession).get(flask_session['session_key'])
        sess.training_data = json.dumps(training_data, default=_to_json)
        db_session.add(sess)
        db_session.commit()
        sample = deduper.data_sample
        args = {
            'data_sample': sample,
            'session_key': flask_session['session_key'],
        }
        rv = dedupeit.delay(**args)
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
    return render_app_template("dedupe_finished.html", user=user)

@trainer.route('/dedupe_finished/checkscore.php')
def pong_score():
    print request.data
    resp = {}
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/about/')
def about():
    return render_app_template("about.html")

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

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = current_app.config
    return render_template(template, **kwargs)

