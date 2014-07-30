from flask import request, make_response, render_template, \
    session as flask_session, redirect, url_for, send_from_directory, jsonify,\
    Blueprint, current_app
from api.auth import login_required
from flask_login import current_user
from werkzeug import secure_filename
import time
from datetime import datetime, timedelta
import json
import requests
import re
import os
import copy
import time
from dedupe import AsciiDammit
from dedupe.serializer import _to_json, dedupe_decoder
import dedupe
from api.dedupe_utils import dedupeit, static_dedupeit, DedupeFileIO,\
    DedupeFileError
from cStringIO import StringIO
import csv
from redis import Redis
from api.queue import DelayedResult
from uuid import uuid4
import collections
from api.models import DedupeSession, User
from api.database import session as db_session

redis = Redis()

ALLOWED_EXTENSIONS = set(['csv', 'xls', 'xlsx'])
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')

trainer = Blueprint('trainer', __name__)

db_path = os.path.abspath(os.path.dirname(__file__))

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@trainer.route('/training/', methods=['GET', 'POST'])
@login_required
def index():
    status_code = 200
    error = None
    api_key = request.args.get('api_key')
    if api_key is not None:
        flask_session['api_key'] = api_key
        flask_session['session_key'] = unicode(uuid4())
    if request.method == 'POST':
        f = request.files['input_file']
        if f and allowed_file(f.filename):
            fname = secure_filename(str(time.time()) + "_" + f.filename)
            file_path = os.path.abspath(os.path.join(UPLOAD_FOLDER, fname))
            f.save(file_path)
            try:
                # Need a better way of getting the connection string
                conn_string = current_app.config['DB_CONN']
                inp_file = DedupeFileIO(
                    conn_string=conn_string,
                    session_key=flask_session['session_key'],
                    filename=fname,
                    file_obj=open(file_path, 'rb'))
                flask_session['last_interaction'] = datetime.now()
                flask_session['deduper'] = {'file_io': inp_file}
                old = datetime.now() - timedelta(seconds=60 * 30)
                if flask_session['last_interaction'] < old:
                    del flask_session['deduper']
                flask_session['filename'] = fname
                flask_session['file_path'] = file_path
                api_user = db_session.query(User).get(flask_session['api_key'])
                sess = DedupeSession(
                    id=flask_session['session_key'], 
                    name=fname, 
                    user=api_user)
                db_session.add(sess)
                db_session.commit()
                return redirect(url_for('trainer.select_fields'))
            except DedupeFileError as e:
                error = e.message
                status_code = 500
        else:
            error = 'Error uploading file. Did you forget to select one?'
            status_code = 500
    return make_response(render_app_template('training.html', error=error), status_code)

def preProcess(column):
    column = AsciiDammit.asciiDammit(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def readData(inp):
    data = {}
    reader = csv.DictReader(StringIO(inp))
    for i, row in enumerate(reader):
        clean_row = [(k, preProcess(v)) for (k,v) in row.items()]
        row_id = i
        data[row_id] = dedupe.core.frozendict(clean_row)
    return data

@trainer.route('/select_fields/', methods=['GET', 'POST'])
@login_required
def select_fields():
    status_code = 200
    error = None
    if not flask_session.get('deduper'):
        return redirect(url_for('trainer.index'))
    else:
        filename = flask_session['filename']
        flask_session['last_interaction'] = datetime.now()
        fields = flask_session['deduper']['file_io'].fieldnames
        data_d = flask_session['deduper']['file_io'].data_d
        if request.method == 'POST':
            field_list = [r for r in request.form if r != 'csrf_token']
            if field_list:
                training = True
                field_defs = {}
                for field in field_list:
                    field_defs[field] = {'type': 'String'}
                flask_session['deduper']['field_defs'] = copy.deepcopy(field_defs)
                start = time.time()
                sess = db_session.query(DedupeSession).get(flask_session['session_key'])
                sess.field_defs = json.dumps(field_defs)
                db_session.add(sess)
                db_session.commit()
                deduper = dedupe.Dedupe(field_defs)
                deduper.sample(data_d, 150000)
                flask_session['deduper']['deduper'] = deduper
                end = time.time()
                return redirect(url_for('trainer.training_run'))
            else:
                error = 'You must select at least one field to compare on.'
                status_code = 500
        return render_app_template('select_fields.html', error=error, fields=fields, filename=filename)

@trainer.route('/training_run/')
@login_required
def training_run():
    if not flask_session.get('deduper'):
        return redirect(url_for('trainer.index'))
    else:
        filename = flask_session['filename']
        return render_app_template('training_run.html', filename=filename)

@trainer.route('/get-pair/')
@login_required
def get_pair():
    if not flask_session.get('deduper'):
        return make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        deduper = flask_session['deduper']['deduper']
        filename = flask_session['filename']
        flask_session['last_interaction'] = datetime.now()
        #fields = [f[0] for f in deduper.data_model.field_comparators]
        fields = deduper.data_model.field_comparators
        record_pair = deduper.uncertainPairs()[0]
        flask_session['deduper']['current_pair'] = record_pair
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
def mark_pair():
    if not flask_session.get('deduper'):
        return make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        action = request.args['action']
        flask_session['last_interaction'] = datetime.now()
        if flask_session['deduper'].get('counter'):
            counter = flask_session['deduper']['counter']
        else:
            counter = {'yes': 0, 'no': 0, 'unsure': 0}
        if flask_session['deduper'].get('training_data'):
            labels = flask_session['deduper']['training_data']
        else:
            labels = {'distinct' : [], 'match' : []}
        deduper = flask_session['deduper']['deduper']
        if action == 'yes':
            current_pair = flask_session['deduper']['current_pair']
            labels['match'].append(current_pair)
            counter['yes'] += 1
            resp = {'counter': counter}
        elif action == 'no':
            current_pair = flask_session['deduper']['current_pair']
            labels['distinct'].append(current_pair)
            counter['no'] += 1
            resp = {'counter': counter}
        elif action == 'finish':
            file_io = flask_session['deduper']['file_io']
            training_data = flask_session['deduper']['training_data']
            sess = db_session.query(DedupeSession).get(flask_session['session_key'])
            sess.training_data = json.dumps(training_data, default=_to_json)
            db_session.add(sess)
            db_session.commit()
            field_defs = flask_session['deduper']['field_defs']
            sample = deduper.data_sample
            args = {
                'field_defs': field_defs,
                'file_io': file_io,
                'data_sample': sample,
                'session_key': flask_session['session_key'],
                'api_key': flask_session['api_key'],
            }
            rv = dedupeit.delay(**args)
            flask_session['deduper_key'] = rv.key
            resp = {'finished': True}
            flask_session['dedupe_start'] = time.time()
        else:
            counter['unsure'] += 1
            flask_session['deduper']['counter'] = counter
            resp = {'counter': counter}
        deduper.markPairs(labels)
        flask_session['deduper']['training_data'] = labels
        flask_session['deduper']['counter'] = counter
        if resp.get('finished'):
            del flask_session['deduper']
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/dedupe_finished/')
@login_required
def dedupe_finished():
    return render_app_template("dedupe_finished.html")

@trainer.route('/adjust_threshold/')
@login_required
def adjust_threshold():
    filename = flask_session['filename']
    file_path = flask_session['file_path']
    start = filename.split('_')[0]
    settings_path = None
    for f in os.listdir(UPLOAD_FOLDER):
        if f.startswith(start) and f.endswith('.dedupe'):
            settings_path = os.path.join(UPLOAD_FOLDER, f)
    recall_weight = request.args.get('recall_weight')
    args = {
        'settings_path': settings_path,
        'file_path': file_path,
        'filename': filename,
        'recall_weight': recall_weight,
    }
    rv = static_dedupeit.delay(**args)
    flask_session['deduper_key'] = rv.key
    flask_session['adjust_start'] = time.time()
    resp = make_response(json.dumps({'adjusted': True}))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@trainer.route('/about/')
@login_required
def about():
  return render_app_template("about.html")

@trainer.route('/working/')
@login_required
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

@trainer.route('/upload_data/<path:filename>/')
@login_required
def upload_data(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = current_app.config
    return render_template(template, **kwargs)

