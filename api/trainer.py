from flask import request, make_response, render_template, \
    session as flask_session, redirect, url_for, send_from_directory, jsonify,\
    Blueprint, current_app, flash
from flask_login import current_user
from werkzeug import secure_filename
import time
from datetime import datetime, timedelta
import json
import pickle
import re
import os
import copy
import time
from itertools import groupby
from dedupe.serializer import _to_json, dedupe_decoder, _from_json
import dedupe
from api.utils.delayed_tasks import dedupeRaw, initializeSession, \
    initializeModel
from api.utils.db_functions import writeRawTable, updateTraining, \
    readTraining, saveTraining
from api.utils.helpers import getDistinct, slugify, STATUS_LIST, tupleizeTraining
from api.models import DedupeSession, User, Group, WorkTable
from api.database import app_session as db_session, init_engine
from api.auth import check_roles, csrf, login_required, check_sessions
from api.exceptions import ImportFailed
from sqlalchemy.exc import OperationalError, NoSuchTableError
from sqlalchemy import Table, MetaData, text
from io import StringIO, BytesIO
from redis import Redis
from uuid import uuid4
import collections
import csv
from csvkit import convert
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
    session_id = str(uuid4())
    f = request.files['input_file']
    flask_session['session_name'] = f.filename
    file_type = f.filename.rsplit('.')[1]
    u = StringIO(f.read().decode('utf-8'))
    u.seek(0)
    if file_type != 'csv': # pragma: no cover
        file_format = convert.guess_format(flask_session['session_name'])
        u = StringIO(convert.convert(u, file_format).decode('utf-8'))
    reader = csv.reader(u)
    fieldnames = [slugify(str(i)) for i in next(reader)]
    sample_values = [[] for i in range(len(fieldnames))]
    v = 0
    while v < 10:
        line = next(reader)
        for i in range(len(fieldnames)):
            sample_values[i].append(str(line[i]))
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
    with open('/tmp/%s_raw.csv' % session_id, 'w', encoding='utf-8') as s:
        s.write(u.getvalue())
    del u
    del reader
    sess.processing = True
    db_session.add(sess)
    db_session.commit()
    initializeSession.delay(session_id, fieldnames)
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
    error = db_session.query(WorkTable)\
            .filter(WorkTable.session_id == dedupe_session.id)\
            .filter(WorkTable.cleared == False)\
            .first()
    if error:
        raise ImportFailed(error.return_value)
    if request.method == 'POST':
        field_list = [r for r in request.form if r != 'csrf_token']
        flask_session['field_list'] = field_list
        if field_list:
            return redirect(url_for('trainer.select_field_types'))
        else:
            flash('You must select at least one field to compare on.', 'danger')

    return render_template('dedupe_session/select_fields.html',
                            fields=fields, 
                            sample_values=sample_values,
                            dedupe_session=dedupe_session)

@trainer.route('/select-field-types/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def select_field_types():
    dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])
    error = db_session.query(WorkTable)\
            .filter(WorkTable.session_id == dedupe_session.id)\
            .filter(WorkTable.cleared == False)\
            .first()
    if error:
        raise ImportFailed(error.return_value)
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
        if not error:
            dedupe_session.processing = True
            db_session.add(dedupe_session)
            db_session.commit()
            initializeModel.delay(dedupe_session.id)
        return redirect(url_for('trainer.processing'))
    return render_template('dedupe_session/select_field_types.html', 
                           field_list=field_list, 
                           dedupe_session=dedupe_session)

@trainer.route('/processing/')
@login_required
@check_roles(roles=['admin'])
@check_sessions()
def processing():
    session_id = flask_session['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    errors = db_session.query(WorkTable)\
            .filter(WorkTable.session_id == dedupe_session.id)\
            .filter(WorkTable.cleared == False)\
            .all()
    if not errors:
        status_code = 200
        db_session.refresh(dedupe_session)
        field_defs = json.loads(dedupe_session.field_defs.decode('utf-8'))
        if not dedupe_session.processing and dedupe_session.sample:
            sample = pickle.loads(dedupe_session.sample)
            deduper = dedupe.Dedupe(field_defs, data_sample=sample)
            flask_session['deduper'] = deduper
            return redirect(url_for('trainer.training_run'))
    return render_template('dedupe_session/processing.html',
                           errors=errors,
                           dedupe_session=dedupe_session)
    

@trainer.route('/training-run/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
@check_sessions()
def training_run():
    session_id = flask_session['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    if flask_session.get('deduper') is None:
        field_defs = json.loads(dedupe_session.field_defs.decode('utf-8'))
        if not dedupe_session.processing and dedupe_session.sample:
            sample = pickle.loads(dedupe_session.sample)
            deduper = dedupe.Dedupe(field_defs, data_sample=sample)
            flask_session['deduper'] = deduper
        else:
            flash('Session not fully initialized', 'danger')
            return redirect(url_for('admin.index'))
    
    training_ids = request.args.get('training_ids')

    previous_ids, next_ids = getPrevNext(session_id, training_ids)
    
    deduper = flask_session['deduper']
    
    if request.method == 'POST':
        record_ids = request.form['training_ids'].split(',')
        decision = request.form['decision']
        if decision == 'yes':
            updateTraining(session_id, 
                           match_ids=record_ids,
                           trainer=current_user.name)
        
        elif decision == 'no':
            updateTraining(session_id, 
                           distinct_ids=record_ids,
                           trainer=current_user.name)
        elif decision == 'unsure':
            training = {'unsure': [flask_session['current_pair']]}
            saveTraining(session_id,
                         training,
                         trainer=current_user.name)
        elif decision == 'finished':
            dedupe_session.processing = True
            db_session.add(dedupe_session)
            db_session.commit()
            dedupeRaw.delay(session_id)
            return redirect(url_for('admin.index'))

        training_data = readTraining(session_id)
        deduper.markPairs(training_data)
    
    training_pair, training_ids = getTrainingPair(session_id, deduper, training_ids=training_ids)
    flask_session['current_pair'] = training_pair
    
    formatted = []
    left, right = training_pair
    fields = list(set([f[0] for f in deduper.data_model.field_comparators]))
    for field in fields:
        # Opportunity to calculate string distance here.
        d = {
            'field': field,
            'left': left[field],
            'right': right[field],
        }
        formatted.append(d)
    
    yes, no, unsure = getTrainingCounts(session_id)
    counter = {
            'yes': yes,
            'no': no,
            'unsure': unsure
        }
    error = db_session.query(WorkTable)\
            .filter(WorkTable.session_id == dedupe_session.id)\
            .filter(WorkTable.cleared == False)\
            .first()
    if error:
        flash(error.return_value, 'danger')
        abort(500)
    return render_template('dedupe_session/training_run.html', 
                            dedupe_session=dedupe_session,
                            counter=counter,
                            training_pair=formatted,
                            training_ids=training_ids,
                            previous_ids=previous_ids,
                            next_ids=next_ids)

def getPrevNext(session_id, training_ids):
    engine = db_session.bind
    previous_ids = None
    if training_ids is None:
        next_ids = None
        prev = ''' 
            SELECT 
              left_record->'record_id' AS left_record_id,
              right_record->'record_id' AS right_record_id
            FROM dedupe_training_data
            WHERE session_id = :session_id
            ORDER BY date_added DESC
            LIMIT 1
        '''
        prev = engine.execute(text(prev), session_id=session_id).first()
        if prev is not None:
            previous_ids = ','.join([str(prev.left_record_id), str(prev.right_record_id)])
        return previous_ids, next_ids
    else:
        left_id, right_id = training_ids.split(',')
        records = '''
          (SELECT 
             d.left_record#>>'{record_id}' AS left_record_id, 
             d.right_record#>>'{record_id}' AS right_record_id,
             d.date_added
           FROM dedupe_training_data AS d, (
             SELECT date_added 
             FROM dedupe_training_data 
             WHERE left_record->>'record_id' = :left_id 
               AND right_record->>'record_id' = :right_id
            ) AS s 
            WHERE d.date_added >= s.date_added 
              AND d.session_id = :session_id
            ORDER BY date_added ASC limit 2
          ) UNION (
            SELECT 
              d.left_record#>>'{record_id}' AS left_record_id, 
              d.right_record#>>'{record_id}' AS right_record_id,
              d.date_added
            FROM dedupe_training_data AS d, (
              SELECT date_added 
              FROM dedupe_training_data 
              WHERE left_record->>'record_id' = :left_id 
                AND right_record->>'record_id' = :right_id
            ) AS s 
            WHERE d.date_added < s.date_added 
              AND d.session_id = :session_id
            ORDER BY date_added DESC LIMIT 1) 
          ORDER BY date_added
        '''
        records = list(engine.execute(text(records), 
                                 session_id=session_id,
                                 left_id=left_id, 
                                 right_id=right_id))
        first = records[0]
        last = records[-1]
        previous_ids = ','.join([str(first.left_record_id), str(first.right_record_id)])
        next_ids = ','.join([str(last.left_record_id), str(last.right_record_id)])
        return previous_ids, next_ids

def getTrainingCounts(session_id):
    counts = ''' 
        SELECT (
          SELECT count(*)
          FROM dedupe_training_data
          WHERE session_id = :session_id
            AND pair_type = :dist
        ) AS distinct_pairs, (
          SELECT count(*)
          FROM dedupe_training_data
          WHERE session_id = :session_id
            AND pair_type = :match
        ) AS match_pairs, (
          SELECT count(*)
          FROM dedupe_training_data
          WHERE session_id = :session_id
            AND pair_type = :unsure
        ) AS unsure_pairs
    '''
    
    engine = db_session.bind
    counts = engine.execute(text(counts), 
                            session_id=session_id, 
                            dist='distinct', 
                            match='match',
                            unsure='unsure').first()

    return counts.match_pairs, counts.distinct_pairs, counts.unsure_pairs

def getTrainingPair(session_id, deduper, training_ids=None):
    if training_ids:
        left_id, right_id = training_ids.split(',')
        sel = ''' 
          SELECT
            json_build_array(left_record, right_record) AS training_pair
          FROM dedupe_training_data
          WHERE session_id = :session_id
            AND left_record->>'record_id' = :left_id
            AND right_record->>'record_id' = :right_id
        '''
        engine = db_session.bind
        training_pair = engine.execute(text(sel), 
                                     left_id=left_id, 
                                     right_id=right_id, 
                                     session_id=session_id).first()
        if training_pair:
            training_pair = json.loads(training_pair.training_pair, 
                                     object_hook=_from_json)
        else:
            # We need an error or something here
            print('not found')
    else:
        training_pair = deduper.uncertainPairs()[0]
        training_ids = ','.join([str(r['record_id']) for r in training_pair])
    return training_pair, training_ids

@trainer.route('/get-pair/')
@login_required
@check_roles(roles=['admin'])
def get_pair():
    deduper = flask_session['deduper']
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
    counter = flask_session.get('counter')
    session_id = flask_session['session_id']
    deduper = flask_session['deduper']

    sess = db_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs.decode('utf-8'))
    fds = {}
    for fd in field_defs:
        try:
            fds[fd['field']].append(fd['type'])
        except KeyError:
            fds[fd['field']] = [fd['type']]
    current_pair = flask_session['current_pair']
    left, right = current_pair
    record_ids = [left['record_id'], right['record_id']]
    if action == 'yes':
        updateTraining(session_id, 
                       match_ids=record_ids, 
                       trainer=current_user.name)
        counter['yes'] += 1
        resp = {'counter': counter}
    elif action == 'no':
        updateTraining(session_id, 
                       distinct_ids=record_ids,
                       trainer=current_user.name)
        counter['no'] += 1
        resp = {'counter': counter}
    elif action == 'finish':
        sess.processing = True
        db_session.add(sess)
        db_session.commit()
        dedupeRaw.delay(session_id)
        resp = {'finished': True}
    else:
        counter['unsure'] += 1
        flask_session['counter'] = counter
        resp = {'counter': counter}
    training_data = readTraining(session_id)
    deduper.markPairs(training_data)
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

@trainer.route('/help/')
def help(): # pragma: no cover
    user_id = flask_session.get('user_id')
    user = None
    if user_id:
        user = db_session.query(User).get(flask_session['user_id'])
    return render_template("help.html", user=user)
