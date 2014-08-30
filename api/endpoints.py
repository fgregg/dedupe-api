import os
import json
from flask import Flask, make_response, request, Blueprint, \
    session as flask_session
from api.models import DedupeSession, User, Group
from api.database import session as db_session, engine, Base
from api.auth import csrf, check_api_key
from api.dedupe_utils import get_engine, make_canonical_table, \
    create_session, preProcess
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from dedupe.convenience import canonicalize
from cPickle import loads
from cStringIO import StringIO
from api.dedupe_utils import retrain
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import Table, select, and_
from sqlalchemy.ext.declarative import declarative_base
from itertools import groupby
from operator import itemgetter
from datetime import datetime, timedelta

endpoints = Blueprint('endpoints', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime) else None

def validate_post(post):
    api_key = post.get('api_key')
    session_key = post.get('session_key')
    obj = post.get('object')
    r = {'status': 'ok', 'message': '', 'object': obj}
    status_code = 200
    # should probably validate if the user has access to the session
    sess = db_session.query(DedupeSession).get(session_key)
    user = db_session.query(User).get(api_key)
    if not api_key:
        r['status'] = 'error'
        r['message'] = 'API Key is required'
        status_code = 401
    elif not session_key:
        r['status'] = 'error'
        r['message'] = 'Session Key is required'
        status_code = 401
    elif not obj:
        r['status'] = 'error'
        r['message'] = 'Match object is required'
        status_code = 400
    elif not user:
        r['status'] = 'error'
        r['message'] = 'Invalid API Key'
        status_code = 400
    elif not sess:
        r['status'] = 'error'
        r['message'] = 'Invalid Session ID'
        status_code = 400
    return r, status_code, user, sess

@csrf.exempt
@endpoints.route('/match/', methods=['POST'])
def match():
    post = json.loads(request.data)
    r, status_code, user, sess = validate_post(post)
    if r['status'] != 'error':
        api_key = post['api_key']
        session_key = post['session_key']
        obj = post['object']
        for k,v in obj.items():
            obj[k]
        canon_table = Table('canon_%s' % session_key, 
            Base.metadata, autoload=True, autoload_with=engine)
        fields = [f for f in canon_table.columns.keys()]
        all_data = db_session.query(canon_table).all()
        data_d = {}
        for row in all_data:
            d = {}
            for k,v in zip(fields, row):
                d[k] = preProcess(unicode(v))
            data_d[int(d['canon_record_id'])] = d
        deduper = dedupe.Gazetteer(json.loads(sess.field_defs))
        deduper.readTraining(StringIO(sess.training_data))
        deduper.train()
        deduper.index(data_d)
        for k,v in obj.items():
            obj[k] = preProcess(unicode(v))
        o = {'blob': obj}
        linked = deduper.match(o, threshold=0)
        match_list = []
        if linked:
            ids = []
            confs = {}
            for l in linked[0]:
                id_set, confidence = l
                ids.extend([i for i in id_set if i != 'blob'])
                confs[id_set[1]] = confidence
            ids = list(set(ids))
            matches = db_session.query(canon_table)\
                .filter(canon_table.c.canon_record_id.in_(ids))\
                .all()
            for match in matches:
                m = {}
                for k,v in zip(fields, match):
                    m[k] = v
                m['match_confidence'] = float(confs[str(m['canon_record_id'])])
                match_list.append(m)
        r['matches'] = match_list

    resp = make_response(json.dumps(r, default=_to_json), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@endpoints.route('/train/', methods=['POST'])
def train():
    post = json.loads(request.data)
    r, status_code, user, sess = validate_post(post)
    if not post.get('matches'):
        r['status'] = 'error'
        r['message'] = 'List of matches is required'
        status_code = 400
    if r['status'] != 'error':
        api_key = post['api_key']
        session_key = post['session_key']
        obj = post['object']
        positive = []
        negative = []
        for match in post['matches']:
            if match['match'] is 1:
                positive.append(match)
            else:
                negative.append(match)
        if len(positive) > 1:
            r['status'] = 'error'
            r['message'] = 'A maximum of 1 matching record can be sent. \
                More indicates a non-canonical dataset'
            status_code = 400
        else:
            training_data = json.loads(sess.training_data, cls=dedupe_decoder)
            training_data['match'].append([positive[0],obj])
            for n in negative:
                training_data['distinct'].append([n,obj])
            sess.training_data = json.dumps(training_data, default=_to_json)
            db_session.add(sess)
            db_session.commit()
            retrain.delay(session_key)
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/training-data/<session_id>/')
@check_api_key()
def training_data(session_id):
    data = db_session.query(DedupeSession).get(session_id)
    training_data = data.training_data
    resp = make_response(training_data, 200)
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_training.json' % data.uuid
    return resp

@endpoints.route('/settings-file/<session_id>/')
@check_api_key()
def settings_file(session_id):
    data = db_session.query(DedupeSession).get(session_id)
    settings_file = data.settings_file
    resp = make_response(settings_file, 200)
    resp.headers['Content-Disposition'] = 'attachment; filename=%s.dedupe_settings' % data.uuid
    return resp

@endpoints.route('/field-definitions/<session_id>/')
@check_api_key()
def field_definitions(session_id):
    data = db_session.query(DedupeSession).get(session_id)
    field_defs = data.field_defs
    resp = make_response(field_defs, 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/delete-session/<session_id>/')
@check_api_key()
def delete_session(session_id):
    data = db_session.query(DedupeSession).get(session_id)
    db_session.delete(data)
    db_session.commit()
    try:
        data_table = Table('entity_%s' % session_id, 
            Base.metadata, autoload=True, autoload_with=engine)
        data_table.drop(engine)
    except NoSuchTableError:
        pass
    try:
        raw_table = Table('raw_%s' % session_id, 
            Base.metadata, autoload=True, autoload_with=engine)
        raw_table.drop(engine)
    except NoSuchTableError:
        pass
    try:
        block_table = Table('block_%s' % session_id, 
            Base.metadata, autoload=True, autoload_with=engine)
        block_table.drop(engine)
    except NoSuchTableError:
        pass
    try:
        master_table = Table('master_%s' % session_id, 
            Base.metadata, autoload=True, autoload_with=engine)
        master_table.drop(engine)
    except NoSuchTableError:
        pass
    resp = make_response(json.dumps({'session_id': session_id, 'status': 'ok'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/session-list/')
@check_api_key()
def review():
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    api_key = request.args.get('api_key')
    if not api_key:
        api_key = flask_session['user_id']
    user = db_session.query(User).get(api_key)
    sessions = db_session.query(DedupeSession)\
        .filter(DedupeSession.group.has(Group.id.in_([i.id for i in user.groups]))).all()
    all_sessions = []
    for sess in sessions:
        d = {
            'name': sess.name,
            'id': sess.id
        }
        all_sessions.append(d)
    resp['objects'] = all_sessions
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

def checkin_sessions():
    now = datetime.now()
    all_sessions = [i.id for i in db_session.query(DedupeSession.id).all()]
    conn = engine.contextual_connect()
    for sess_id in all_sessions:
        table = Table('entity_%s' % sess_id, Base.metadata, 
            autoload=True, autoload_with=engine)
        upd = table.update().where(table.c.checkout_expire <= now)\
            .where(table.c.clustered == False)\
            .values(checked_out = False, checkout_expire = None)
        conn.execute(upd)
    return None

@endpoints.route('/get-review-cluster/<session_id>/')
@check_api_key()
def get_cluster(session_id):
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    api_key = request.args.get('api_key')
    if not api_key:
        api_key = flask_session['user_id']
    user = db_session.query(User).get(api_key)
    sess = db_session.query(DedupeSession)\
        .filter(DedupeSession.group.has(
            Group.id.in_([i.id for i in user.groups])))\
        .filter(DedupeSession.id == session_id)\
        .first()
    if not sess:
        resp['status'] = 'error'
        resp['message'] = "You don't have access to session '%s'" % session_id
        status_code = 401
    else:
        entity_table = Table('entity_%s' % session_id, Base.metadata,
            autoload=True, autoload_with=engine)
        clusters_q = db_session.query(entity_table.c.group_id.distinct())
        total_clusters = clusters_q.count()
        review_remainder = clusters_q.filter(entity_table.c.clustered == False).count()
        cluster_list = []
        if review_remainder > 0:
            field_defs = [f['field'] for f in json.loads(sess.field_defs)]
            raw_session = create_session(sess.conn_string)
            raw_engine = raw_session.bind
            raw_base = declarative_base()
            raw_table = Table(sess.table_name, raw_base.metadata, 
                autoload=True, autoload_with=raw_engine)
            entity_fields = ['record_id', 'group_id', 'confidence']
            entity_cols = [getattr(entity_table.c, f) for f in entity_fields]
            subq = db_session.query(entity_table.c.group_id)\
                .filter(entity_table.c.checked_out == False)\
                .filter(entity_table.c.clustered == False)\
                .order_by(entity_table.c.confidence).limit(1).subquery()
            cluster = db_session.query(*entity_cols)\
                .filter(entity_table.c.group_id.in_(subq)).all()
            raw_ids = [c[0] for c in cluster]
            raw_cols = [getattr(raw_table.c, f) for f in field_defs]
            primary_key = [p.name for p in raw_table.primary_key][0]
            pk_col = getattr(raw_table.c, primary_key)
            records = raw_session.query(*raw_cols).filter(pk_col.in_(raw_ids))
            raw_fields = [f['name'] for f in records.column_descriptions]
            records = records.all()
            ten_minutes = datetime.now() + timedelta(minutes=10)
            upd = entity_table.update()\
                .where(entity_table.c.group_id.in_(subq))\
                .values(checked_out=True, checkout_expire=ten_minutes)
            conn = engine.contextual_connect()
            conn.execute(upd)
            resp['confidence'] = cluster[0][2]
            resp['group_id'] = cluster[0][1]
            for thing in records:
                d = {}
                for k,v in zip(raw_fields, thing):
                    d[k] = v
                cluster_list.append(d)
        else:
            make_canonical_table.delay(session_id)
        resp['objects'] = cluster_list
        resp['total_clusters'] = total_clusters
        resp['review_remainder'] = review_remainder
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@endpoints.route('/mark-cluster/<session_id>/')
@check_api_key()
def mark_cluster(session_id):
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    api_key = request.args.get('api_key')
    if not api_key:
        api_key = flask_session['user_id']
    user = db_session.query(User).get(api_key)
    sess = db_session.query(DedupeSession)\
        .filter(DedupeSession.group.has(
            Group.id.in_([i.id for i in user.groups])))\
        .filter(DedupeSession.id == session_id)\
        .first()
    if not sess:
        resp['status'] = 'error'
        resp['message'] = "You don't have access to session '%s'" % session_id
        status_code = 401
    else:
        entity_table = Table('entity_%s' % session_id, Base.metadata,
            autoload=True, autoload_with=engine)
        conn = engine.contextual_connect()
        group_id = request.args.get('group_id')
        action = request.args.get('action')
        if action == 'yes':
            upd = entity_table.update()\
                .where(entity_table.c.group_id == group_id)\
                .values(clustered=True, checked_out=False, checkout_expire=None)
            conn.execute(upd)
            conn.close()
        elif action == 'no':
            dels = entity_table.delete()\
                .where(entity_table.c.group_id == group_id)
            conn.execute(dels)
            conn.close()
        r = {
            'session_id': session_id, 
            'group_id': group_id, 
            'status': 'ok', 
            'action': action,
            'message': ''
        }
        status_code = 200
    resp = make_response(json.dumps(r), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

