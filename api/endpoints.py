import os
import json
from flask import Flask, make_response, request, Blueprint, \
    session as flask_session
from api.models import DedupeSession, User, Group
from api.database import session as db_session, engine, Base
from api.auth import csrf
from api.dedupe_utils import get_or_create_master_table
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from dedupe.convenience import canonicalize
from cPickle import loads
from cStringIO import StringIO
from api.dedupe_utils import retrain
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import Table, select, and_
from itertools import groupby
from operator import itemgetter
from datetime import datetime, timedelta

endpoints = Blueprint('endpoints', __name__)

def validate_post(post):
    api_key = post.get('api_key')
    session_key = post.get('session_key')
    obj = post.get('object')
    r = {'status': 'ok', 'message': '', 'object': obj}
    status_code = 200
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
        data_table = db_Table('%s_data' % session_key, 
            Base.metadata, autoload=True, autoload_with=db_engine)
        all_data = db_session.query(data_table).all()
        data_d = {}
        for d in all_data:
            data_d[d.id] = loads(d.blob)
        deduper = dedupe.StaticGazetteer(StringIO(sess.settings_file))
        o = {'blob': obj}
        linked = deduper.match(o, data_d, threshold=0, n_matches=post.get('num_results', 5))
        match_list = []
        if linked:
            ids = []
            confs = {}
            for l in linked[0]:
                id_set, confidence = l
                ids.extend([i for i in id_set if i not in ids])
                confs[id_set[1]] = confidence
            matches = db_session.query(data_table).filter(data_table.c.id.in_(ids)).all()
            for match in matches:
                m = dict(loads(match.blob))
                # m['match_confidence'] = float(confs[str(match.id)])
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

@csrf.exempt
@endpoints.route('/training-data/<session_id>/')
def training_data(session_id):
    data = db_session.query(DedupeSession).get(session_id)
    training_data = data.training_data
    resp = make_response(training_data, 200)
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_training.json' % data.uuid
    return resp

@csrf.exempt
@endpoints.route('/settings-file/<session_id>/')
def settings_file(session_id):
    data = db_session.query(DedupeSession).get(session_id)
    settings_file = data.settings_file
    resp = make_response(settings_file, 200)
    resp.headers['Content-Disposition'] = 'attachment; filename=%s.dedupe_settings' % data.uuid
    return resp

@csrf.exempt
@endpoints.route('/field-definitions/<session_id>/')
def field_definitions(session_id):
    data = db_session.query(DedupeSession).get(session_id)
    field_defs = data.field_defs
    resp = make_response(field_defs, 200)
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_field_defs.json' % data.uuid
    return resp

@csrf.exempt
@endpoints.route('/delete-session/<session_id>/')
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

@endpoints.route('/master/<session_id>/')
def master(session_id):
    master_table = Table('master_%s' % session_id, Base.metadata,
        autoload=True, autoload_with=engine)
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    count = db_session.query(master_table).count()
    base_query = db_session.query(master_table)
    if limit and offset:
        base_query = base_query.limit(int(limit)).offset(int(offset))
    rows = [r for r in base_query.all()]
    master_rows = []
    for row in rows:
        d = {}
        for k,v in zip(master_table.columns.keys(), row):
            d[k] = v
        master_rows.append(d)
    resp = make_response(json.dumps(master_rows))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/session-list/')
def review():
    api_key = None
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    if flask_session.get('user_id'):
        api_key = flask_session['user_id']
    else:
        api_key = request.args.get('api_key')
    if not api_key:
        resp['status'] = 'error'
        resp['message'] = "'api_key' is a required parameter"
        status_code = 401
    else:
        user = db_session.query(User).get(api_key)
        sessions = db_session.query(DedupeSession)\
            .filter(DedupeSession.group.has(Group.id.in_([i.id for i in user.groups]))).all()
        all_sessions = [s.as_dict() for s in sessions]
        resp['objects'] = all_sessions
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@endpoints.route('/review-queue/<session_id>/')
def review_queue(session_id):
    api_key = None
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    if flask_session.get('user_id'):
        api_key = flask_session['user_id']
    else:
        api_key = request.args.get('api_key')
    if not api_key:
        resp['status'] = 'error'
        resp['message'] = "'api_key' is a required parameter"
        status_code = 401
    else:
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
            resp['objects'] = grouped,
            resp['session_id'] = session_id
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
def get_cluster(session_id):
    api_key = None
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    checkin_sessions()
    if flask_session.get('user_id'):
        api_key = flask_session['user_id']
    else:
        api_key = request.args.get('api_key')
    if not api_key:
        resp['status'] = 'error'
        resp['message'] = "'api_key' is a required parameter"
        status_code = 401
    else:
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
            field_defs = [f['field'] for f in json.loads(sess.field_defs)]
            raw_table = Table('raw_%s' % session_id, Base.metadata, 
                autoload=True, autoload_with=engine)
            entity_table = Table('entity_%s' % session_id, Base.metadata,
                autoload=True, autoload_with=engine)
            cols = [getattr(raw_table.c, f) for f in field_defs]
            cols.append(raw_table.c.record_id)
            cols.extend([getattr(entity_table.c, f) \
                for f in ['confidence', 'record_id', 'group_id']])
            q = db_session.query(*cols)
            fields = [f['name'] for f in q.column_descriptions]
            subq = db_session.query(entity_table.c.group_id)\
                .filter(entity_table.c.checked_out == False)\
                .filter(entity_table.c.clustered == False)\
                .order_by(entity_table.c.confidence.desc()).limit(1).subquery()
            cluster = q.filter(raw_table.c.record_id == entity_table.c.record_id)\
                .filter(entity_table.c.group_id.in_(subq))\
                .all()
            ten_minutes = datetime.now() + timedelta(minutes=10)
            upd = entity_table.update()\
                .where(entity_table.c.group_id.in_(subq))\
                .values(checked_out=True, checkout_expire=ten_minutes)
            conn = engine.contextual_connect()
            conn.execute(upd)
            cluster_list = []
            for thing in cluster:
                d = {}
                for k,v in zip(fields, thing):
                    d[k] = v
                cluster_list.append(d)

            clusters_q = db_session.query(entity_table.c.group_id.distinct())
            total_clusters = clusters_q.count()
            review_remainder = clusters_q.filter(entity_table.c.clustered == False).count()
            resp['objects'] = cluster_list
            resp['total_clusters'] = total_clusters
            resp['review_remainder'] = review_remainder
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@endpoints.route('/mark-cluster/<session_id>/')
def mark_cluster(session_id):
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
        raw_table = Table('raw_%s' % session_id, Base.metadata,
            autoload=True, autoload_with=engine)
        sel = select([raw_table]).where(and_(
                      raw_table.c.record_id == entity_table.c.record_id,
                      entity_table.c.group_id == group_id))
        rows = conn.execute(sel)
        cluster = []
        nulls = []
        for row in rows:
            d = {}
            for name, value in zip(rows.keys(), row):
                if name == 'record_id':
                    continue
                d[name] = value
            cluster.append(d)
        canonical = canonicalize(cluster)
        master_table = get_or_create_master_table(engine.url, session_id)
        ins = master_table.insert().values(**canonical)
        conn.execute(ins)
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
    }
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp

