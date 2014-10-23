import os
import json
from flask import Flask, make_response, request, Blueprint, \
    session as flask_session, make_response, send_from_directory, jsonify
from api.models import DedupeSession, User, Group
from api.app_config import DOWNLOAD_FOLDER
from api.queue import DelayedResult, redis
from api.database import app_session as db_session, app_engine as engine, Base
from api.auth import csrf, check_api_key
from api.utils.delayed_tasks import retrain, bulkMatchWorker, dedupeCanon
from api.utils.helpers import preProcess
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from dedupe.convenience import canonicalize
from cPickle import loads
from cStringIO import StringIO
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import Table, select, and_, func, distinct
from sqlalchemy.ext.declarative import declarative_base
from itertools import groupby
from operator import itemgetter
from datetime import datetime, timedelta
from hashlib import md5

endpoints = Blueprint('endpoints', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime) else None

def validate_post(post, user_sessions):
    session_id = post.get('session_key')
    obj = post.get('object')
    r = {'status': 'ok', 'message': '', 'object': obj}
    status_code = 200
    # should probably validate if the user has access to the session
    sess = db_session.query(DedupeSession).get(session_id)
    if session_id not in user_sessions:
        r['status'] = 'error'
        r['message'] = "You don't have access to session %s" % session_id
        status_code = 401
    elif not session_id:
        r['status'] = 'error'
        r['message'] = 'Session ID is required'
        status_code = 401
    elif not obj:
        r['status'] = 'error'
        r['message'] = 'Match object is required'
        status_code = 400
    elif not sess:
        r['status'] = 'error'
        r['message'] = 'Invalid Session ID'
        status_code = 400
    return r, status_code, user, sess

@csrf.exempt
@endpoints.route('/match/', methods=['POST'])
@check_api_key()
def match():
    try:
        post = json.loads(request.data)
    except ValueError:
        post = json.loads(request.form.keys()[0])
    user_sessions = flask_session['user_sessions']
    r, status_code, user, sess = validate_post(post, user_sessions)
    if r['status'] != 'error':
        api_key = post['api_key']
        session_id = post['session_key']
        n_matches = post.get('num_matches', 5)
        obj = post['object']
        field_defs = json.loads(sess.field_defs)
        model_fields = [f['field'] for f in field_defs]
        entity_table = Table('entity_%s' % session_id, Base.metadata, 
            autoload=True, autoload_with=engine, keep_existing=True)
        raw_table = Table(sess.table_name, Base.metadata, 
            autoload=True, autoload_with=engine, keep_existing=True)
        raw_cols = [getattr(raw_table.c, f) for f in model_fields]
        pk_col = [p for p in raw_table.primary_key][0]
        hash_me = ';'.join([preProcess(unicode(obj[i])) for i in model_fields])
        md5_hash = md5(hash_me).hexdigest()
        exact_match = db_session.query(entity_table)\
            .filter(entity_table.c.source_hash == md5_hash).first()
        match_list = []
        if exact_match:
            cluster = db_session.query(entity_table.c.record_id)\
                .filter(entity_table.c.entity_id == exact_match.entity_id)\
                .all()
            raw_ids = [c[0] for c in cluster]
            raw_record = db_session.query(*raw_cols)\
                .filter(pk_col.in_(raw_ids)).first()
            d = { f: getattr(raw_record, f) for f in model_fields }
            d['entity_id'] = exact_match.record_id
            d['match_confidence'] = '1.0'
            match_list.append(d)
        else:
            deduper = dedupe.StaticGazetteer(StringIO(sess.gaz_settings_file))
            for k,v in obj.items():
                obj[k] = preProcess(unicode(v))
            o = {'blob': obj}
            raw_cols.append(pk_col)
            raw_data = db_session.query(*raw_cols).all()
            data_d = {}
            for row in raw_data:
                d = {f: preProcess(unicode(getattr(row, f))) for f in model_fields}
                data_d[int(getattr(row, pk_col.name))] = d
            deduper.index(data_d)
            linked = deduper.match(o, threshold=0, n_matches=n_matches)
            if linked:
                ids = []
                confs = {}
                for l in linked[0]:
                    id_set, confidence = l
                    ids.extend([i for i in id_set if i != 'blob'])
                    confs[id_set[1]] = confidence
                ids = list(set(ids))
                matches = db_session.query(*raw_cols)\
                    .filter(pk_col.in_(ids)).all()
                for match in matches:
                    m = {f: getattr(match, f) for f in model_fields}
                    m['entity_id'] = getattr(match, pk_col.name)
                    m['match_confidence'] = float(confs[str(m['entity_id'])])
                    match_list.append(m)
        r['matches'] = match_list

    resp = make_response(json.dumps(r, default=_to_json), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@endpoints.route('/train/', methods=['POST'])
@check_api_key()
def train():
    try:
        post = json.loads(request.data)
    except ValueError:
        post = json.loads(request.form.keys()[0])
    user_sessions = flask_session['user_sessions']
    r, status_code, user, sess = validate_post(post)
    if not post.get('matches'):
        r['status'] = 'error'
        r['message'] = 'List of matches is required'
        status_code = 400
    if r['status'] != 'error':
        api_key = post['api_key']
        session_id = post['session_key']
        obj = post['object']
        positive = []
        negative = []
        for match in post['matches']:
            if match['match'] is 1:
                positive.append(match)
            else:
                negative.append(match)
            for k,v in match.items():
                match[k] = preProcess(unicode(v))
            del match['match_confidence']
            del match['match']
        if len(positive) > 1:
            r['status'] = 'error'
            r['message'] = 'A maximum of 1 matching record can be sent. \
                More indicates a non-canonical dataset'
            status_code = 400
        else:
            training_data = json.loads(sess.training_data)
            if positive:
                training_data['match'].append([positive[0],obj])
            for n in negative:
                training_data['distinct'].append([n,obj])
            sess.training_data = json.dumps(training_data)
            db_session.add(sess)
            db_session.commit()
            retrain.delay(session_id)
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/training-data/<session_id>/')
@check_api_key()
def training_data(session_id):
    user_sessions = flask_session['user_sessions']
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        resp = make_response(resp, 401)
        resp.headers['Content-Type'] = 'application/json'
    else:
        data = db_session.query(DedupeSession).get(session_id)
        training_data = data.training_data
        resp = make_response(training_data, 200)
        resp.headers['Content-Type'] = 'text/plain'
        resp.headers['Content-Disposition'] = 'attachment; filename=%s_training.json' % data.uuid
    return resp

@endpoints.route('/settings-file/<session_id>/')
@check_api_key()
def settings_file(session_id):
    user_sessions = flask_session['user_sessions']
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        resp = make_response(resp, 401)
        resp.headers['Content-Type'] = 'application/json'
    else:
        data = db_session.query(DedupeSession).get(session_id)
        settings_file = data.settings_file
        resp = make_response(settings_file, 200)
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.dedupe_settings' % data.uuid
    return resp

@endpoints.route('/field-definitions/<session_id>/')
@check_api_key()
def field_definitions(session_id):
    user_sessions = flask_session['user_sessions']
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        resp = make_response(resp, 401)
        resp.headers['Content-Type'] = 'application/json'
    else:
        data = db_session.query(DedupeSession).get(session_id)
        field_defs = data.field_defs
        resp = make_response(field_defs, 200)
        resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/delete-session/<session_id>/')
@check_api_key()
def delete_session(session_id):
    user_sessions = flask_session['user_sessions']
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        resp = make_response(resp, 401)
        resp.headers['Content-Type'] = 'application/json'
    else:
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
    user_sessions = flask_session['user_sessions']
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    sessions = db_session.query(DedupeSession)\
        .filter(DedupeSession.id.in_(user_sessions))\
        .all()
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
    for sess_id in all_sessions:
        table = Table('entity_%s' % sess_id, Base.metadata, 
            autoload=True, autoload_with=engine)
        upd = table.update().where(table.c.checkout_expire <= now)\
            .where(table.c.clustered == False)\
            .values(checked_out = False, checkout_expire = None)
        engine.execute(upd)
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
        checkin_sessions()
        entity_table = Table('entity_%s' % session_id, Base.metadata,
            autoload=True, autoload_with=engine)
        total_clusters = db_session.query(entity_table.c.entity_id.distinct()).count()
        review_remainder = db_session.query(entity_table.c.entity_id.distinct())\
            .filter(entity_table.c.clustered == False)\
            .count()
        cluster_list = []
        if review_remainder > 0:
            field_defs = [f['field'] for f in json.loads(sess.field_defs)]
            raw_table = Table(sess.table_name, Base.metadata, 
                autoload=True, autoload_with=engine, keep_existing=True)
            entity_fields = ['record_id', 'entity_id', 'confidence']
            entity_cols = [getattr(entity_table.c, f) for f in entity_fields]
            subq = db_session.query(entity_table.c.entity_id)\
                .filter(entity_table.c.checked_out == False)\
                .filter(entity_table.c.clustered == False)\
                .order_by(entity_table.c.confidence).limit(1).subquery()
            cluster = db_session.query(*entity_cols)\
                .filter(entity_table.c.entity_id.in_(subq)).all()
            raw_ids = [c[0] for c in cluster]
            raw_cols = [getattr(raw_table.c, f) for f in field_defs]
            primary_key = [p.name for p in raw_table.primary_key][0]
            pk_col = getattr(raw_table.c, primary_key)
            records = db_session.query(*raw_cols).filter(pk_col.in_(raw_ids))
            raw_fields = [f['name'] for f in records.column_descriptions]
            records = records.all()
            one_minute = datetime.now() + timedelta(minutes=1)
            upd = entity_table.update()\
                .where(entity_table.c.entity_id.in_(subq))\
                .values(checked_out=True, checkout_expire=one_minute)
            engine.execute(upd)
            resp['confidence'] = cluster[0][2]
            resp['entity_id'] = cluster[0][1]
            for thing in records:
                d = {}
                for k,v in zip(raw_fields, thing):
                    d[k] = v
                cluster_list.append(d)
        else:
            # This is where we run dedupeCanon 
            if sess.status == 'first pass review complete':
                sess.status = 'review complete'
            else:
                sess.status = 'first pass review complete'
                dedupeCanon.delay(sess.id)
            db_session.add(sess)
            db_session.commit()
        resp['objects'] = cluster_list
        resp['total_clusters'] = total_clusters
        resp['review_remainder'] = review_remainder
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@endpoints.route('/mark-all-clusters/<session_id>/')
@check_api_key()
def mark_all_clusters(session_id):
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
        raw_table = Table('raw_%s' % session_id, Base.metadata,
            autoload=True, autoload_with=engine)
        entity_id = request.args.get('entity_id')
        action = request.args.get('action')
        canon_table = Table('canon_%s' % session_id, Base.metadata,
            autoload=True, autoload_with=engine)
        canon_ids = db_session.query(canon_table.c.canon_record_id.distinct())\
            .join(entity_table, canon_table.c.canon_record_id == entity_table.c.canon_record_id)\
            .filter(entity_table.c.entity_id == entity_id)\
            .subquery()
        canons = db_session.query(canon_table)\
            .filter(canon_table.c.canon_record_id.in_(canon_ids))\
            .all()
        fields = [c for c in canon_table.columns.keys() if c != 'canon_record_id']
        pairs = []
        for canon in canons:
            pairs.append({k:v for k,v in zip(fields, canon)})
        training_data = json.loads(sess.training_data)
        if action == 'yes':
            upd = entity_table.update()\
                .where(entity_table.c.entity_id == entity_id)\
                .values(clustered=True, checked_out=False, checkout_expire=None)
            engine.execute(upd)
            training_data['match'].append(pairs)
        elif action == 'no':
            rows = db_session.query(entity_table)\
                .filter(entity_table.c.entity_id == entity_id)\
                .all()
            upd_rows = [r for r in rows if r.former_entity_id]
            del_rows = [r for r in rows if not r.former_entity_id]
            for row in upd_rows:
                upd = entity_table.update()\
                    .where(entity_table.c.entity_id == row.entity_id)\
                    .where(entity_table.c.former_entity_id == row.former_entity_id)\
                    .values(entity_id=row.former_entity_id, clustered=True)
                engine.execute(upd)
            for row in del_rows:
                delete = entity_table.delete()\
                    .where(entity_table.c.entity_id == row.entity_id)\
                    .where(entity_table.c.former_entity_id == row.former_entity_id)
                engine.execute(delete)
            training_data['distinct'].append(pairs)
        sess.training_data = json.dumps(training_data)
        db_session.add(sess)
        db_session.commit()
        r = {
            'session_id': session_id, 
            'entity_id': entity_id, 
            'status': 'ok', 
            'action': action,
            'message': ''
        }
        status_code = 200
    resp = make_response(json.dumps(r), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

status_lookup = {
    'dataset uploaded': 'training started',
    'training started': 'training completed',
    'training completed': 'dedupe started',
    'dedupe started': 'review queue ready',
    'review queue ready': 'review complete',
    'review complete': '',
}

@endpoints.route('/session-status/<session_id>/')
@check_api_key()
def session_status(session_id):
    resp = {
        'status': 'ok',
        'message': '',
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
        resp['session_status'] = sess.status
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@endpoints.route('/bulk-match/<session_id>/', methods=['POST'])
@check_api_key()
def bulk_match(session_id):
    """ 
    field_map looks like:
        {
            '<dataset_field_name>': '<uploaded_field_name>',
            '<dataset_field_name>': '<uploaded_field_name>',
        }
    """
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    files = request.files.values()
    if not files:
        try:
            files = flask_session['bulk_match_upload']
            filename = flask_session['bulk_match_filename']
        except KeyError:
            resp['status'] = 'error'
            resp['message'] = 'File upload required'
            status_code = 400
    else:
        files = files[0].read()
        filename = files.filename
    field_map = request.form.get('field_map')
    if not field_map:
        resp['status'] = 'error'
        resp['message'] = 'field_map is required'
        status_code = 400
    if status_code is 200:
        field_map = json.loads(field_map)
        token = bulkMatchWorker.delay(
            session_id,
            files, 
            field_map, 
            filename
        )
        resp['token'] = token.key
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/check-bulk-match/<token>/')
@check_api_key()
def check_bulk_match(token):
    rv = DelayedResult(token)
    if rv.return_value is None:
        return jsonify(ready=False)
    redis.delete(token)
    result = rv.return_value
    if result['status'] == 'ok':
        result['result'] = '/downloads/%s' % result['result']
    resp = make_response(json.dumps(result))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/downloads/<path:filename>/')
def downloads(filename):
    return send_from_directory(DOWNLOAD_FOLDER, filename)
