import os
import json
from flask import Flask, make_response, request, session, Blueprint
from api.database import db, DedupeSession, ApiUser
from api.auth import csrf
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from cPickle import loads
from cStringIO import StringIO
from api.dedupe_utils import retrain
import sqlalchemy.exc

endpoints = Blueprint('endpoints', __name__)

def validate_post(post):
    api_key = post.get('api_key')
    session_key = post.get('session_key')
    obj = post.get('object')
    r = {'status': 'ok', 'message': '', 'object': obj}
    status_code = 200
    sess = db.session.query(DedupeSession).get(session_key)
    user = db.session.query(ApiUser).get(api_key)
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
            obj[k] = v.lower()
        data_table = db.Table('%s_data' % session_key, 
            db.metadata, autoload=True, autoload_with=db.engine)
        all_data = db.session.query(data_table).all()
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
            matches = db.session.query(data_table).filter(data_table.c.id.in_(ids)).all()
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
            db.session.add(sess)
            db.session.commit()
            retrain.delay(session_key)
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@endpoints.route('/training-data/<session_id>/')
def training_data(session_id):
    data = db.session.query(DedupeSession).get(session_id)
    training_data = data.training_data
    resp = make_response(training_data, 200)
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_training.json' % data.uuid
    return resp

@csrf.exempt
@endpoints.route('/settings-file/<session_id>/')
def settings_file(session_id):
    data = db.session.query(DedupeSession).get(session_id)
    settings_file = data.settings_file
    resp = make_response(settings_file, 200)
    resp.headers['Content-Disposition'] = 'attachment; filename=%s.dedupe_settings' % data.uuid
    return resp

@csrf.exempt
@endpoints.route('/delete-session/<session_id>/')
def delete_session(session_id):
    data = db.session.query(DedupeSession).get(session_id)
    db.session.delete(data)
    db.session.commit()
    data_table = db.Table('%s_data' % session_id, 
        db.metadata, autoload=True, autoload_with=db.engine)
    try:
        data_table.drop(db.engine)
    except db.exc.NoSuchTableError:
        pass
    resp = make_response(json.dumps({'session_id': session_id, 'status': 'ok'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@endpoints.route('/field-definitions/<session_id>/')
def field_definitions(session_id):
    data = db.session.query(DedupeSession).get(session_id)
    field_defs = data.field_defs
    resp = make_response(field_defs, 200)
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['Content-Disposition'] = 'attachment; filename=%s_field_defs.json' % data.uuid
    return resp
