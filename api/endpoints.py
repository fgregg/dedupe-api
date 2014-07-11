import os
import json
from flask import Flask, make_response, request, session, Blueprint
from api.database import db, DedupeSession, ApiUser
from api.auth import csrf
import dedupe
from cPickle import loads

endpoints = Blueprint('endpoints', __name__)

@csrf.exempt
@endpoints.route('/match/', methods=['POST'])
def match():
    post = json.loads(request.data)
    api_key = post.get('api_key')
    session_key = post.get('session_key')
    obj = post.get('object')
    r = {'status': 'ok', 'message': '', 'objects': []}
    status_code = 200
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
        r['message'] = 'Object to match is required'
        status_code = 400
    else:
        user = db.session.query(ApiUser).get(api_key)
        if not user:
            r['status'] = 'error'
            r['message'] = 'Invalid API Key'
            status_code = 400
        else:
            sess = db.session.query(DedupeSession).get(session_key)
            if not sess:
                r['status'] = 'error'
                r['message'] = 'Invalid Session ID'
                status_code = 400
            else:
                canon_table = db.Table('%s_canon' % session_key, 
                    db.metadata, autoload=True, autoload_with=db.engine)
                canon = db.session.query(canon_table).all()
                canon_data = {}
                for c in canon:
                    canon_data[c.row_id] = loads(c.row_blob)
                settings_file = '/tmp/%s.settings' % session_key
                with open('/tmp/%s.settings' % session_key, 'wb') as f:
                    f.write(sess.settings_file)
                deduper = dedupe.StaticRecordLink(str(settings_file))
                o = {'blob': obj}
                linked = deduper.match(canon_data, o, 0)
                row_id = linked[0].tolist()[0]
                matches = db.session.query(canon_table)\
                    .filter(canon_table.c.row_id == row_id)\
                    .all()
                match_list = [loads(m.row_blob) for m in matches]
                r['objects'] = match_list

    resp = make_response(json.dumps(r), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@endpoints.route('/train/', methods=['POST'])
def train():
    print request.data
    resp = make_response(json.dumps({}))
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
    resp = make_response(json.dumps({'session_id': session_id, 'status': 'ok'}))
    resp.headers['Content-Type'] = 'application/json'
    return resp
