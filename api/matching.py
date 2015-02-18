import os
import json
from uuid import uuid4
from flask import Flask, make_response, request, Blueprint, \
    session as flask_session, make_response, render_template, jsonify, \
    current_app
from api.models import DedupeSession, User
from api.app_config import DOWNLOAD_FOLDER, TIME_ZONE
from api.database import app_session as db_session, init_engine, Base
from api.auth import csrf, check_sessions, login_required, check_roles
from api.utils.helpers import preProcess, getMatches
#from api.utils.delayed_tasks import getNextHumanReview
from api.track_usage import tracker
import dedupe
from dedupe.serializer import _to_json
from cStringIO import StringIO
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from sqlalchemy import Table, text
from datetime import datetime
from hashlib import md5
from unidecode import unidecode
from collections import OrderedDict

matching = Blueprint('matching', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime) else None

try: # pragma: no cover
    from raven import Client as Sentry
    from api.app_config import SENTRY_DSN
    sentry = Sentry(dsn=SENTRY_DSN) 
except ImportError:
    sentry = None
except KeyError: #pragma: no cover
    sentry = None

def validate_post(post):
    session_id = post.get('session_id')
    obj = post.get('object')
    r = {'status': 'ok', 'message': '', 'object': obj}
    status_code = 200
    sess = db_session.query(DedupeSession).get(session_id)
    if not session_id:
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
    return r, status_code, sess

@tracker.include
@csrf.exempt
@check_sessions()
@matching.route('/match/', methods=['POST'])
def match():
    try:
        post = json.loads(request.data)
    except ValueError:
        r = {
            'status': 'error',
            'message': ''' 
                The content of your request should be a 
                string encoded JSON object.
            ''',
            'object': request.data,
        }
        resp = make_response(json.dumps(r), 400)
        resp.headers['Content-Type'] = 'application/json'
        return resp
    r, status_code, sess = validate_post(post)
    if r['status'] != 'error':
        api_key = post['api_key']
        session_id = post['session_id']
        n_matches = post.get('num_matches', 5)
        obj = post['object']
        
        field_defs = json.loads(sess.field_defs)
        model_fields = sorted(list(set([f['field'] for f in field_defs])))
        fields = ', '.join(['r.{0}'.format(f) for f in model_fields])
        engine = db_session.bind
        entity_table = Table('entity_{0}'.format(session_id), Base.metadata, 
            autoload=True, autoload_with=engine, keep_existing=True)
        try:
            hash_me = []
            for field in model_fields:
                if obj[field]:
                    hash_me.append(unicode(obj[field]))
                else:
                    hash_me.append('')
            hash_me = ';'.join(hash_me)
        except KeyError, e:
            r['status'] = 'error'
            r['message'] = 'Sent fields "{0}" do no match model fields "{1}"'\
                .format(','.join(obj.keys()), ','.join(model_fields))
            resp = make_response(json.dumps(r), 400)
            resp.headers['Content-Type'] = 'application/json'
            return resp
        if set(obj.keys()).isdisjoint(set(model_fields)):
            r['status'] = 'error'
            r['message'] = 'Sent fields "{0}" do no match model fields "{1}"'\
                .format(','.join(obj.keys()), ','.join(model_fields))
            resp = make_response(json.dumps(r), 400)
            resp.headers['Content-Type'] = 'application/json'
            return resp
        md5_hash = md5(unidecode(hash_me)).hexdigest()
        exact_match = db_session.query(entity_table)\
            .filter(entity_table.c.source_hash == md5_hash).first()
        match_list = []
        if exact_match:
            sel = text(''' 
                  SELECT {0} 
                  FROM "raw_{1}" AS r
                  JOIN "entity_{1}" AS e
                    ON r.record_id = e.record_id
                  WHERE e.entity_id = :entity_id
                  LIMIT :limit
                '''.format(fields, session_id))
            rows = []
            with engine.begin() as conn:
                rows = list(conn.execute(sel, 
                    entity_id=exact_match.entity_id, limit=n_matches))
            for row in rows:
                d = {f: getattr(row, f) for f in model_fields}
                d['entity_id'] = exact_match.entity_id
                d['match_confidence'] = '1.0'
                match_list.append(d)
        matches = getMatches(session_id, obj)
        for match in matches:
            m = OrderedDict([(f, getattr(match, f),) for f in model_fields])
            m['record_id'] = getattr(match, 'record_id')
            m['entity_id'] = getattr(match, 'entity_id')
            # m['match_confidence'] = float(confs[str(m['entity_id'])])
            match_list.append(m)
        r['matches'] = match_list

    resp = make_response(json.dumps(r, default=_to_json, sort_keys=False), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@matching.route('/add-entity/', methods=['POST'])
@check_sessions()
def add_entity():
    ''' 
    Add an entry to the entity map. 
    POST data should be a string encoded JSON object which looks like:
    
    {
        "object": {
            "city":"Macon",
            "cont_name":"Kinght & Fisher, LLP",
            "zip":"31201",
            "firstname":null,
            "employer":null,
            "address":"350 Second St",
            "record_id":3,
            "type":"Monetary",
            "occupation":null
        },
        "api_key":"6bf73c41-404e-47ae-bc2d-051e935c298e",
        "match_id": 100,
    }

    The object key should contain a mapping of fields that are in the data
    model. If the record_id field is present, an attempt will be made to look
    up the record in the raw / processed table before making the entry. If
    match_id is present, the record will be added as a member of the entity
    referenced by the id.
    '''
    r = {
        'status': 'ok',
        'message': ""
    }
    status_code = 200
    session_id = flask_session['session_id']

    try:
        post = json.loads(request.data)
    except ValueError:
        r = {
            'status': 'error',
            'message': ''' 
                The content of your request should be a 
                string encoded JSON object.
            ''',
            'object': request.data,
        }
        resp = make_response(json.dumps(r), 400)
        resp.headers['Content-Type'] = 'application/json'
        return resp
    obj = post['object']
    record_id = obj.get('record_id')
    if record_id:
        del obj['record_id']
    match_ids = json.loads(request.data).get('match_ids')
    sess = db_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs)
    fds = {}
    for fd in field_defs:
        try:
            fds[fd['field']].append(fd['type'])
        except KeyError:
            fds[fd['field']] = [fd['type']]
    if not set(fds.keys()) == set(obj.keys()):
        r['status'] = 'error'
        r['message'] = "The fields in the object do not match the fields in the model"
        status_code = 400
    else:
        if match_ids:
            match_ids = match_ids.split(',')
        addToEntityMap(session_id, obj, match_ids=match_ids)
    if sess.review_count:
        sess.review_count = sess.review_count - 1
        db_session.add(sess)
        db_session.commit()
    resp = make_response(json.dumps(r), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@matching.route('/train/', methods=['POST'])
@check_sessions()
def train():
    try:
        post = json.loads(request.data)
    except ValueError:
        post = json.loads(request.form.keys()[0])
    r, status_code, sess = validate_post(post)
    # TODO: Check if model fields are present in matches
    if not post.get('matches'):
        r['status'] = 'error'
        r['message'] = 'List of matches is required'
        status_code = 400
    if r['status'] != 'error':
        api_key = post['api_key']
        session_id = post['session_id']
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
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@matching.route('/get-unmatched-record/')
@check_sessions()
def get_unmatched():
    resp = {
        'status': 'ok',
        'message': '',
        'remaining': 0,
    }
    status_code = 200
    session_id = flask_session['session_id']

    dedupe_session = db_session.query(DedupeSession).get(session_id)
    resp['remaining'] = dedupe_session.review_count
    dedupe_session.processing = True
    db_session.add(dedupe_session)
    db_session.commit()
    getNextHumanReview.delay(session_id)
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

