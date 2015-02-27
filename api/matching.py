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
from api.utils.helpers import preProcess, getMatches, updateTraining
from api.utils.db_functions import addToEntityMap
from api.utils.delayed_tasks import populateHumanReview
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
from flask_login import current_user

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
        add_entity = post.get('add_entity', False)
        # positive = []
        # negative = []
        match_ids = []
        distinct_ids = []
        for match in post['matches']:
            if match['match'] is 1:
                # positive.append(match)
                if match.get('record_id'):
                    match_ids.append(match['record_id'])
            else:
                # negative.append(match)
                if match.get('record_id'):
                    distinct_ids.append(match['record_id'])
            for k,v in match.items():
                match[k] = preProcess(unicode(v))
            del match['match']

        # Assuming for the time being that all of the incoming training pairs 
        # already exist in the raw data table. This will need to be updated
        # to allow us to add new training that does not already exist in the
        # raw data
        updateTraining(session_id, 
                       distinct_ids=distinct_ids, 
                       match_ids=match_ids)
        if add_entity:
            user = db_session.query(User).get(api_key)
            addToEntityMap(session_id, obj, match_ids=match_ids, reviewer=user.name)
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
    session_id = request.args['session_id']
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    resp['remaining'] = dedupe_session.review_count
    fields = set([f['field'] for f in json.loads(dedupe_session.field_defs)])
    fields.add('record_id')
    match_fields = ','.join(['MAX(match.{0}) AS match_{0}'.format(f) for f in fields])
    raw_fields = ','.join(['MAX(raw.{0}) AS raw_{0}'.format(f) for f in fields])
    sel = '''
        SELECT 
          {0}, 
          {1}, 
          ent.entity_id, 
          MAX(ent.confidence) AS confidence
        FROM "raw_{2}" AS match 
        JOIN (
          SELECT 
            e.record_id, 
            e.entity_id, 
            s.record_id as match_record_id, 
            s.confidence[(
              SELECT i 
              FROM generate_subscripts(s.entities, 1) AS i
              WHERE s.entities[i] = e.entity_id
            )] as confidence
          FROM "entity_{2}" AS e, 
          (
            SELECT record_id, entities, confidence 
              FROM "match_review_{2}" 
            WHERE array_upper(entities, 1) IS NOT NULL
              AND reviewed = FALSE
            LIMIT 1
          ) AS s 
          WHERE e.entity_id = ANY(s.entities)
        ) AS ent 
          ON match.record_id = ent.record_id 
        JOIN "raw_{2}" AS raw 
          ON ent.match_record_id = raw.record_id 
        GROUP BY ent.entity_id
        ORDER BY MAX(ent.confidence) DESC
    '''.format(match_fields, raw_fields, session_id)
    engine = db_session.bind
    records = list(engine.execute(sel))
    if records:
        populateHumanReview.delay(session_id)
    matches = []
    raw_record = {}
    for record in records:
        match = {}
        for key in record.keys():
            if key.startswith('raw_'):
                raw_record[key.replace('raw_', '')] = getattr(record, key)
            elif key.startswith('match_'):
                match[key.replace('match_', '')] = getattr(record, key)
        match = OrderedDict(sorted(match.items()))
        raw_record = OrderedDict(sorted(raw_record.items()))
        match['entity_id'] = record.entity_id
        match['confidence'] = record.confidence
        matches.append(match)
    resp['object'] = raw_record
    resp['matches'] = matches
    response = make_response(json.dumps(resp, sort_keys=False), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

