import os
import json
from flask import Flask, make_response, request, Blueprint, \
    session as flask_session, make_response, send_from_directory, jsonify
from api.models import DedupeSession, User, Group
from api.app_config import DOWNLOAD_FOLDER, TIME_ZONE
from api.queue import DelayedResult, redis
from api.database import app_session as db_session, engine, Base
from api.auth import csrf, check_sessions
from api.utils.delayed_tasks import dedupeCanon, getMatchingReady, cleanupTables
from api.utils.helpers import preProcess, getCluster, updateTraining, \
    updateSessionStatus, getMatchingDataDict
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from dedupe.convenience import canonicalize
from cPickle import loads
from cStringIO import StringIO
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from sqlalchemy import Table, select, and_, func, distinct, text
from sqlalchemy.ext.declarative import declarative_base
from itertools import groupby
from operator import itemgetter
from datetime import datetime, timedelta
from hashlib import md5
from unidecode import unidecode

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
    return r, status_code, sess

@csrf.exempt
@endpoints.route('/match/', methods=['POST'])
@check_sessions()
def match():
    print request.form
    try:
        post = json.loads(request.data)
    except ValueError:
        post = json.loads(request.form.keys()[0])
    user_sessions = flask_session['user_sessions']
    r, status_code, sess = validate_post(post, user_sessions)
    if r['status'] != 'error':
        api_key = post['api_key']
        session_id = post['session_key']
        n_matches = post.get('num_matches', 5)
        obj = post['object']
        field_defs = json.loads(sess.field_defs)
        model_fields = [f['field'] for f in field_defs]
        fields = ', '.join(['r.{0}'.format(f) for f in model_fields])
        entity_table = Table('entity_{0}'.format(session_id), Base.metadata, 
            autoload=True, autoload_with=engine, keep_existing=True)
        hash_me = ';'.join([preProcess(unicode(obj[i])) for i in model_fields])
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
                  LIMT :limit
                '''.format(fields, session_id), 
                entity_id=exact_match.entity_id, limit=n_matches)
            rows = []
            with engine.begin() as conn:
                rows = conn.execute(sel)
            for row in rows:
                d = {f: getattr(row, f) for f in model_fields}
                d['entity_id'] = exact_match.entity_id
                d['match_confidence'] = '1.0'
                match_list.append(d)
        else:
            deduper = dedupe.StaticGazetteer(StringIO(sess.gaz_settings_file))
            for k,v in obj.items():
                obj[k] = preProcess(unicode(v))
            o = {'blob': obj}
            data_d = getMatchingDataDict(sess.id)
            deduper.index(data_d)
            linked = deduper.match(o, threshold=0, n_matches=n_matches)
            if linked:
                ids = []
                confs = {}
                for l in linked[0]:
                    id_set, confidence = l
                    ids.extend([i for i in id_set if i != 'blob'])
                    confs[id_set[1]] = confidence
                ids = tuple(set(ids))
                sel = text(''' 
                      SELECT {0}, r.record_id, e.entity_id
                      FROM "raw_{1}" as r
                      JOIN "entity_{1}" as e
                        ON r.record_id = e.record_id
                      WHERE r.record_id IN :ids
                    '''.format(fields, session_id))
                matches = []
                with engine.begin() as conn:
                    matches = list(conn.execute(sel, ids=ids))
                for match in matches:
                    m = {f: getattr(match, f) for f in model_fields}
                    m['entity_id'] = getattr(match, 'entity_id')
                    # m['match_confidence'] = float(confs[str(m['entity_id'])])
                    match_list.append(m)
        r['matches'] = match_list

    resp = make_response(json.dumps(r, default=_to_json), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@csrf.exempt
@endpoints.route('/train/', methods=['POST'])
@check_sessions()
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
            #retrain.delay(session_id)
    resp = make_response(json.dumps(r))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/get-unmatched-record/<session_id>/')
@check_sessions()
def get_unmatched(session_id):
    resp = {
        'status': 'ok',
        'message': '',
        'object': {},
    }
    status_code = 200
    if session_id not in flask_session['user_sessions']:
        resp['status'] = 'error'
        resp['message'] = "You don't have access to session '{0}'".format(session_id)
        status_code = 401
    else:
        sess = db_session.query(DedupeSession).get(session_id)
        raw_fields = [f['field'] for f in json.loads(sess.field_defs)]
        raw_fields.append('record_id')
        fields = ', '.join(['r.{0}'.format(f) for f in raw_fields])
        sel = ''' 
          SELECT {0}
          FROM "raw_{1}" as r
          LEFT JOIN "entity_{1}" as e
            ON r.record_id = e.record_id
          WHERE e.record_id IS NULL
          LIMIT 1
        '''.format(fields, session_id)
        with engine.begin() as conn:
            rows = [dict(zip(raw_fields, r)) for r in conn.execute(sel)]
        resp['object'] = rows[0]
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
        with engine.begin() as c:
            c.execute(upd)
    return None

@endpoints.route('/get-review-cluster/<session_id>/')
@check_sessions()
def get_cluster(session_id):
    resp = {
        'status': 'ok',
        'message': '',
        'objects': [],
    }
    status_code = 200
    if session_id not in flask_session['user_sessions']:
        resp['status'] = 'error'
        resp['message'] = "You don't have access to session '{0}'".format(session_id)
        status_code = 401
    else:
        sess = db_session.query(DedupeSession).get(session_id)
        checkin_sessions()
        entity_id, cluster = getCluster(session_id, 
                             'entity_{0}', 
                             'raw_{0}')
        if cluster:
            resp['entity_id'] = entity_id 
            resp['objects'] = cluster
        else:
            sess.status = 'first pass review complete'
            dedupeCanon.delay(sess.id)
            db_session.add(sess)
            db_session.commit()
        resp['total_clusters'] = sess.entity_count
        resp['review_remainder'] = sess.review_count
       #resp['total_clusters'] = total_clusters
       #resp['review_remainder'] = review_remainder
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@endpoints.route('/get-canon-review-cluster/<session_id>/')
@check_sessions()
def get_canon_cluster(session_id):
    resp = {
        'status': 'ok',
        'message': '',
        'objects': [],
    }
    status_code = 200
    if session_id not in flask_session['user_sessions']:
        resp['status'] = 'error'
        resp['message'] = "You don't have access to session '{0}'".format(session_id)
        status_code = 401
    else:
        checkin_sessions()
        sess = db_session.query(DedupeSession).get(session_id)
        entity_id, cluster = getCluster(session_id, 
                             'entity_{0}_cr', 
                             'cr_{0}')
        if cluster:
            resp['entity_id'] = entity_id
            resp['objects'] = cluster
        resp['total_clusters'] = sess.entity_count
        resp['review_remainder'] = sess.review_count
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@endpoints.route('/mark-all-clusters/<session_id>/')
@check_sessions()
def mark_all_clusters(session_id):
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    if session_id not in flask_session['user_sessions']:
        resp['status'] = 'error'
        resp['message'] = "You don't have access to session '%s'" % session_id
        status_code = 401
    else:
        # Need to update existing clusters with new entity_id here, too.
        user = db_session.query(User).get(flask_session['api_key']) 
        now =  datetime.now().replace(tzinfo=TIME_ZONE)
        upd_vals = {
            'user_name': user.name, 
            'clustered': True,
            'match_type': 'bulk accepted',
            'last_update': now,
        }
        upd = text(''' 
            UPDATE "entity_{0}" SET 
                entity_id=subq.entity_id,
                clustered= :clustered,
                reviewer = :user_name,
                match_type = :match_type,
                last_update = :last_update
            FROM (
                    SELECT 
                        s.entity_id AS entity_id,
                        e.record_id 
                    FROM "entity_{0}" AS e
                    JOIN (
                        SELECT 
                            record_id, 
                            entity_id
                        FROM "entity_{0}"
                    ) AS s
                        ON e.target_record_id = s.record_id
                ) as subq 
            WHERE "entity_{0}".record_id=subq.record_id 
                AND ( "entity_{0}".clustered=FALSE 
                      OR "entity_{0}".match_type != 'clerical review' )
            RETURNING "entity_{0}".entity_id
            '''.format(session_id))
        with engine.begin() as c:
            child_entities = c.execute(upd, **upd_vals)
        upd = text(''' 
            UPDATE "entity_{0}" SET
                clustered = :clustered,
                reviewer = :user_name,
                last_update = :last_update,
                match_type = :match_type
            WHERE target_record_id IS NULL
                AND clustered=FALSE
            RETURNING entity_id;
        '''.format(session_id))
        with engine.begin() as c:
            parent_entities = c.execute(upd, **upd_vals)
        child_entities = set([c.entity_id for c in child_entities])
        parent_entities = set([p.entity_id for p in parent_entities])
        count = len(child_entities.union(parent_entities))
        sess = db_session.query(DedupeSession).get(session_id)
        sess.review_count = 0
        sess.entity_count = count
        db_session.add(sess)
        db_session.commit()
        resp['message'] = 'Marked {0} entities as clusters'.format(count)
        dedupeCanon.delay(session_id)
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response
    
@endpoints.route('/mark-cluster/<session_id>/')
@check_sessions()
def mark_cluster(session_id):
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    if session_id not in flask_session['user_sessions']:
        resp['status'] = 'error'
        resp['message'] = "You don't have access to session '%s'" % session_id
        status_code = 401
    else:
        sess = db_session.query(DedupeSession).get(session_id)
        user = db_session.query(User).get(flask_session['api_key'])
        entity_table = Table('entity_{0}'.format(session_id), Base.metadata,
            autoload=True, autoload_with=engine)
        # TODO: Return an error if these args are not present.
        entity_id = request.args.get('entity_id')
        match_ids = request.args.get('match_ids')
        distinct_ids = request.args.get('distinct_ids')
        training_data = json.loads(sess.training_data)
        if match_ids:
            match_ids = tuple([int(m) for m in match_ids.split(',')])
            upd = entity_table.update()\
                .where(entity_table.c.entity_id == entity_id)\
                .where(entity_table.c.record_id.in_(match_ids))\
                .values(clustered=True, 
                        checked_out=False, 
                        checkout_expire=None,
                        last_update=datetime.now().replace(tzinfo=TIME_ZONE),
                        reviewer=user.name)
            with engine.begin() as c:
                c.execute(upd)
            upd_vals = {
                'entity_id': entity_id,
                'record_ids': match_ids,
                'user_name': user.name, 
                'clustered': True,
                'match_type': 'clerical review',
                'last_update': datetime.now().replace(tzinfo=TIME_ZONE)
            }
            update_existing = text('''
                UPDATE "entity_{0}" SET 
                    entity_id = :entity_id, 
                    clustered = :clustered,
                    reviewer = :user_name,
                    match_type = :match_type,
                    last_update = :last_update
                    FROM (
                        SELECT e.record_id 
                            FROM "entity_{0}" AS e 
                            JOIN (
                                SELECT record_id 
                                    FROM "entity_{0}"
                                    WHERE entity_id = :entity_id
                                        AND record_id IN :record_ids
                            ) AS s 
                            ON e.target_record_id = s.record_id
                    ) AS subq 
                WHERE "entity_{0}".record_id = subq.record_id
                '''.format(sess.id))
            with engine.begin() as c:
                c.execute(update_existing,**upd_vals)
            # training_data['match'].extend(pairs)
        if distinct_ids:
            distinct_ids = tuple([int(d) for d in distinct_ids.split(',')])
            delete = entity_table.delete()\
                .where(entity_table.c.entity_id == entity_id)\
                .where(entity_table.c.record_id.in_(distinct_ids))
            with engine.begin() as c:
                c.execute(delete)
            #training_data['distinct'].append(pairs)
        sess.review_count = sess.review_count - 1
        db_session.add(sess)
        db_session.commit()
        resp = {
            'session_id': session_id, 
            'entity_id': entity_id, 
            'match_ids': match_ids,
            'distinct_ids': distinct_ids,
            'status': 'ok', 
            'message': ''
        }
        status_code = 200
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/mark-canon-cluster/<session_id>/')
@check_sessions()
def mark_canon_cluster(session_id):
    user_sessions = flask_session['user_sessions']
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        status_code = 401
    elif not request.args.get('entity_id'):
        resp = {
            'status': 'error',
            'message': '"entity_id" is a required parameter'
        }
        status_code = 401
    else:
        entity_id = request.args.get('entity_id')
        match_ids = request.args.get('match_ids')
        distinct_ids = request.args.get('distinct_ids')
        user = db_session.query(User).get(flask_session['api_key'])
        if match_ids:
            match_ids = tuple([d for d in match_ids.split(',')])
            upd = text('''
                UPDATE "entity_{0}" SET 
                    entity_id = :entity_id,
                    clustered = TRUE,
                    checked_out = FALSE,
                    last_update = :last_update,
                    reviewer = :user_name
                WHERE entity_id in (
                    SELECT record_id 
                        FROM "entity_{0}_cr"
                    WHERE entity_id = :entity_id
                        AND record_id IN :record_ids
                )
                '''.format(session_id))
            last_update = datetime.now().replace(tzinfo=TIME_ZONE)
            with engine.begin() as c:
                c.execute(upd, 
                          entity_id=entity_id, 
                          last_update=last_update,
                          user_name=user.name,
                          record_ids=match_ids)
        if distinct_ids:
            distinct_ids = tuple([d for d in distinct_ids.split(',')])
            delete = text(''' 
                DELETE FROM "entity_{0}_cr"
                WHERE entity_id = :entity_id
                    AND record_id IN :record_ids
            '''.format(session_id))
            with engine.begin() as c:
                c.execute(delete, entity_id=entity_id, record_ids=distinct_ids)
        resp = {
            'session_id': session_id, 
            'entity_id': entity_id,
            'match_ids': match_ids,
            'distinct_ids': distinct_ids,
            'status': 'ok', 
            'message': ''
        }
        status_code = 200
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/mark-all-canon-clusters/<session_id>/')
@check_sessions()
def mark_all_canon_cluster(session_id):
    user_sessions = flask_session['user_sessions']
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        status_code = 401
    else:
        status_code = 200
        user = db_session.query(User).get(flask_session['api_key'])
        upd_vals = {
            'user_name': user.name, 
            'clustered': True,
            'match_type': 'bulk accepted - canon',
            'last_update': datetime.now().replace(tzinfo=TIME_ZONE)
        }
        upd = text(''' 
            UPDATE "entity_{0}" SET 
                entity_id=subq.entity_id,
                clustered= :clustered,
                reviewer = :user_name,
                match_type = :match_type,
                last_update = :last_update
            FROM (
                SELECT 
                    c.entity_id, 
                    e.record_id 
                FROM "entity_{0}" as e
                JOIN "entity_{0}_cr" as c 
                    ON e.entity_id = c.record_id 
                LEFT JOIN (
                    SELECT record_id, target_record_id FROM "entity_{0}"
                    ) AS s 
                    ON e.record_id = s.target_record_id
                ) as subq 
            WHERE "entity_{0}".record_id=subq.record_id 
            RETURNING "entity_{0}".entity_id
            '''.format(session_id))
        with engine.begin() as c:
            updated = c.execute(upd,**upd_vals)
        resp = {
            'session_id': session_id,
            'status': 'ok',
            'message': '',
        }
        count = len(set([c.entity_id for c in updated]))
        resp['message'] = 'Marked {0} entities as clusters'.format(count)
        getMatchingReady.delay(session_id)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/training-data/<session_id>/')
@check_sessions()
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
        resp.headers['Content-Disposition'] = 'attachment; filename=%s_training.json' % data.id
    return resp

@endpoints.route('/settings-file/<session_id>/')
@check_sessions()
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
        resp.headers['Content-Disposition'] = 'attachment; filename=%s.dedupe_settings' % data.id
    return resp

@endpoints.route('/field-definitions/<session_id>/')
@check_sessions()
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

@endpoints.route('/delete-data-model/<session_id>/')
@check_sessions()
def delete_data_model(session_id):
    user_sessions = flask_session['user_sessions']
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        status_code = 401
    else:
        sess = db_session.query(DedupeSession).get(session_id)
        sess.field_defs = None
        sess.training_data = None
        sess.sample = None
        sess.status = 'session initialized'
        db_session.add(sess)
        db_session.commit()
        tables = [
            'entity_{0}',
            'block_{0}',
            'plural_block_{0}',
            'covered_{0}',
            'plural_key_{0}',
            'small_cov_{0}',
        ]
        for table in tables:
            try:
                data_table = Table(table.format(session_id), 
                    Base.metadata, autoload=True, autoload_with=engine)
                data_table.drop(engine)
            except NoSuchTableError:
                pass
            except ProgrammingError:
                pass
        resp = {
            'status': 'ok',
            'message': 'Data model for session {0} deleted'.format(session_id)
        }
        status_code = 200
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/delete-session/<session_id>/')
@check_sessions()
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
        tables = [
            'entity_{0}',
            'entity_{0}_cr',
            'raw_{0}',
            'processed_{0}',
            'processed_{0}_cr',
            'block_{0}',
            'block_{0}_cr',
            'plural_block_{0}',
            'plural_block_{0}_cr',
            'cr_{0}',
            'covered_{0}',
            'covered_{0}_cr',
            'plural_key_{0}',
            'plural_key_{0}_cr',
            'small_cov_{0}',
            'small_cov_{0}_cr',
            'canon_{0}',
            'exact_match_{0}',
        ]
        cleanupTables.delay(session_id, tables=tables)
        resp = make_response(json.dumps({'session_id': session_id, 'status': 'ok'}))
        resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/session-list/')
@check_sessions()
def review():
    user_sessions = flask_session['user_sessions']
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    sess_id = request.args.get('session_id')
    all_sessions = []
    if not sess_id:
        sessions = db_session.query(DedupeSession)\
            .filter(DedupeSession.id.in_(user_sessions))\
            .all()
        for sess in sessions:
            s = sess.as_dict()
            all_sessions.append(s)
    else:
        if sess_id in user_sessions:
            sess = db_session.query(DedupeSession).get(sess_id)
            s = sess.as_dict()
            all_sessions.append(s)
    resp['objects'] = all_sessions
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@csrf.exempt
@endpoints.route('/bulk-match/<session_id>/', methods=['POST'])
@check_sessions()
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
@check_sessions()
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
