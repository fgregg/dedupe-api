import json
from datetime import datetime
from flask import Flask, make_response, request, Blueprint, \
    session as flask_session, render_template, current_app
from api.database import app_session as db_session, Base
from api.models import User, DedupeSession
from api.auth import login_required, check_roles, check_sessions
from api.utils.helpers import checkinSessions, getCluster
from api.utils.delayed_tasks import bulkMarkClusters, bulkMarkCanonClusters
from api.app_config import TIME_ZONE
from sqlalchemy import text, Table

review = Blueprint('review', __name__)

@review.route('/match-review/<session_id>/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def match_review(session_id): # pragma: no cover
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('match-review.html', 
                            session_id=session_id, 
                            user=user)

@review.route('/session-review/<session_id>/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
def session_review(session_id):
    first_review = True
    if request.args.get('second_review'):
        first_review = False
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('session-review.html', 
                            session_id=session_id, 
                            user=user, 
                            first_review=first_review)

@review.route('/get-review-cluster/<session_id>/')
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
        checkinSessions()
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
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@review.route('/get-canon-review-cluster/<session_id>/')
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
        checkinSessions()
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

@review.route('/mark-all-clusters/<session_id>/')
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
        bulkMarkClusters.delay(session_id, user=user.name)
    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@review.route('/mark-cluster/<session_id>/')
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
        engine = db_session.bind
        entity_table = Table('entity_{0}'.format(session_id), Base.metadata,
            autoload=True, autoload_with=engine)
        # TODO: Return an error if these args are not present.
        entity_id = request.args.get('entity_id')
        match_ids = request.args.get('match_ids')
        distinct_ids = request.args.get('distinct_ids')
        training_data = json.loads(sess.training_data)
        if match_ids:
            match_ids = tuple([int(m) for m in match_ids.split(',')])
            upd_vals = {
                'entity_id': entity_id,
                'record_ids': match_ids,
                'user_name': user.name, 
                'clustered': True,
                'match_type': 'clerical review',
                'last_update': datetime.now().replace(tzinfo=TIME_ZONE), 
                'match_ids': match_ids,
            }
            upd = text(''' 
                UPDATE "entity_{0}" SET
                  entity_id = :entity_id,
                  reviewer = :user_name,
                  clustered = :clustered,
                  match_type = :match_type,
                  last_update = :last_update
                WHERE entity_id = :entity_id
                  AND record_id IN :match_ids
            '''.format(session_id))
            with engine.begin() as conn:
                conn.execute(upd, **upd_vals)
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

@review.route('/mark-canon-cluster/<session_id>/')
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
        status_code = 400
    else:
        entity_id = request.args.get('entity_id')
        match_ids = request.args.get('match_ids')
        distinct_ids = request.args.get('distinct_ids')
        user = db_session.query(User).get(flask_session['api_key'])
        engine = db_session.bind
        if match_ids:
            match_ids = tuple([d for d in match_ids.split(',')])
            upd = text('''
                UPDATE "entity_{0}" SET 
                    entity_id = :entity_id,
                    clustered = :clustered,
                    checked_out = :checked_out,
                    last_update = :last_update,
                    reviewer = :user_name
                WHERE entity_id in (
                    SELECT record_id 
                        FROM "entity_{0}_cr"
                    WHERE entity_id = :entity_id
                        AND record_id IN :record_ids
                )
                RETURNING record_id
                '''.format(session_id))
            last_update = datetime.now().replace(tzinfo=TIME_ZONE)
            with engine.begin() as c:
                ids = c.execute(upd, 
                          entity_id=entity_id, 
                          last_update=last_update,
                          user_name=user.name,
                          record_ids=match_ids,
                          clustered=True,
                          checked_out=False)
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

@review.route('/mark-all-canon-clusters/<session_id>/')
@check_sessions()
def mark_all_canon_cluster(session_id):
    user_sessions = flask_session['user_sessions']
    resp = {}
    if session_id not in user_sessions:
        resp = {
            'status': 'error', 
            'message': "You don't have access to session %s" % session_id
        }
        status_code = 401
    else:
        status_code = 200
        user = db_session.query(User).get(flask_session['api_key'])
        bulkMarkCanonClusters.delay(session_id, user=user.name)
    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp
