import json
from datetime import datetime
from flask import Flask, make_response, request, Blueprint, \
    session as flask_session, render_template, current_app
from api.database import app_session as db_session, Base
from api.models import User, DedupeSession
from api.auth import login_required, check_roles, check_sessions
from api.utils.helpers import checkinSessions, getCluster, updateTraining
from api.utils.delayed_tasks import bulkMarkClusters, bulkMarkCanonClusters, \
    dedupeCanon, getMatchingReady
from api.app_config import TIME_ZONE
from sqlalchemy import text, Table
from cPickle import loads, dumps

review = Blueprint('review', __name__)

@review.route('/match-review/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
@check_sessions()
def match_review(): # pragma: no cover
    dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])
    return render_template('dedupe_session/match-review.html', 
                            session_id=flask_session['session_id'], 
                            dedupe_session=dedupe_session)

@review.route('/session-review/')
@login_required
@check_roles(roles=['admin', 'reviewer'])
@check_sessions()
def session_review():
    first_review = True
    if request.args.get('second_review'):
        first_review = False
    
    dedupe_session = db_session.query(DedupeSession).get(flask_session['session_id'])

    return render_template('dedupe_session/session-review.html', 
                            session_id=flask_session['session_id'],
                            first_review=first_review,
                            dedupe_session=dedupe_session)

@review.route('/get-review-cluster/')
@check_sessions()
def get_cluster():
    resp = {
        'status': 'ok',
        'message': '',
        'objects': [],
    }
    status_code = 200
    session_id = flask_session['session_id']

    dedupe_session = db_session.query(DedupeSession).get(session_id)
    checkinSessions()
    entity_id, cluster, false_pos, false_neg = getCluster(session_id, 
                         'entity_{0}', 
                         'raw_{0}')
    if cluster:
        resp['entity_id'] = entity_id 
        resp['objects'] = cluster
        resp['false_positive'] = false_pos
        resp['false_negative'] = false_neg
    else:
        dedupe_session.processing = True
        db_session.add(dedupe_session)
        db_session.commit()
        dedupeCanon.delay(dedupe_session.id)
    resp['total_clusters'] = dedupe_session.entity_count
    resp['review_remainder'] = dedupe_session.review_count

    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@review.route('/get-canon-review-cluster/')
@check_sessions()
def get_canon_cluster():
    resp = {
        'status': 'ok',
        'message': '',
        'objects': [],
    }
    status_code = 200
    session_id = flask_session['session_id']
    
    checkinSessions()
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    entity_id, cluster, false_pos, false_neg = getCluster(session_id, 
                         'entity_{0}_cr', 
                         'cr_{0}')
    if cluster:
        resp['entity_id'] = entity_id
        resp['objects'] = cluster
        resp['false_positive'] = false_pos
        resp['false_negative'] = false_neg
    else:
        dedupe_session.processing = True
        db_session.add(dedupe_session)
        db_session.commit()
        getMatchingReady.delay(session_id)
    resp['total_clusters'] = dedupe_session.entity_count
    resp['review_remainder'] = dedupe_session.review_count

    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@review.route('/mark-all-clusters/')
@check_sessions()
def mark_all_clusters():
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200

    session_id = flask_session['session_id']
    # Need to update existing clusters with new entity_id here, too.
    user = db_session.query(User).get(flask_session['api_key']) 
    dedupe_session = db_session.query(DedupeSession).get(session_id) 
    dedupe_session.processing = True
    db_session.add(dedupe_session)
    db_session.commit()
    bulkMarkClusters.delay(session_id, user=user.name)

    response = make_response(json.dumps(resp), status_code)
    response.headers['Content-Type'] = 'application/json'
    return response

@review.route('/mark-cluster/')
@check_sessions()
def mark_cluster():
    resp = {
        'status': 'ok',
        'message': ''
    }
    status_code = 200
    session_id = flask_session['session_id']

    user = db_session.query(User).get(flask_session['api_key'])
    engine = db_session.bind
    # TODO: Return an error if these args are not present.
    entity_id = request.args.get('entity_id')
    match_ids = request.args.get('match_ids')
    distinct_ids = request.args.get('distinct_ids')
    if match_ids:
        ids = tuple([int(m) for m in match_ids.split(',')])
        upd_vals = {
            'entity_id': entity_id,
            'record_ids': ids,
            'user_name': user.name, 
            'clustered': True,
            'match_type': 'clerical review',
            'last_update': datetime.now().replace(tzinfo=TIME_ZONE), 
            'match_ids': ids,
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
            '''.format(session_id))
        with engine.begin() as c:
            c.execute(update_existing,**upd_vals)
    if distinct_ids:
        entity_table = Table('entity_{0}'.format(session_id), Base.metadata,
            autoload=True, autoload_with=engine)
        ids = tuple([int(d) for d in distinct_ids.split(',')])
        delete = entity_table.delete()\
            .where(entity_table.c.entity_id == entity_id)\
            .where(entity_table.c.record_id.in_(ids))
        with engine.begin() as c:
            c.execute(delete)
    distinct_ids = [d for d in distinct_ids.split(',') if d]
    match_ids = [m for m in match_ids.split(',') if m]
    updateTraining(session_id, 
                   match_ids=match_ids, 
                   distinct_ids=distinct_ids)
    dedupe_session = db_session.query(DedupeSession).get(session_id)
    machine = loads(dedupe_session.review_machine)
    if distinct_ids:
        machine.label(entity_id, 0)
    else:
        machine.label(entity_id, 1)
    dedupe_session.review_machine = dumps(machine)
    dedupe_session.review_count = dedupe_session.review_count - 1
    db_session.add(dedupe_session)
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

@review.route('/mark-canon-cluster/')
@check_sessions()
def mark_canon_cluster():
    session_id = flask_session['session_id']

    if not request.args.get('entity_id'):
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
                '''.format(session_id))
            upd_cr = text(''' 
                UPDATE "entity_{0}_cr" SET
                    target_record_id = :entity_id,
                    clustered = :clustered,
                    checked_out = :checked_out,
                    last_update = :last_update,
                    reviewer = :user_name
                WHERE record_id IN :record_ids
            '''.format(session_id))
            last_update = datetime.now().replace(tzinfo=TIME_ZONE)
            with engine.begin() as c:
                c.execute(upd, 
                          entity_id=entity_id, 
                          last_update=last_update,
                          user_name=user.name,
                          record_ids=match_ids,
                          clustered=True,
                          checked_out=False)
                c.execute(upd_cr, 
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
        dedupe_session = db_session.query(DedupeSession).get(session_id)
        machine = loads(dedupe_session.review_machine)
        if distinct_ids:
            machine.label(entity_id, 0)
        else:
            machine.label(entity_id, 1)
        dedupe_session.review_machine = dumps(machine)
        dedupe_session.review_count = dedupe_session.review_count - 1
        db_session.add(dedupe_session)
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

@review.route('/mark-all-canon-clusters/')
@check_sessions()
def mark_all_canon_cluster():
    resp = {}
    status_code = 200
    session_id = flask_session['session_id']
    user = db_session.query(User).get(flask_session['api_key'])
    dedupe_session = db_session.query(DedupeSession).get(session_id) 
    dedupe_session.processing = True
    db_session.add(dedupe_session)
    db_session.commit()
    bulkMarkCanonClusters.delay(session_id, user=user.name)

    resp = make_response(json.dumps(resp), status_code)
    resp.headers['Content-Type'] = 'application/json'
    return resp
