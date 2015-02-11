from flask import current_app
import re
import os
import json
from dedupe.core import frozendict
from dedupe import canonicalize
from api.database import app_session, worker_session, Base, init_engine
from api.models import DedupeSession
from sqlalchemy import Table, MetaData, distinct, and_, func, Column, text
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from unidecode import unidecode
from unicodedata import normalize
from itertools import count
from csvkit.unicsv import UnicodeCSVDictWriter
from csv import QUOTE_ALL
from datetime import datetime, timedelta
from unidecode import unidecode
import cPickle
from itertools import combinations

STATUS_LIST = [
    {
        'machine_name' : 'dataset uploaded',
        'human_name': 'Dataset uploaded', 
        'next_step_name': 'Fields to compare',
        'next_step': '/select-fields/?session_id={0}',
        'step': 1
    },
    {
        'machine_name': 'model defined',
        'human_name': 'Model defined', 
        'next_step_name': 'Train',
        'next_step': '/training-run/?session_id={0}',
        'step': 2
    },
    {
        'machine_name': 'entity map updated',
        'human_name': 'Training finished', 
        'next_step_name': 'Review entites',
        'next_step': '/session-review/?session_id={0}',
        'step': 3
    },
    {
        'machine_name': 'canon clustered',
        'human_name': 'Clusters reviewed', 
        'next_step_name': 'Merge entities',
        'next_step': '/session-review/?session_id={0}&second_review=True',
        'step': 4
    },
    {
        'machine_name': 'matching ready',
        'human_name': 'Clusters merged', 
        'next_step_name': 'Final review',
        'next_step': '/match-review/?session_id={0}',
        'step': 5
    },
    {
        'machine_name':'canonical',
        'human_name': 'Dataset is canonical', 
        'next_step_name': 'Ready for matching!',
        'next_step': '/session-admin/?session_id={0}',
        'step': 6
    },
]

def updateTraining(session_id, distinct_ids=[], match_ids=[]):
    ''' 
    Update the sessions training data with the given record_ids
    '''
    sess = worker_session.query(DedupeSession).get(session_id)
    worker_session.refresh(sess)
    engine = worker_session.bind
    training = {'distinct': [], 'match': []}
    
    all_ids = tuple([i for i in distinct_ids + match_ids])
    sel = text(''' 
        SELECT * FROM "processed_{0}" 
        WHERE record_id IN :record_ids
    '''.format(session_id))
    all_records = {r.record_id: dict(zip(r.keys(), r.values())) \
            for r in engine.execute(sel, record_ids=all_ids)}

    if sess.training_data:
        training = json.loads(sess.training_data)
    if distinct_ids and match_ids:
        distinct_ids.extend(match_ids)
    
    distinct_combos = []
    match_combos = []
    if distinct_ids:
        distinct_combos = combinations(distinct_ids, 2)
    if match_ids:
        match_combos = combinations(match_ids, 2)
    
    distinct_records = []
    for combo in distinct_combos:
        combo = tuple([int(c) for c in combo])
        records = [all_records[combo[0]], all_records[combo[1]]]
        distinct_records.append(records)
    training['distinct'].extend(distinct_records)
    
    match_records = []
    for combo in match_combos:
        combo = tuple([int(c) for c in combo])
        records = [all_records[combo[0]], all_records[combo[1]]]
        match_records.append(records)

    training['match'].extend(match_records)
    sess.training_data = json.dumps(training)
    worker_session.add(sess)
    worker_session.commit()
    return None

def getCluster(session_id, entity_pattern, raw_pattern):
    ent_name = entity_pattern.format(session_id)
    raw_name = raw_pattern.format(session_id)
    sess = app_session.query(DedupeSession).get(session_id)
    app_session.refresh(sess)
    
    cluster_list = []
    prediction = None
    machine = cPickle.loads(sess.review_machine)
    entity_id = machine.get_next()
    if entity_id:
        sess.review_machine = cPickle.dumps(machine)
        app_session.add(sess)
        app_session.commit()
        engine = app_session.bind
        model_fields = list(set([f['field'] for f in json.loads(sess.field_defs)]))
        raw_cols = ', '.join(['r.{0}'.format(f) for f in model_fields])
        sel = text('''
            SELECT 
                e.confidence,
                {0},
                r.record_id
            FROM "{1}" AS r
            JOIN "{2}" as e 
                ON r.record_id = e.record_id
            WHERE e.entity_id = :entity_id
            ORDER BY e.confidence
            '''.format(raw_cols, raw_name, ent_name))
        records = list(engine.execute(sel, entity_id=entity_id))
 
        if records:
            raw_fields = ['confidence'] + model_fields + ['record_id']
            false_pos, false_neg = machine.predict_remainder()
            for thing in records:
                d = {}
                for k,v in zip(raw_fields, thing):
                    d[k] = v
                cluster_list.append(d)
            one_minute = datetime.now() + timedelta(minutes=1)
            upd = text(''' 
                UPDATE "{0}" SET
                  checked_out = TRUE,
                  checkout_expire = :one_minute
                WHERE entity_id = :entity_id
                '''.format(ent_name))
            with engine.begin() as c:
                c.execute(upd, entity_id=entity_id, one_minute=one_minute)
            return entity_id, cluster_list, false_pos, false_neg
    return None, None, None, None

def column_windows(session, column, windowsize):
    def int_for_range(start_id, end_id):
        if end_id: # pragma: no cover
            return and_(
                column>=start_id,
                column<end_id
            )
        else:
            return column>=start_id

    q = session.query(
                column, 
                func.row_number().\
                        over(order_by=column).\
                        label('rownum')
                ).\
                from_self(column)
    if windowsize > 1:
        q = q.filter("rownum %% %d=1" % windowsize)

    intervals = [id for id, in q]

    while intervals:
        start = intervals.pop(0)
        if intervals: # pragma: no cover
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)

def windowed_query(q, column, windowsize):
    ''' 
    Details on how this works can be found here:
    https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/WindowedRangeQuery
    '''
    for whereclause in column_windows(q.session, 
                                        column, windowsize):
        for row in q.filter(whereclause).order_by(column):
            yield row

def slugify(text, delim=u'_'):
    if text:
        text = unicode(text)
        punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.:;]+')
        result = []
        for word in punct_re.split(text.lower()):
            word = normalize('NFKD', word).encode('ascii', 'ignore')
            if word:
                result.append(word)
        return unicode(delim.join(result))
    else: # pragma: no cover
        return text

def preProcess(column):
    if not column:
        column = u''
    if column == None:
        column = u''
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def clusterGen(result_set, fields):
    lset = set
    block_id = None
    records = []
    for row in result_set:
        row = dict(zip(fields, row))
        if row['block_id'] != block_id:
            if records:
                yield records
            block_id = row['block_id']
            records = []
        smaller_ids = row['smaller_ids']
        if smaller_ids:
            smaller_ids = lset(smaller_ids.split(','))
        else:
            smaller_ids = lset([])
        records.append((row['record_id'], row, smaller_ids))
    if records:
        yield records

def makeSampleDict(session_id):
    session = worker_session
    engine = session.bind
    sel = ''' 
        SELECT p.*
        FROM "processed_{0}" as p
        LEFT JOIN "entity_{0}" as e
            ON p.record_id = e.record_id
        WHERE e.target_record_id IS NULL
    '''.format(session_id)
    curs = engine.execute(sel)
    result = dict((i, frozendict(zip(row.keys(), row.values()))) 
                            for i, row in enumerate(curs))
    return result

def getDistinct(field_name, session_id):
    engine = app_session.bind
    sel = ''' 
        SELECT DISTINCT {0}
        FROM "processed_{1}"
        WHERE {0} IS NOT NULL
            AND {0} != ''
    '''.format(field_name, session_id)
    distinct_values = list(set([unicode(v[0]) for v in engine.execute(sel)]))
    return distinct_values

def checkinSessions():
    now = datetime.now()
    all_sessions = [i.id for i in app_session.query(DedupeSession.id).all()]
    engine = init_engine(current_app.config['DB_CONN'])
    for sess_id in all_sessions:
        try:
            table = Table('entity_%s' % sess_id, Base.metadata, 
                autoload=True, autoload_with=engine)
            upd = table.update().where(table.c.checkout_expire <= now)\
                .where(table.c.clustered == False)\
                .values(checked_out = False, checkout_expire = None)
            with engine.begin() as c:
                c.execute(upd)
        except (NoSuchTableError, ProgrammingError): # pragma: no cover 
            pass
    return None

def formatPercentage(num):
    return "{0:.0f}%".format(float(num) * 100)
