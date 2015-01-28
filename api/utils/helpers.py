from flask import current_app
import re
import os
import json
from dedupe.core import frozendict
from dedupe import canonicalize
from api.database import app_session, worker_session, Base, init_engine
from api.models import DedupeSession
from sqlalchemy import Table, MetaData, distinct, and_, func, Column, text
from sqlalchemy.exc import NoSuchTableError
from unidecode import unidecode
from unicodedata import normalize
from itertools import count
from csvkit.unicsv import UnicodeCSVDictWriter
from csv import QUOTE_ALL
from datetime import datetime, timedelta
from unidecode import unidecode
import cPickle

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

def updateTraining(session_id, record_ids, distinct=False):
    ''' 
    Update the sessions training data with the given record_ids
    '''
    return None

def getCluster(session_id, entity_pattern, raw_pattern):
    ent_name = entity_pattern.format(session_id)
    raw_name = raw_pattern.format(session_id)
    sess = app_session.query(DedupeSession).get(session_id)

    cluster_list = []
    model_fields = list(set([f['field'] for f in json.loads(sess.field_defs)]))
    entity_fields = ['record_id', 'entity_id', 'confidence']
    sel = ''' 
        SELECT e.entity_id 
            FROM "{0}" AS e
        WHERE e.checked_out = FALSE
            AND e.clustered = FALSE
        ORDER BY e.confidence
        LIMIT 1
        '''.format(ent_name)
    entity_id = None
    engine = app_session.bind
    with engine.begin() as conn:
        entity_id = list(conn.execute(sel))
    if entity_id:
        entity_id = entity_id[0][0]
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
        records = []
        with engine.begin() as conn:
            records = list(conn.execute(sel, entity_id=entity_id))
        raw_fields = ['confidence'] + model_fields + ['record_id']
        for thing in records:
            d = {}
            for k,v in zip(raw_fields, thing):
                d[k] = v

            # d['confidence'] = formatPercentage(d['confidence'])
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
    return entity_id, cluster_list

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

def makeSampleDict(session_id, fields):
    session = worker_session
    engine = session.bind
    metadata = MetaData()
    proc_table = Table('processed_%s' % session_id, metadata, 
        autoload=True, autoload_with=engine)
    entity_table = Table('entity_%s' % session_id, metadata, 
        autoload=True, autoload_with=engine)
    result = {}
    cols = [getattr(proc_table.c, f) for f in fields]
    '''
    Get one record from each cluster of exact duplicates that are 
    already in entity map + all records that don't have entries in 
    the entity_map
    
    SELECT p.<fields from model>
      FROM processed as p
      LEFT JOIN entity as e
      WHERE e.target_record_id IS NULL
    '''
    curs = session.query(*cols)\
        .outerjoin(entity_table, 
            proc_table.c.record_id == entity_table.c.record_id)\
        .filter(entity_table.c.target_record_id == None)
    result = dict((i, frozendict(zip(fields, row))) 
                            for i, row in enumerate(curs))
    return result

def getDistinct(field_name, session_id):
    engine = app_session.bind
    metadata = MetaData()
    table = Table('processed_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    col = getattr(table.c, field_name)
    q = app_session.query(distinct(col)).filter(and_(col != None, col != ''))
    distinct_values = list(set([unicode(v[0]) for v in q.all()]))
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
        except NoSuchTableError: # pragma: no cover 
            pass
    return None

def formatPercentage(num):
    return "{0:.0f}%".format(float(num) * 100)
