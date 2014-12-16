import re
import os
import json
from dedupe.core import frozendict
from dedupe import canonicalize
from api.database import app_session, worker_session, Base, engine
from api.models import DedupeSession
from sqlalchemy import Table, MetaData, distinct, and_, func, Column, text
from unidecode import unidecode
from unicodedata import normalize
from itertools import count
from csvkit.unicsv import UnicodeCSVDictWriter
from csv import QUOTE_ALL
from datetime import datetime, timedelta
from unidecode import unidecode

STATUS_LIST = [
    'dataset uploaded',       # File stored, waiting for raw tables to be written
    'session initialized',    # Raw and processed tables written
    'model defined',          # User gave us a model
    'entity map initialized', # Looked for exact duplicates and made entity map from that
    'training started',       # User started training
    'clustering started',     # User finished training and clustering process is running
    'entity map updated',     # Entity map updated with results of clustering
    'canon clustered',        # First cluster review complete and results of canonical dedupe are ready
    'matching ready',         # Canonical clusters are reviewed and Gazetteer settings are saved
]

def updateTraining(session_id, record_ids, distinct=False):
    ''' 
    Update the sessions training data with the given record_ids
    '''
    return None

def updateSessionStatus(session_id, increment=True):
    ''' 
    Advance or reverse the status of a session by one step
    '''
    dd = worker_session.query(DedupeSession).get(session_id)
    try:
        current = STATUS_LIST.index(dd.status)
    except ValueError:
        current = 0
    print 'CURRENT STATUS {0}'.format(STATUS_LIST[current])
    if increment:
        dd.status = STATUS_LIST[current + 1]
        print 'NEW STATUS {0}'.format(STATUS_LIST[current + 1])
    else:
        dd.status = STATUS_LIST[current - 1]
        print 'NEW STATUS {0}'.format(STATUS_LIST[current + 1])
    worker_session.add(dd)
    worker_session.commit()

def getCluster(session_id, entity_pattern, raw_pattern):
    ent_name = entity_pattern.format(session_id)
    raw_name = raw_pattern.format(session_id)
    sess = app_session.query(DedupeSession).get(session_id)

    cluster_list = []
    model_fields = [f['field'] for f in json.loads(sess.field_defs)]
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
            '''.format(raw_cols, raw_name, ent_name))
        records = []
        with engine.begin() as conn:
            records = list(conn.execute(sel, entity_id=entity_id))
        raw_fields = ['confidence'] + model_fields + ['record_id']
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
    return entity_id, cluster_list

def column_windows(session, column, windowsize):
    def int_for_range(start_id, end_id):
        if end_id:
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
        if intervals:
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
    else:
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

def makeDataDict(session_id, fields=None, name_pattern='processed_{0}'):
    session = worker_session
    engine = session.bind
    metadata = MetaData()
    table_name = name_pattern.format(session_id)
    table = Table(table_name, metadata, 
        autoload=True, autoload_with=engine)
    if not fields:
        fields = [unicode(s) for s in table.columns.keys()]
    primary_key = [p.name for p in table.primary_key][0]
    result = {}

    cols = [getattr(table.c, f) for f in fields]
    cols.append(getattr(table.c, primary_key))
    curs = session.query(*cols)
    for row in curs:
        try:
            result[int(getattr(row, primary_key))] = frozendict(zip(fields, row))
        except ValueError:
            result[getattr(row, primary_key)] = frozendict(zip(fields, row))
    return result

def getDistinct(field_name, session_id):
    engine = app_session.bind
    metadata = MetaData()
    table = Table('processed_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    col = getattr(table.c, field_name)
    q = app_session.query(distinct(col)).filter(col != None)
    distinct_values = list(set([unicode(v[0]) for v in q.all()]))
    return distinct_values

def getMatchingDataDict(session_id):
    dd_session = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(dd_session.field_defs)
    model_fields = [d['field'] for d in field_defs]
    fields = ', '.join(['p.{0}'.format(f) for f in model_fields])
    sel = ''' 
        SELECT e.record_id, {0}
        FROM "entity_{1}" AS e
        JOIN "processed_{1}" as p
          ON e.record_id = p.record_id
        WHERE e.record_id NOT IN (
          SELECT UNNEST(member_ids)
          FROM "exact_match_{1}"
        )
        '''.format(fields, session_id)
    rows = []
    engine = worker_session.bind
    with engine.begin() as conn:
        rows = list(conn.execute(sel))
    dd = {}
    for row in rows:
        dd[row.record_id] = {f:getattr(row, f) for f in model_fields}
    return dd
    
