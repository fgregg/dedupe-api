import re
import os
import json
from dedupe.core import frozendict
from dedupe import canonicalize
from api.database import app_session, worker_session, Base, engine
from api.models import DedupeSession
from sqlalchemy import Table, MetaData, distinct, and_, func, Column
from unidecode import unidecode
from unicodedata import normalize
from itertools import count
from csvkit.unicsv import UnicodeCSVDictWriter
from csv import QUOTE_ALL
from datetime import datetime, timedelta

def getCluster(session_id, entity_table_name, raw_table_name):
    sess = app_session.query(DedupeSession).get(session_id)
    entity_table = Table(entity_table_name, Base.metadata,
        autoload=True, autoload_with=engine)
    total_clusters = app_session.query(entity_table.c.entity_id.distinct()).count()
    review_remainder = app_session.query(entity_table.c.entity_id.distinct())\
        .filter(entity_table.c.clustered == False)\
        .count()
    cluster_list = []
    field_defs = [f['field'] for f in json.loads(sess.field_defs)]
    raw_table = Table(raw_table_name, Base.metadata, 
        autoload=True, autoload_with=engine, keep_existing=True)
    entity_fields = ['record_id', 'entity_id', 'confidence']
    entity_cols = [getattr(entity_table.c, f) for f in entity_fields]
    subq = app_session.query(entity_table.c.entity_id)\
        .filter(entity_table.c.checked_out == False)\
        .filter(entity_table.c.clustered == False)\
        .order_by(entity_table.c.confidence).limit(1).subquery()
    cluster = app_session.query(*entity_cols)\
        .filter(entity_table.c.entity_id.in_(subq)).all()
    if cluster:
        raw_ids = [c[0] for c in cluster]
        raw_cols = [getattr(raw_table.c, f) for f in field_defs]
        raw_cols.append(raw_table.c.record_id)
        primary_key = [p.name for p in raw_table.primary_key][0]
        pk_col = getattr(raw_table.c, primary_key)
        records = app_session.query(*raw_cols).filter(pk_col.in_(raw_ids))
        raw_fields = [f['name'] for f in records.column_descriptions]
        records = records.all()
        one_minute = datetime.now() + timedelta(minutes=1)
        upd = entity_table.update()\
            .where(entity_table.c.entity_id.in_(subq))\
            .values(checked_out=True, checkout_expire=one_minute)
        with engine.begin() as c:
            c.execute(upd)
        confidence = cluster[0][2]
        entity_id = cluster[0][1]
        for thing in records:
            d = {}
            for k,v in zip(raw_fields, thing):
                d[k] = v
            cluster_list.append(d)
        return confidence, entity_id, cluster_list

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
    if column == 'None':
        column = u''
    # column = unidecode(column)
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
    curs = session.query(*cols)\
        .outerjoin(entity_table, 
            proc_table.c.record_id == entity_table.c.record_id)\
        .filter(entity_table.c.target_record_id == None)
    result = dict((i, frozendict(zip(fields, row))) 
                            for i, row in enumerate(curs))
    return result

def makeDataDict(session_id, fields=None):
    session = worker_session
    engine = session.bind
    metadata = MetaData()
    table_name = 'processed_%s' % session_id
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
        result[int(getattr(row, primary_key))] = frozendict(zip(fields, row))
    return result

def getDistinct(field_name, session_id):
    engine = app_session.bind
    metadata = MetaData()
    table = Table('raw_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    q = app_session.query(distinct(getattr(table.c, field_name)))
    distinct_values = [preProcess(unicode(v[0])) for v in q.all()]
    return distinct_values

