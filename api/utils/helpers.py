from __future__ import unicode_literals
from flask import current_app
import re
import os
import json
import sys
import numpy
from dedupe.core import frozendict
from dedupe import canonicalize
from dedupe.api import StaticGazetteer, Gazetteer
from dedupe.serializer import _to_json
import dedupe
from api.database import app_session, worker_session, Base, init_engine
from api.models import DedupeSession
from sqlalchemy import Table, MetaData, distinct, and_, func, \
    Column, text, String, select, Integer
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from sqlalchemy.dialects.postgresql.base import ARRAY
from unicodedata import normalize
from itertools import count
from csvkit.unicsv import UnicodeCSVDictWriter
from csv import QUOTE_ALL
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from collections import OrderedDict, defaultdict
from operator import itemgetter


import pickle

STATUS_LIST = [
    {
        'step': 1,
        'machine_name' : 'dataset uploaded',
        'human_name': 'Dataset uploaded', 
        'next_step_name': 'Fields to compare',
        'next_step_url': '/select-fields/?session_id={0}',
        'next_step': 2
    },
    {
        'step': 2,
        'machine_name': 'model defined',
        'human_name': 'Model defined', 
        'next_step_name': 'Train',
        'next_step_url': '/training-run/?session_id={0}',
        'next_step': 3
    },
    {
        'step': 3,
        'machine_name': 'entity map updated',
        'human_name': 'Training finished', 
        'next_step_name': 'Review entities',
        'next_step_url': '/session-review/?session_id={0}',
        'next_step': 3
    },
    {
        'step': 4,
        'machine_name': 'canon clustered',
        'human_name': 'Entities reviewed', 
        'next_step_name': 'Merge entities',
        'next_step_url': '/session-review/?session_id={0}&second_review=True',
        'next_step': 5
    },
    {
        'step': 5,
        'machine_name': 'matching ready',
        'human_name': 'Entities merged', 
        'next_step_name': 'Final review',
        'next_step_url': '/match-review/?session_id={0}',
        'next_step': 6
    },
    {
        'step': 6,
        'machine_name':'canonical',
        'human_name': 'Dataset is canonical', 
        'next_step_name': 'Ready for matching!',
        'next_step_url': '/session-admin/?session_id={0}',
        'next_step': None
    },
]

def sklearner(labels, examples, alpha) :
    from sklearn.linear_model import LogisticRegression
    learner = LogisticRegression(penalty='l2', C=1/alpha)

    learner.fit(examples, labels)

    weight, bias = list(learner.coef_[0]), learner.intercept_[0]

    return weight, bias

class RetrainGazetteer(StaticGazetteer, Gazetteer):
    
    def __init__(self, *args, **kwargs):
        super(RetrainGazetteer, self).__init__(*args, **kwargs)

        training_dtype = [('label', 'S8'), 
                         ('distances', 'f4', 
                          (len(self.data_model['fields']), ))]

        self.training_data = numpy.zeros(0, dtype=training_dtype)
        self.training_pairs = OrderedDict({u'distinct': [], u'match': []}) 
        
        self.learner = sklearner

class DatabaseGazetteer(StaticGazetteer):
    def __init__(self, *args, **kwargs):

        self.engine = kwargs['engine']
        self.session_id = kwargs['session_id']
        
        del kwargs['engine']
        del kwargs['session_id']

        super(DatabaseGazetteer, self).__init__(*args, **kwargs)

    def _blockData(self, messy_data):

        if messy_data :
            block_groups = ''' 
                SELECT
                  record_id,
                  array_agg(block_key) AS block_keys
                FROM "match_blocks_{0}" 
                WHERE record_id in :record_ids
                GROUP BY record_id
            '''.format(self.session_id)

            block_groups = self.engine.execute(text(block_groups), 
                               record_ids=tuple(messy_data.keys()))
        else :
            block_groups = []

        # Distinct by record id where records exist in entity_map
        sel = ''' 
            SELECT
              DISTINCT ON (p.record_id)
              p.*
            FROM "processed_{0}" AS p
            JOIN "match_blocks_{0}" AS m
              ON p.record_id = m.record_id
            JOIN "entity_{0}" AS e
              ON m.record_id = e.record_id
            WHERE m.block_key IN :block_keys
            ORDER BY p.record_id
        '''.format(self.session_id)
        
        B = []

        local_engine = self.engine.execute

        for group in block_groups:
            A = [(group.record_id, messy_data[group.record_id], set())]
            rows = local_engine(text(sel), 
                    block_keys=tuple(group.block_keys))
            B = [(row.record_id, row, set()) for row in rows]

            if B:
                yield (A,B)

def tupleizeTraining(training):
    distinct = []
    match = []
    td = {'distinct': [], 'match': []}
    for match_type, pairs in training.items():
        for pair in pairs:
            p = []
            for item in pair:
                for k,v in item.items():
                    if isinstance(v, list):
                        item[k] = tuple(v)
                p.append(item)
            td[match_type].append(p)
    return td

def getCluster(session_id, entity_pattern, raw_pattern):
    ent_name = entity_pattern.format(session_id)
    raw_name = raw_pattern.format(session_id)
    sess = app_session.query(DedupeSession).get(session_id)
    app_session.refresh(sess)
    
    cluster_list = []
    prediction = None
    machine = pickle.loads(sess.review_machine)
    if machine.get_next() is not None:
        entity_id = bytes(machine.get_next()).decode('utf-8')
        sess.review_machine = pickle.dumps(machine)
        app_session.add(sess)
        app_session.commit()
        engine = app_session.bind
        model_fields = list(set([f['field'] for f in \
                json.loads(sess.field_defs.decode('utf-8'))]))
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
            false_pos, false_neg = machine.predict_remainder(threshold=0.0)
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

def getMatches(session_id, records):
    engine = worker_session.bind
    dedupe_session = worker_session.query(DedupeSession).get(session_id)
    settings_file = BytesIO(dedupe_session.gaz_settings_file)

    deduper = DatabaseGazetteer(settings_file, 
                                num_cores=1, 
                                engine=engine, 
                                session_id=session_id)

    messy_records = {int(r['record_id']): r for r in records}

    linked_records = deduper.match(messy_records, n_matches=5)

    match_ids = set()
    match_mapping = defaultdict(list)

    for possible_links in linked_records:
        for link in possible_links :
            (messy_id, match_id), confidence = link
            match_ids.add(int(match_id))
            match_mapping[int(messy_id)].append((int(match_id), confidence,))

    if match_ids :
        entities = ''' 
            SELECT 
              e.entity_id,
              e.record_id
            FROM "entity_{0}" AS e
            JOIN "raw_{0}" AS r
              ON e.record_id = r.record_id
            WHERE e.record_id IN :record_ids
        '''.format(session_id)
        match_records = engine.execute(text(entities), 
                                       record_ids=tuple(match_ids))
    else :
        match_records = []


    match_records = {r.record_id: r for r in match_records}
    matches = []

    for messy_id in messy_records : 
        possible_matches = match_mapping.get(messy_id, [])
        best_records = defaultdict(dict)
        for match_id, confidence in possible_matches :
            record = match_records[match_id]
            entity = record['entity_id']
            if confidence > best_records[entity].get('confidence', 0.0) :
                best_records[entity] = dict(record)
                best_records[entity]['confidence'] = float(confidence)

        matches.append((messy_records[messy_id], 
                        list(best_records.values())))
            
    del deduper

    return matches


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

def slugify(text, delim='_'):
    if text:
        punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.:;]+')
        result = []
        for word in punct_re.split(text.lower()):
            if word:
                result.append(str(word))
        return delim.join(result)
    else: # pragma: no cover
        return text

def preProcess(column, field_types):
    if 'Price' in field_types:
        if column:
            column = float(column)
        else:
            column = 0
    elif 'Set' in field_types:
        if column :
            column = tuple(column.split(','))
        else :
            column = ()
    else:
        if column :
            column = str(column)
            column = re.sub('  +', ' ', column)
            column = re.sub('\n', ' ', column)
            column = column.strip().strip('"').strip("'").lower().strip()
            if not column :
                column = ''
        else :
            column = ''

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
            smaller_ids = lset(smaller_ids)
        else:
            smaller_ids = lset([])
        records.append((row['record_id'], row, smaller_ids))
    if records:
        yield records

def getTupleColumns(table):
    new_cols = []
    for col in table.columns:
        if isinstance(col.type, ARRAY):
            new_cols.append(Column(col.name, ARRAY(String, as_tuple=True)))
        else:
            new_cols.append(Column(col.name, col.type))
    return new_cols

def selectWithTuples(session_id, 
                     proc_fmt='processed_{0}', 
                     join_fmt='entity_{0}',
                     isouter=True):
    engine = worker_session.bind
    m = MetaData()
    proc = Table(proc_fmt.format(session_id), m, 
                  autoload=True, autoload_with=engine)
    new_cols = getTupleColumns(proc)
    meta = MetaData()
    new_proc = Table(proc_fmt.format(session_id), meta, *new_cols)
    entity = Table(join_fmt.format(session_id), meta, 
                  autoload=True, autoload_with=engine)
    sel = select([new_proc])\
            .select_from(new_proc.join(entity, 
                             new_proc.c.record_id == entity.c.record_id,
                             isouter=isouter))\
            .where(entity.c.record_id == None)
    return sel

def makeSampleDict(session_id):
    engine = worker_session.bind
    sel = selectWithTuples(session_id)
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
            AND {0}::varchar != ''
    '''.format(field_name, session_id)
    distinct_values = list(set([u'{0}'.format(v[0]) for v in engine.execute(sel)]))
    return distinct_values

def hasMissing(field_name, session_id):
    engine = app_session.bind
    sel = ''' 
        SELECT (
          SELECT COUNT(*)
          FROM "raw_{0}"
        ) - (
          SELECT COUNT(*)
          FROM "raw_{0}"
          WHERE {1} IS NOT NULL
        ) AS blank_count
    '''.format(session_id, field_name)
    blank_count = engine.execute(sel).first().blank_count
    if blank_count == 0:
        return False
    return True

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

def updateEntityCount(session_id):
    engine = worker_session.bind
    upd = ''' 
        UPDATE dedupe_session SET
        entity_count = subq.entity_count FROM (
          SELECT SUM(s.row_number) AS entity_count
          FROM (
            SELECT ROW_NUMBER() OVER(PARTITION BY e.entity_id) AS row_number 
            FROM "raw_{0}" AS r 
            JOIN "entity_{0}" AS e 
              ON r.record_id = e.record_id 
            GROUP BY e.entity_id
          ) AS s
        ) AS subq
        WHERE id = :id
    '''.format(session_id)
    with engine.begin() as conn:
        conn.execute(text(upd), id=session_id)

    # Create or refresh materialized view used by entity browser
    conn = engine.connect()
    trans = conn.begin()
    try:
        conn.execute('REFRESH MATERIALIZED VIEW "browser_{0}"'.format(session_id))
        trans.commit()
    except ProgrammingError:
        trans.rollback()
        conn = engine.connect()
        trans = conn.begin()
        dedupe_session = worker_session.query(DedupeSession).get(session_id)
        field_names = set([f['field'] for f in \
                json.loads(dedupe_session.field_defs.decode('utf-8'))])
        fields = ', '.join(['MAX(r.{0}) AS {0}'.format(f) for f in field_names])
        create = ''' 
            CREATE MATERIALIZED VIEW "browser_{1}" AS (
              SELECT {0},
                COUNT(*) AS record_count,
                e.entity_id
              FROM "raw_{1}" AS r
              JOIN "entity_{1}" AS e
                ON r.record_id = e.record_id
              GROUP BY e.entity_id
              ORDER BY record_count DESC
            )
            '''.format(fields, session_id)
        conn.execute(create)
        trans.commit()

