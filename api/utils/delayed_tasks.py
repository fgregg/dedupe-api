import dedupe
from dedupe.serializer import _from_json, _to_json
import os
import simplejson as json
import time
from io import StringIO, BytesIO
from api.queue import queuefunc
from api.app_config import DB_CONN, DOWNLOAD_FOLDER
from api.models import DedupeSession, User, entity_map
from api.database import worker_session
from api.utils.helpers import clusterGen, makeSampleDict, getDistinct, \
    getMatches, updateEntityCount, RetrainGazetteer, hasMissing, \
    readFieldDefs, readTraining
from api.utils.db_functions import updateEntityMap, writeBlockingMap, \
    writeRawTable, initializeEntityMap, writeProcessedTable, writeCanonRep, \
    addRowHash, addToEntityMap, readTraining
from api.utils.review_machine import ReviewMachine
from sqlalchemy import Table, MetaData, Column, String, func, text, \
    Integer, select
from sqlalchemy.sql import label
from sqlalchemy.exc import NoSuchTableError, ProgrammingError, IntegrityError
from collections import defaultdict
from itertools import groupby, islice
from operator import itemgetter
from collections import OrderedDict
from csvkit import convert
from os.path import join, dirname, abspath
from datetime import datetime
import pickle
import csv
from uuid import uuid4
from api.app_config import TIME_ZONE


### Bulk acceptance tasks ###

@queuefunc
def bulkMarkClusters(session_id, user=None):
    engine = worker_session.bind
    now =  datetime.now().replace(tzinfo=TIME_ZONE)
    
    upd_vals = {
        'user_name': user, 
        'last_update': now,
        'match_type': 'bulk accepted'
    }
    
    update_records = updateClusterRecords(session_id)
    with engine.begin() as c:
        c.execute(update_records, **upd_vals)
    
    getMatchingReady(session_id)

@queuefunc
def bulkMarkCanonClusters(session_id, user=None):
    engine = worker_session.bind
    last_update = datetime.now().replace(tzinfo=TIME_ZONE)
    
    root_args = {
        'reviewed': True,
        'checked_out': False,
        'last_update': last_update,
        'reviewer': user,
        'match_type': 'bulk entity merge'
    }
    
    update_roots = '''
        UPDATE "entity_{0}" AS ent_map SET 
            entity_id = cr_ent_map.entity_id,
            reviewed = :reviewed,
            checked_out = :checked_out,
            last_update = :last_update,
            reviewer = :reviewer,
            match_type = :match_type
        FROM (
            SELECT entity_id, record_id
            FROM "entity_{0}_cr"
            WHERE reviewed = FALSE
        ) AS cr_ent_map
        WHERE ent_map.entity_id = cr_ent_map.record_id
          AND target_record_id IS NULL
        '''.format(session_id)
    
    branch_args = {
        'last_update': last_update,
    }
    update_branches = ''' 
        UPDATE "entity_{0}" AS ent_map SET 
            entity_id = cr_ent_map.entity_id,
            last_update = :last_update
        FROM (
            SELECT entity_id, record_id
            FROM "entity_{0}_cr"
            WHERE reviewed = FALSE
        ) AS cr_ent_map
        WHERE ent_map.entity_id = cr_ent_map.record_id
          AND target_record_id IS NOT NULL
    '''.format(session_id)

    with engine.begin() as c:
        c.execute(text(update_roots), **root_args)
        c.execute(text(update_branches), **branch_args)

    updateEntityCount(session_id)
    with engine.begin() as conn:
        conn.execute(text('''
            UPDATE dedupe_session SET 
              status = :status 
            WHERE id = :session_id'''), 
            status='canonical', 
            session_id=session_id)

def updateFromCanon(session_id):
    return text(''' 
        UPDATE "entity_{0}" SET 
            entity_id=subq.entity_id,
            reviewed= :reviewed,
            reviewer = :user_name,
            match_type = :match_type,
            last_update = :last_update
        FROM (
            SELECT 
                c.record_id as canon_record_id,
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
        RETURNING "entity_{0}".entity_id, subq.canon_record_id
        '''.format(session_id))

def updateClusterRecords(session_id):
    return text(''' 
        UPDATE "entity_{0}" SET 
          entity_id = subq.entity_id,
          reviewed = TRUE,
          reviewer = :user_name,
          last_update = :last_update,
          match_type = :match_type
        FROM (
            SELECT 
              entity_id,
              record_id,
              match_type
            FROM "entity_{0}"
            WHERE reviewed=FALSE 
          ) as subq 
        WHERE "entity_{0}".record_id=subq.record_id 
        '''.format(session_id))

def updateExactMatches(session_id):
    return text(''' 
        UPDATE "entity_{0}" SET
          entity_id = subq.parent_entity_id,
          last_update = :last_update
        FROM (
          SELECT
            parent.entity_id AS parent_entity_id,
            child.record_id AS child_record_id
          FROM "entity_{0}" AS parent
          JOIN (
            SELECT
              record_id,
              target_record_id,
              entity_id
            FROM "entity_{0}"
            WHERE match_type = :exact_match_type
          ) AS child
            ON parent.record_id = child.target_record_id
        ) as subq 
        WHERE "entity_{0}".record_id = subq.child_record_id
        '''.format(session_id))


### Prepare session to match records ###

@queuefunc
def getMatchingReady(session_id):
    
    # Update orphaned exact matches
    now =  datetime.now().replace(tzinfo=TIME_ZONE)
    upd_vals = {
        'last_update': now,
        'exact_match_type': 'exact',
    }
    
    update_exact = updateExactMatches(session_id)
    engine = worker_session.bind
    
    with engine.begin() as c:
        c.execute(update_exact, **upd_vals)

    updateEntityCount(session_id)

    addRowHash(session_id)
    cleanupTables(session_id)
    with engine.begin() as conn:
        conn.execute('DROP TABLE IF EXISTS "match_blocks_{0}"'\
            .format(session_id))
        conn.execute(''' 
            CREATE TABLE "match_blocks_{0}" (
                block_key VARCHAR, 
                record_id BIGINT
            )
            '''.format(session_id))
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = readFieldDefs(session_id)

    # Save Gazetteer settings
    d = dedupe.Gazetteer(field_defs)
    
    training_data = readTraining(session_id)

    d.readTraining(StringIO(json.dumps(training_data, 
                                       default=_to_json,
                                       tuple_as_array=False)))
    
    d.train(ppc=0.1, index_predicates=False)
    g_settings = BytesIO()
    d.writeSettings(g_settings)
    g_settings.seek(0)
    sess.gaz_settings_file = g_settings.getvalue()
    worker_session.add(sess)
    worker_session.commit()

    # Write match_block table
    model_fields = list(set([f['field'] for f in field_defs]))
    fields = ', '.join(['p.{0}'.format(f) for f in model_fields])
    sel = ''' 
        SELECT 
          p.record_id, 
          {0}
        FROM "processed_{1}" AS p 
        LEFT JOIN "exact_match_{1}" AS e 
          ON p.record_id = e.match 
        WHERE e.record_id IS NULL;
        '''.format(fields, session_id)
    conn = engine.connect()
    rows = conn.execute(sel)
    data = ((getattr(row, 'record_id'), 
                 dict(zip(row.keys()[1:], row.values()[1:]))) \
             for row in rows)
    block_gen = d.blocker(data)
    s = StringIO()
    writer = csv.writer(s)
    writer.writerows(block_gen)
    conn.close()
    s.seek(0)
    conn = engine.raw_connection()
    curs = conn.cursor()
    try:
        curs.copy_expert('COPY "match_blocks_{0}" FROM STDIN CSV'\
            .format(session_id), s)
        conn.commit()
    except (ProgrammingError, IntegrityError) as e: # pragma: no cover
        conn.rollback()
        raise e
    conn.close()
    with engine.begin() as conn:
        conn.execute('DROP INDEX IF EXISTS "match_blocks_key_{0}_idx"'.format(session_id))
    
    with engine.begin() as conn:
        conn.execute('''
            CREATE INDEX "match_blocks_key_{0}_idx" 
              ON "match_blocks_{0}" (block_key)
            '''.format(session_id)
        )

    delete = ''' 
        DELETE FROM "entity_{0}" 
        WHERE record_id IN (
            SELECT record_id
            FROM "match_review_{0}"
        )
    '''.format(session_id)
    conn = engine.connect()
    trans = conn.begin()
    try:
        conn.execute(delete)
        trans.commit()
    except (ProgrammingError, IntegrityError) as e:
        trans.rollback()
    
    create_human_review = '''
        CREATE TABLE "match_review_{0}" AS
          SELECT
            r.record_id,
            ARRAY[]::varchar[] AS entities,
            FALSE AS reviewed, 
            ARRAY[]::double precision[] AS confidence,
            NULL::varchar AS reviewer,
            FALSE AS sent_for_review 
          FROM "raw_{0}" AS r
          LEFT JOIN "entity_{0}" AS e
            ON r.record_id = e.record_id
          WHERE e.record_id IS NULL
    '''.format(session_id)
    with engine.begin() as conn:
        conn.execute('DROP TABLE IF EXISTS "match_review_{0}"'.format(session_id))
        conn.execute(create_human_review)
        conn.execute('CREATE INDEX "match_rev_idx_{0}" ON "match_review_{0}" (record_id)'.format(session_id))

    populateHumanReview(session_id)
    
    # Get review count
    sel = ''' 
      SELECT COUNT(*)
      FROM "raw_{0}" AS p
      LEFT JOIN "entity_{0}" AS e
        ON p.record_id = e.record_id
      WHERE e.record_id IS NULL
    '''.format(session_id)
    count = engine.execute(sel).first()
    sess.status = 'matching ready'
    sess.review_count = count[0]
    
    sess.processing = False
    worker_session.add(sess)
    worker_session.commit()
    del d
    return None

@queuefunc
def populateHumanReview(session_id, floor_it=False):
    
    engine = worker_session.bind
    
    field_defs = readFieldDefs(session_id)
    raw_fields = sorted(list(set([f['field'] for f in field_defs])))
    raw_fields.append('record_id')
    fields = ', '.join(['r.{0}'.format(f) for f in raw_fields])
    
    sel = ''' 
      SELECT {0}
      FROM "processed_{1}" as r
      LEFT JOIN "entity_{1}" as e
        ON r.record_id = e.record_id
      WHERE e.record_id IS NULL
      ORDER BY RANDOM()
    '''.format(fields, session_id)
    
    rows = (OrderedDict(zip(raw_fields, r)) for r in engine.execute(text(sel)))
    
    human_queue = []
    cleared = []
    chunk_size = 100
    query_exhausted = False


    while len(human_queue) < 20 and not query_exhausted :
        
        records = list(islice(rows, chunk_size))
        if len(records) < chunk_size :
            query_exhausted = True
        
        record_matches = getMatches(session_id, records)

        for record, matches in record_matches:
            matches = [match for match in matches if match['confidence'] > 0.2]
             
            if floor_it:
                addToEntityMap(session_id, 
                               record, 
                               match_ids=[m['record_id'] for m in matches],
                               reviewer='machine')
                markAsReviewed(session_id, record['record_id'], 'machine') 

            elif len(matches) == 1 and matches[0]['confidence'] >= 0.8:
                # Means Auto adding match 
                addToEntityMap(session_id, 
                               record, 
                               match_ids=[m['record_id'] for m in matches],
                               reviewer='machine')
                markAsReviewed(session_id, record['record_id'], 'machine') 
            elif len(matches) :
                r = {
                    'record_id': record['record_id'], 
                    'entities': [m['entity_id'] for m in matches],
                    'confidence': [m['confidence'] for m in matches]
                }
                upd = ''' 
                    UPDATE "match_review_{0}" SET
                      entities = :entities,
                      confidence = :confidence,
                      sent_for_review = TRUE
                    WHERE record_id = :record_id
                    '''.format(session_id)
                with engine.begin() as conn:
                    conn.execute(text(upd), **r)
                human_queue.append(record)

            elif len(matches) == 0 :
                addToEntityMap(session_id, 
                               record, 
                               reviewer='machine')
                markAsReviewed(session_id, 
                               record['record_id'], 
                               'machine')
            
            else :
                raise ValueError("should not get here") 

    del rows
    
    updateEntityCount(session_id)
    
    settings = ''' 
        SELECT gaz_settings_file AS sf
        FROM dedupe_session
        WHERE id = :session_id
    '''
    settings = engine.execute(text(settings), session_id=session_id).first().sf
    
    # Train classifier
    deduper = RetrainGazetteer(BytesIO(settings), num_cores=1)
    
    training_data = readTraining(session_id)

    deduper.readTraining(StringIO(json.dumps(training_data, 
                                             default=_to_json,
                                             tuple_as_array=False)))
    deduper._trainClassifier()
    fobj = BytesIO()
    deduper.writeSettings(fobj)
    
    update_settings = ''' 
        UPDATE dedupe_session SET
          gaz_settings_file = :settings_file
        WHERE id = :session_id
    '''

    with engine.begin() as conn:
        conn.execute(text(update_settings), 
                     settings_file=fobj.getvalue(),
                     session_id=session_id)

    if floor_it:
        dedupeCanon(session_id)

def markAsReviewed(session_id, record_id, reviewer) :
    engine = worker_session.bind

    reviewed = ''' 
        UPDATE "match_review_{0}" SET 
        reviewed = TRUE,
        reviewer = :reviewer,
        sent_for_review = TRUE
        WHERE record_id = :record_id
    '''.format(session_id)
    with engine.begin() as conn:
        conn.execute(text(reviewed), 
                     record_id=record_id, 
                     reviewer=reviewer)

@queuefunc
def cleanupTables(session_id, tables=None):
    engine = worker_session.bind
    if not tables:
        tables = [
            'processed_{0}_cr',
            'block_{0}_cr',
            'plural_block_{0}_cr',
            'covered_{0}_cr',
            'plural_key_{0}_cr',
            'small_cov_{0}_cr',
            'cr_{0}',
            'block_{0}',
            'plural_block_{0}',
            'covered_{0}',
            'plural_key_{0}',
        ]
    conn = engine.connect()
    for table in tables:
        tname = table.format(session_id)
        trans = conn.begin()
        conn.execute('DROP TABLE IF EXISTS "{0}" CASCADE'.format(tname))
        trans.commit()
    conn.close()

def drawSample(session_id):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = readFieldDefs(session_id)
    d = dedupe.Dedupe(field_defs)
    data_d = makeSampleDict(sess.id)
    if len(data_d) < 50001:
        sample_size = 5000
    else: # pragma: no cover
        sample_size = round(int(len(data_d) * 0.01), -3)
    d.sample(data_d, sample_size=sample_size, blocked_proportion=1)
    sess.sample = pickle.dumps(d.data_sample)
    worker_session.add(sess)
    worker_session.commit()
    del d

@queuefunc
def initializeSession(session_id, fieldnames):
    sess = worker_session.query(DedupeSession).get(session_id)
    file_path = '/tmp/{0}_raw.csv'.format(session_id)
    kwargs = {
        'session_id':session_id,
        'file_path':file_path,
        'fieldnames': fieldnames,
    }
    writeRawTable(**kwargs)
    engine = worker_session.bind
    metadata = MetaData()
    raw_table = Table('raw_{0}'.format(session_id), metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    sess.record_count = worker_session.query(raw_table).count()
    worker_session.add(sess)
    worker_session.commit()
    print('session initialized')

@queuefunc
def initializeModel(session_id, init=True):
    worker_session.expire_all()
    sess = worker_session.query(DedupeSession).get(session_id)
    while True:
        worker_session.refresh(sess, ['field_defs', 'sample', 'record_count'])
        if not sess.field_defs: # pragma: no cover
            time.sleep(3)
        else:
            if init:
                writeProcessedTable(session_id)
            sess.status = 'model defined'
            worker_session.add(sess)
            worker_session.commit()
            if init:
                field_defs = json.loads(sess.field_defs.decode('utf-8'))
                fields = list(set([f['field'] for f in field_defs]))
                initializeEntityMap(session_id, fields)
                drawSample(session_id)
            print('got sample')
            break

@queuefunc
def trainDedupe(session_id):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    data_sample = pickle.loads(dd_session.sample)
    
    field_defs = readFieldDefs(session_id)
    training_data = readTraining(session_id)
    
    deduper = dedupe.Dedupe(field_defs, data_sample=data_sample)
    
    training_data = readTraining(session_id)
    deduper.readTraining(StringIO(json.dumps(training_data, 
                                             default=_to_json,
                                             tuple_as_array=False)))
    
    deduper.train()
    
    settings_file_obj = BytesIO()
    deduper.writeSettings(settings_file_obj)
    dd_session.settings_file = settings_file_obj.getvalue()
    worker_session.add(dd_session)
    worker_session.commit()
    deduper.cleanupTraining()
    del deduper

def blockDedupe(session_id, 
                table_name=None, 
                entity_table_name=None, 
                canonical=False):

    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    deduper = dedupe.StaticDedupe(BytesIO(dd_session.settings_file))
    engine = worker_session.bind
    
    if table_name is None:
        table_name = 'processed_{0}'.format(session_id)
    if entity_table_name is None:
        entity_table_name = 'entity_{0}'.format(session_id)
    
    for field in deduper.blocker.index_fields:
        fd = (str(f[0]) for f in \
                engine.execute('SELECT DISTINCT {0} FROM "{1}"'.format(field, table_name)))
        deduper.blocker.index(fd, field)

    select_block_records = ''' 
        SELECT p.*
        FROM "{0}" AS p
        LEFT OUTER JOIN "{1}" AS e
          ON p.record_id = e.record_id
        WHERE e.target_record_id IS NULL
    '''.format(table_name, entity_table_name)

    full_data = ((getattr(row, 'record_id'), dict(zip(row.keys(), row.values()))) \
        for row in engine.execute(select_block_records))

    return deduper.blocker(full_data)

def clusterDedupe(session_id, canonical=False, threshold=0.75):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    worker_session.refresh(dd_session)
    deduper = dedupe.StaticDedupe(BytesIO(dd_session.settings_file))
    engine = worker_session.bind
    sc_format = 'small_cov_{0}'
    proc_format = 'processed_{0}'
    if canonical:
        sc_format = 'small_cov_{0}_cr'
        proc_format = 'cr_{0}'
    trained_fields = list(set([f['field'] for f in \
        json.loads(dd_session.field_defs.decode('utf-8'))]))
    clustered_dupes = []
    sc_table = sc_format.format(session_id)
    proc_table = proc_format.format(session_id)
    while not clustered_dupes:
        clustered_dupes = deduper.matchBlocks(
            genRows(sc_table, proc_table, trained_fields), 
            threshold=threshold
        )
        threshold = threshold - 0.1
        if threshold <= 0.1:
            break
    del deduper
    worker_session.close()
    return clustered_dupes

def genRows(sc_table, proc_table, fields):
    intervals = getRecordIntervals(sc_table)
    field_names = ','.join(['p.{0}'.format(f) for f in fields])
    engine = worker_session.bind
    for clause, args in getRecordRanges(intervals):
        sel = ''' 
          SELECT 
            p.record_id, 
            {field_names}, 
            s.block_id, 
            s.smaller_ids
          FROM "{proc_table}" AS p
          JOIN "{sc_table}" AS s
            ON p.record_id = s.record_id
          {clause}
          ORDER BY s.block_id
        '''.format(field_names=field_names,
                   proc_table=proc_table,
                   sc_table=sc_table,
                   clause=clause)
        lset = set
        block_id = None
        records = []
        for row in engine.execute(text(sel), **args):
            row = dict(zip(row.keys(), row.values()))
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

def getRecordIntervals(small_cov_table):
    interval_size = '50000'
    engine = worker_session.bind
    sel = '''
        SELECT 
          s.block_id AS block_id
        FROM (
          SELECT 
            block_id,
            row_number() OVER (ORDER BY block_id) AS rownum
          FROM "{0}"
        ) AS s
        WHERE rownum %% 50000=1
    '''.format(small_cov_table)
    return [i.block_id for i in engine.execute(sel)]

def getRecordRanges(intervals):
    while intervals:
        int_dict = {'start': intervals.pop(0)}
        if intervals: # pragma: no cover
            int_dict['end'] = intervals[0]
            yield 'WHERE s.block_id >= :start AND s.block_id < :end', int_dict
        else:
            yield 'WHERE s.block_id >= :start', int_dict

@queuefunc
def reDedupeRaw(session_id, threshold=0.75):
    delete = ''' 
        DELETE FROM "entity_{0}" 
        WHERE record_id IN (
          SELECT e.record_id
          FROM "entity_{0}" AS e
          LEFT JOIN "exact_match_{0}" as m
            ON e.record_id = m.record_id
          WHERE m.record_id IS NULL
        )
    '''
    engine = worker_session.bind
    with engine.begin() as conn:
        conn.execute(delete.format(session_id))
    clustered_dupes = clusterDedupe(session_id)
    updateEntityMap(clustered_dupes, session_id)
    entity_count, review_count = updateSessionInfo(session_id)
    dd = worker_session.query(DedupeSession).get(session_id)
    dd.entity_count = entity_count
    dd.review_count = review_count
    dd.status = 'entity map updated'
    worker_session.add(dd)
    worker_session.commit()

@queuefunc
def reDedupeCanon(session_id, threshold=0.90):
    upd = text(''' 
        UPDATE "entity_{0}" SET
            entity_id = subq.old_entity_id,
            last_update = :last_update
        FROM (
            SELECT 
               c.record_id AS old_entity_id,
               e.entity_id AS new_entity_id
            FROM "entity_{0}_cr" AS c
            JOIN "entity_{0}" AS e
                ON c.target_record_id = e.entity_id
            WHERE c.reviewed = TRUE
            ) AS subq
        WHERE "entity_{0}".entity_id = subq.new_entity_id
    '''.format(session_id))
    engine = worker_session.bind
    last_update = datetime.now().replace(tzinfo=TIME_ZONE)
    with engine.begin() as c:
        c.execute(upd, last_update=last_update)

    dedupeCanon(session_id)

@queuefunc
def dedupeRaw(session_id, threshold=0.75):
    trainDedupe(session_id)
    block_gen = blockDedupe(session_id)
    writeBlockingMap(session_id, block_gen, canonical=False)
    clustered_dupes = clusterDedupe(session_id)
    updateEntityMap(clustered_dupes, session_id)
    entity_count, review_count = updateSessionInfo(session_id)
    dd = worker_session.query(DedupeSession).get(session_id)
    dd.entity_count = entity_count
    dd.review_count = review_count
    dd.status = 'entity map updated'
    worker_session.add(dd)
    worker_session.commit()

def updateSessionInfo(session_id, table_fmt='entity_{0}'):
    engine = worker_session.bind
    metadata = MetaData()
    table_name = table_fmt.format(session_id)
    entity_table = Table(table_name, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    entity_count = worker_session.query(entity_table.c.entity_id.distinct())\
        .count()
    review_count = worker_session.query(entity_table.c.entity_id.distinct())\
        .filter(entity_table.c.reviewed == False)\
        .count()
    sel = ''' 
        SELECT 
            entity_id, 
            array_agg(confidence)
        FROM "{0}"
        WHERE reviewed = FALSE
        GROUP BY entity_id
    '''.format(table_name)
    clusters = engine.execute(sel)
    machine = ReviewMachine(clusters)
    dd = worker_session.query(DedupeSession).get(session_id)
    dd.review_machine = pickle.dumps(machine)
    worker_session.add(dd)
    worker_session.commit()
    return entity_count, review_count

@queuefunc
def dedupeCanon(session_id, threshold=0.90):
    engine = worker_session.bind
    
    trainDedupe(session_id)
    
    dd = worker_session.query(DedupeSession).get(session_id)
    metadata = MetaData()
    writeCanonRep(session_id)
    writeProcessedTable(session_id, 
                        proc_table_format='processed_{0}_cr', 
                        raw_table_format='cr_{0}')
    entity_table_name = 'entity_{0}_cr'.format(session_id)
    entity_table = entity_map(entity_table_name, metadata, record_id_type=String)
    entity_table.drop(bind=engine, checkfirst=True)
    entity_table.create(bind=engine)
    block_gen = blockDedupe(session_id, 
        table_name='processed_{0}_cr'.format(session_id), 
        entity_table_name='entity_{0}_cr'.format(session_id), 
        canonical=True)
    writeBlockingMap(session_id, block_gen, canonical=True)
    clustered_dupes = clusterDedupe(session_id, canonical=True, threshold=threshold)
    
    if clustered_dupes:
        writeCanonicalEntities(session_id, clustered_dupes)
        entity_count, review_count = updateSessionInfo(session_id, table_fmt='entity_{0}_cr')
        dd.review_count = review_count
        dd.status = 'canon clustered'
    else: # pragma: no cover
        dd.status = 'canonical'
        dd.review_count = None
    worker_session.add(dd)
    worker_session.commit()

def writeCanonicalEntities(session_id, clustered_dupes):
    cluster_file = StringIO()
    writer = csv.writer(cluster_file)
    for ids, scores in clustered_dupes:
        new_ent = str(uuid4())
        writer.writerow([
            new_ent,
            ids[0],
            scores[0],
            None,
            False,
            False,
        ])
        for id, score in zip(ids[1:], scores):
            writer.writerow([
                new_ent,
                id,
                score,
                ids[0],
                False,
                False,
            ])
    engine = worker_session.bind
    cluster_file.seek(0)
    conn = engine.raw_connection()
    cur = conn.cursor()
    try:
        cur.copy_expert(''' 
            COPY "entity_{0}_cr" (
                entity_id,
                record_id,
                confidence,
                target_record_id,
                reviewed,
                checked_out
            ) 
            FROM STDIN CSV'''.format(session_id), cluster_file)
        conn.commit()
    except (ProgrammingError, IntegrityError) as e: # pragma: no cover
        conn.rollback()
        raise e

@queuefunc
def updateSettingsFiles():
    sessions = worker_session.query(DedupeSession).all()
    for session in sessions:
        if session.settings_file:
            trainDedupe(session.id)

            
