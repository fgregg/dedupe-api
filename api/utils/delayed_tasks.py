import dedupe
from dedupe.serializer import _to_json, _from_json
import os
import json
import time
from io import StringIO, BytesIO
from api.queue import queuefunc
from api.app_config import DB_CONN, DOWNLOAD_FOLDER
from api.models import DedupeSession, User, entity_map
from api.database import worker_session
from api.utils.helpers import clusterGen, \
    makeSampleDict, windowed_query, getDistinct, getMatches, \
    updateEntityCount, RetrainGazetteer, hasMissing, selectWithTuples, \
    getTupleColumns
from api.utils.db_functions import updateEntityMap, writeBlockingMap, \
    writeRawTable, initializeEntityMap, writeProcessedTable, writeCanonRep, \
    addRowHash, addToEntityMap, readTraining
from api.utils.review_machine import ReviewMachine
from sqlalchemy import Table, MetaData, Column, String, func, text, \
    Integer, select
from sqlalchemy.sql import label
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from itertools import groupby
from operator import itemgetter
from collections import OrderedDict
from csvkit import convert
from os.path import join, dirname, abspath
from datetime import datetime
import pickle
import csv
from uuid import uuid4
from api.app_config import TIME_ZONE

def profiler(f):
    def decorated(*args, **kwargs):
        import cProfile, pstats
        
        pr = cProfile.Profile()
        pr.enable()
        rv = f(*args, **kwargs)
        pr.disable()
        s = StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats(.1)
        print(s.getvalue())

        return rv
    return decorated

### Bulk acceptance tasks ###

@queuefunc
def bulkMarkClusters(session_id, user=None):
    engine = worker_session.bind
    now =  datetime.now().replace(tzinfo=TIME_ZONE)
    
    upd_vals = {
        'user_name': user, 
        'clustered': True,
        'match_type': 'bulk accepted',
        'last_update': now,
    }
    
    update_children = updateChildren(session_id)
    with engine.begin() as c:
        child_entities = c.execute(update_children, **upd_vals)
    
    update_parents = updateParents(session_id)
    with engine.begin() as c:
        parent_entities = c.execute(update_parents, **upd_vals)
    
    updateEntityCount(session_id)
    dedupeCanon(session_id)

@queuefunc
def bulkMarkCanonClusters(session_id, user=None):
    engine = worker_session.bind
    upd_vals = {
        'user_name': user, 
        'clustered': True,
        'match_type': 'bulk accepted - canon',
        'last_update': datetime.now().replace(tzinfo=TIME_ZONE)
    }
    update_entity_map = updateFromCanon(session_id)
    with engine.begin() as c:
        updated = c.execute(update_entity_map, **upd_vals)
        for row in updated:
            c.execute(text(''' 
                    UPDATE "entity_{0}_cr" SET
                        target_record_id = :target,
                        clustered = TRUE
                    WHERE record_id = :record_id
                '''.format(session_id)),
                target=row[0], record_id=row[1])
    updateEntityCount(session_id)
    getMatchingReady(session_id)

def updateFromCanon(session_id):
    return text(''' 
        UPDATE "entity_{0}" SET 
            entity_id=subq.entity_id,
            clustered= :clustered,
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

def updateParents(session_id):
    return text(''' 
        UPDATE "entity_{0}" SET
            clustered = :clustered,
            reviewer = :user_name,
            last_update = :last_update,
            match_type = :match_type
        WHERE target_record_id IS NULL
            AND clustered=FALSE
    '''.format(session_id))

def updateChildren(session_id):
    return text(''' 
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
        '''.format(session_id))

### Prepare session to match records ###

@queuefunc
def getMatchingReady(session_id):
    addRowHash(session_id)
    cleanupTables(session_id)
    engine = worker_session.bind
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
    field_defs = json.loads(sess.field_defs.decode('utf-8'))

    # Save Gazetteer settings
    d = dedupe.Gazetteer(field_defs)
    
    training_data = readTraining(session_id)

    d.readTraining(StringIO(json.dumps(training_data, default=_to_json)))
    d.train(ppc=0.1, index_predicates=False)
    g_settings = BytesIO()
    d.writeSettings(g_settings)
    g_settings.seek(0)
    sess.gaz_settings_file = g_settings.getvalue()
    worker_session.add(sess)
    worker_session.commit()

    for field in d.blocker.index_fields:
        fd = (str(f[0]) for f in \
                engine.execute('select distinct {0} from "processed_{1}"'\
                    .format(field, session_id)))
        d.blocker.index(fd, field)
    
    # Write match_block table
    model_fields = list(set([f['field'] for f in field_defs]))
    
    m = MetaData()
    proc = Table('processed_{0}'.format(session_id), m, 
        autoload=True, autoload_with=engine)
    cols = getTupleColumns(proc)
    proc_cols = [Column('record_id', Integer)] + [c for c in cols if c.name in model_fields]
    
    meta = MetaData()
    proc = Table('processed_{0}'.format(session_id), meta, *proc_cols)
    exact = Table('exact_match_{0}'.format(session_id), meta, 
        autoload=True, autoload_with=engine)
    sel = select([proc]).select_from(proc.outerjoin(exact, 
                                     proc.c.record_id == exact.c.record_id))\
                        .where(exact.c.record_id == None)
    rows = engine.execute(sel)
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
    except Exception as e: # pragma: no cover
        conn.rollback()
        raise e
    conn.close()
    try:
        conn = engine.connect()
        trans = conn.begin()
        conn.execute('DROP INDEX "match_blocks_key_{0}_idx"'.format(session_id))
        trans.commit()
    except Exception:
        trans.rollback()
    with engine.begin() as conn:
        conn.execute('''
            CREATE INDEX "match_blocks_key_{0}_idx" 
              ON "match_blocks_{0}" (block_key)
            '''.format(session_id)
        )

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
    worker_session.add(sess)
    worker_session.commit()
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
    # populateHumanReview(session_id)
    del d
    return None

@queuefunc
def populateHumanReview(session_id):
    dedupe_session = worker_session.query(DedupeSession).get(session_id)
    dedupe_session.processing = True
    worker_session.add(dedupe_session)
    worker_session.commit()
    
    engine = worker_session.bind

    raw_fields = sorted(list(set([f['field'] \
            for f in json.loads(dedupe_session.field_defs.decode('utf-8'))])))
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

    while len(human_queue) < 20:
        records = []
        for i in range(500):
            try:
                records.append(next(rows))
            except StopIteration:
                break

        for matches, record in getMatches(session_id, records):
            # check if any of the matches are low confidence
            matches = [match for match in matches if match['confidence'] > 0.2]

            if len(matches) == 1 and matches[0]['confidence'] >= 0.8:
                # Means Auto adding match 
                addToEntityMap(session_id, 
                               record, 
                               match_ids=[m['record_id'] for m in matches],
                               reviewer='machine')
                cleared.append(record['record_id'])
            elif len(matches):
                # Send these to humans
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
            elif len(matches) == 0:
                # Means Auto adding single record entity
                addToEntityMap(session_id, 
                               record, 
                               reviewer='machine')
                cleared.append(record['record_id'])

    reviewed = ''' 
        UPDATE "match_review_{0}" SET 
        reviewed = TRUE,
        reviewer = :reviewer,
        sent_for_review = TRUE
        WHERE record_id IN :ids
    '''.format(session_id)
    with engine.begin() as conn:
        if cleared:
            conn.execute(text(reviewed), ids=tuple(cleared), reviewer='machine')
    
    updateEntityCount(session_id)
    
    # Train classifier
    settings_file = BytesIO(dedupe_session.gaz_settings_file)
    deduper = RetrainGazetteer(settings_file, num_cores=1)
    
    training_data = readTraining(session_id)

    deduper.readTraining(StringIO(json.dumps(training_data, default=_to_json)))
    deduper._trainClassifier()
    fobj = BytesIO()
    deduper.writeSettings(fobj)
    dedupe_session.gaz_settings_file = fobj.getvalue()

    dedupe_session.processing = False
    worker_session.add(dedupe_session)
    worker_session.commit()

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
    trans = conn.begin()
    for table in tables:
        tname = table.format(session_id)
        try:
            conn.execute('DROP TABLE "{0}"'.format(tname))
            trans.commit()
        except Exception as e:
            trans.rollback()
    conn.close()

def drawSample(session_id):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs.decode('utf-8'))
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
            field_defs = json.loads(sess.field_defs.decode('utf-8'))
            fields = list(set([f['field'] for f in field_defs]))
            if init:
                writeProcessedTable(session_id)
            updated_fds = []
            for field in field_defs:
                if field['type'] == 'Categorical':
                    distinct_vals = getDistinct(field['field'], session_id)
                    if len(distinct_vals) <= 6:
                        field.update({'categories': distinct_vals})
                    else:
                        field['type'] = 'Exact'
                if field['type'] in ['Text', 'Set']:
                    corpus = getDistinct(field['field'], session_id)
                    field.update({'corpus': corpus})
                if field['type'] == 'Address':
                    field.update({'log file': '/tmp/addresses.csv'})
                if field['type'] == 'Name':
                    field.update({'log file': '/tmp/name.csv'})
                if hasMissing(field['field'], session_id):
                    field.update({'has_missing': True})
                updated_fds.append(field)
            sess.field_defs = bytes(json.dumps(updated_fds).encode('utf-8'))
            sess.status = 'model defined'
            worker_session.add(sess)
            worker_session.commit()
            if init:
                initializeEntityMap(session_id, fields)
                drawSample(session_id)
            print('got sample')
            break

@queuefunc
def trainDedupe(session_id):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    data_sample = pickle.loads(dd_session.sample)
    field_defs = json.loads(dd_session.field_defs.decode('utf-8'))
    
    deduper = dedupe.Dedupe(field_defs, data_sample=data_sample)
    
    training_data = readTraining(session_id)
    deduper.readTraining(StringIO(json.dumps(training_data, default=_to_json)))
    
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
                engine.execute('select distinct {0} from "{1}"'.format(field, table_name)))
        deduper.blocker.index(fd, field)

    select_block_records = selectBlockRecords(session_id, 
                                              table_name, 
                                              entity_table_name)
    
    full_data = ((getattr(row, 'record_id'), dict(zip(row.keys(), row.values()))) \
        for row in engine.execute(select_block_records))

    return deduper.blocker(full_data)

def selectBlockRecords(session_id, table_name, entity_table_name):
    engine = worker_session.bind
    m = MetaData()
    proc = Table(table_name, m, 
                  autoload=True, autoload_with=engine)
    new_cols = getTupleColumns(proc)
    meta = MetaData()
    new_proc = Table(table_name, meta, *new_cols).alias(name='p')
    entity = Table(entity_table_name, meta, 
                  autoload=True, autoload_with=engine).alias(name='e')
    sel = select([new_proc])\
            .select_from(new_proc.join(entity, 
                             new_proc.c.record_id == entity.c.record_id,
                             isouter=True))\
            .where(entity.c.target_record_id == None)
    return sel


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
    metadata = MetaData()
    p = Table(proc_format.format(session_id), metadata,
        autoload=True, autoload_with=engine)
    proc_cols = getTupleColumns(p)
    trained_fields = list(set([f['field'] for f in \
        json.loads(dd_session.field_defs.decode('utf-8'))]))
    
    m = MetaData()
    proc = Table(proc_format.format(session_id), m, *proc_cols)
    proc_cols = [getattr(proc.c, f) for f in trained_fields]
    small_cov = Table(sc_format.format(session_id), m,
            autoload=True, autoload_with=engine)
    
    cols = [c for c in small_cov.columns] + proc_cols
    rows = worker_session.query(*cols)\
        .join(proc, small_cov.c.record_id == proc.c.record_id)
    fields = [c.name for c in cols]
    clustered_dupes = []
    while not clustered_dupes:
        clustered_dupes = deduper.matchBlocks(
            clusterGen(windowed_query(rows, small_cov.c.block_id, 50000), fields), 
            threshold=threshold
        )
        threshold = threshold - 0.1
        if threshold <= 0.1:
            break
    del rows
    del deduper
    worker_session.close()
    return clustered_dupes

@queuefunc
def reDedupeRaw(session_id, threshold=0.75):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs.decode('utf-8'))
    fields = list(set([f['field'] for f in field_defs]))
    initializeEntityMap(session_id, fields)
    dedupeRaw(session_id, threshold=threshold)
    sess.status = 'entity map updated'
    worker_session.add(sess)
    worker_session.commit()

@queuefunc
def reDedupeCanon(session_id, threshold=0.25):
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
            WHERE c.clustered = TRUE
            ) AS subq
        WHERE "entity_{0}".entity_id = subq.new_entity_id
    '''.format(session_id))
    engine = worker_session.bind
    last_update = datetime.now().replace(tzinfo=TIME_ZONE)
    with engine.begin() as c:
        c.execute(upd, last_update=last_update)
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
    except Exception:
        trans.rollback()
    dedupeCanon(session_id, threshold=threshold)
    sess = worker_session.query(DedupeSession).get(session_id)
    sess.status = 'canon clustered'
    worker_session.add(sess)
    worker_session.commit()

@queuefunc
def dedupeRaw(session_id, threshold=0.75):
    trainDedupe(session_id)
    block_gen = blockDedupe(session_id)
    writeBlockingMap(session_id, block_gen, canonical=False)
    clustered_dupes = clusterDedupe(session_id)
    updateEntityMap(clustered_dupes, session_id)
    engine = worker_session.bind
    metadata = MetaData()
    entity_table = Table('entity_{0}'.format(session_id), metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    entity_count = worker_session.query(entity_table.c.entity_id.distinct())\
        .count()
    review_count = worker_session.query(entity_table.c.entity_id.distinct())\
        .filter(entity_table.c.clustered == False)\
        .count()
    sel = ''' 
        SELECT 
            entity_id, 
            array_agg(confidence)
        FROM "entity_{0}"
        WHERE clustered = FALSE
        GROUP BY entity_id
    '''.format(session_id)
    clusters = engine.execute(sel)
    machine = ReviewMachine(clusters)
    dd = worker_session.query(DedupeSession).get(session_id)
    dd.review_machine = pickle.dumps(machine)
    dd.entity_count = entity_count
    dd.review_count = review_count
    dd.status = 'entity map updated'
    worker_session.add(dd)
    worker_session.commit()

@queuefunc
def dedupeCanon(session_id, threshold=0.25):
    trainDedupe(session_id)
    dd = worker_session.query(DedupeSession).get(session_id)
    engine = worker_session.bind
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
        fname = '/tmp/clusters_{0}.csv'.format(session_id)
        with open(fname, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
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
        with open(fname, 'r', encoding='utf-8') as f:
            conn = engine.raw_connection()
            cur = conn.cursor()
            try:
                cur.copy_expert(''' 
                    COPY "entity_{0}_cr" (
                        entity_id,
                        record_id,
                        confidence,
                        target_record_id,
                        clustered,
                        checked_out
                    ) 
                    FROM STDIN CSV'''.format(session_id), f)
                conn.commit()
                os.remove(fname)
            except Exception as e: # pragma: no cover
                conn.rollback()
                raise e
    else: # pragma: no cover
        print('did not find clusters')
        getMatchingReady(session_id)
    review_count = worker_session.query(entity_table.c.entity_id.distinct())\
        .filter(entity_table.c.clustered == False)\
        .count()
    sel = ''' 
        SELECT 
            entity_id, 
            array_agg(confidence)
        FROM "entity_{0}_cr"
        WHERE clustered = FALSE
        GROUP BY entity_id
    '''.format(session_id)
    clusters = engine.execute(sel)
    machine = ReviewMachine(clusters)
    dd.review_machine = pickle.dumps(machine)
    dd.review_count = review_count
    dd.status = 'canon clustered'
    worker_session.add(dd)
    worker_session.commit()
