import dedupe
import os
import json
import time
from cStringIO import StringIO
from api.queue import queuefunc
from api.app_config import DB_CONN, DOWNLOAD_FOLDER
from api.models import DedupeSession, User, entity_map
from api.database import worker_session
from api.utils.helpers import preProcess, makeDataDict, clusterGen, \
    makeSampleDict, windowed_query
from api.utils.db_functions import updateEntityMap, writeBlockingMap, \
    writeRawTable, initializeEntityMap, writeProcessedTable
from sqlalchemy import Table, MetaData, Column, String, func
from sqlalchemy.sql import label
from itertools import groupby
from operator import itemgetter
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader, UnicodeCSVReader, \
    UnicodeCSVWriter
from os.path import join, dirname, abspath
from datetime import datetime
import cPickle

def drawSample(session_id):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs)
    fields = [f['field'] for f in field_defs]
    d = dedupe.Dedupe(field_defs)
    data_d = makeSampleDict(sess.id, fields=fields)
    if len(data_d) < 50001:
        sample_size = 5000
    else:
        sample_size = round(int(len(data_d) * 0.01), -3)
    d.sample(data_d, sample_size=sample_size, blocked_proportion=1)
    sess.sample = cPickle.dumps(d.data_sample)
    worker_session.add(sess)
    worker_session.commit()

@queuefunc
def initializeSession(session_id, filename):
    file_obj = open('/tmp/%s_raw.csv' % session_id, 'rb')
    kwargs = {
        'session_id':session_id,
        'filename': filename,
        'file_obj':file_obj
    }
    writeRawTable(**kwargs)
    print 'session initialized'
    os.remove('/tmp/%s_raw.csv' % session_id)

@queuefunc
def initializeModel(session_id):
    sess = worker_session.query(DedupeSession).get(session_id)
    while True:
        worker_session.refresh(sess, ['field_defs', 'sample'])
        if not sess.field_defs:
            time.sleep(3)
        else:
            print 'found field_defs'
            fields = [f['field'] for f in json.loads(sess.field_defs)]
            initializeEntityMap(session_id, fields)
            print 'made entity map'
            drawSample(session_id)
            print 'got sample'
            break
    return 'woo'

def trainDedupe(session_id):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    data_sample = cPickle.loads(dd_session.sample)
    deduper = dedupe.Dedupe(json.loads(dd_session.field_defs), 
        data_sample=data_sample)
    training_data = StringIO(dd_session.training_data)
    deduper.readTraining(training_data)
    deduper.train()
    settings_file_obj = StringIO()
    deduper.writeSettings(settings_file_obj)
    dd_session.settings_file = settings_file_obj.getvalue()
    worker_session.add(dd_session)
    worker_session.commit()
    deduper.cleanupTraining()

def blockDedupe(session_id, 
                table_name=None, 
                entity_table_name=None, 
                canonical=False):

    if not table_name:
        table_name = 'processed_{0}'.format(session_id)
    if not entity_table_name:
        entity_table_name = 'entity_{0}'.format(session_id)
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    deduper = dedupe.StaticDedupe(StringIO(dd_session.settings_file))
    engine = worker_session.bind
    metadata = MetaData()
    proc_table = Table(table_name, metadata,
        autoload=True, autoload_with=engine)
    entity_table = Table(entity_table_name, metadata,
        autoload=True, autoload_with=engine)
    for field in deduper.blocker.tfidf_fields:
        fd = worker_session.query(proc_table.c.record_id, 
            getattr(proc_table.c, field))
        field_data = (row for row in fd.yield_per(50000))
        deduper.blocker.tfIdfBlock(field_data, field)
        del field_data
    """ 
    SELECT p.* <-- need the fields that we trained on at least
        FROM processed as p
        LEFT OUTER JOIN entity_map as e
           ON s.record_id = e.record_id
        WHERE e.target_record_id IS NULL
    """
    proc_records = worker_session.query(proc_table)\
        .outerjoin(entity_table, proc_table.c.record_id == entity_table.c.record_id)\
        .filter(entity_table.c.target_record_id == None)
    fields = proc_table.columns.keys()
    full_data = ((getattr(row, 'record_id'), dict(zip(fields, row))) \
        for row in proc_records.yield_per(50000))
    block_gen = deduper.blocker(full_data)
    writeBlockingMap(session_id, block_gen, canonical=canonical)

def clusterDedupe(session_id, canonical=False):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    deduper = dedupe.StaticDedupe(StringIO(dd_session.settings_file))
    engine = worker_session.bind
    metadata = MetaData()
    sc_format = 'small_cov_{0}'
    proc_format = 'processed_{0}'
    if canonical:
        sc_format = 'small_cov_{0}_cr'
        proc_format = 'cr_{0}'
    small_cov = Table(sc_format.format(session_id), metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    proc = Table(proc_format.format(session_id), metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    trained_fields = [f['field'] for f in json.loads(dd_session.field_defs)]
    proc_cols = [getattr(proc.c, f) for f in trained_fields]
    cols = [c for c in small_cov.columns] + proc_cols
    rows = worker_session.query(*cols)\
        .join(proc, small_cov.c.record_id == proc.c.record_id)
    fields = [c.name for c in cols]
    clustered_dupes = deduper.matchBlocks(
        clusterGen(windowed_query(rows, small_cov.c.block_id, 50000), fields), 
        threshold=0.75
    )
    if not clustered_dupes:
        clustered_dupes = deduper.matchBlocks(
            clusterGen(windowed_query(rows, small_cov.c.block_id, 50000), fields), 
            threshold=0.5
        )
    return clustered_dupes

@queuefunc
def dedupeRaw(session_id):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    trainDedupe(session_id)
    blockDedupe(session_id)
    clustered_dupes = clusterDedupe(session_id)
    review_count = updateEntityMap(clustered_dupes, session_id)
    dd_session.status = 'entity map created'
    worker_session.add(dd_session)
    worker_session.commit()
    return 'ok'

@queuefunc
def dedupeCanon(session_id):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    engine = worker_session.bind
    metadata = MetaData()
    entity = Table('entity_{0}'.format(session_id), metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    proc_table = Table('processed_{0}'.format(session_id), metadata,
        autoload=True, autoload_with=engine, keep_existing=True)

    cr_cols = [Column('record_id', String, primary_key=True)]
    for col in proc_table.columns:
        if col.name != 'record_id':
            cr_cols.append(Column(col.name, col.type))
    cr = Table('cr_{0}'.format(session_id), metadata, *cr_cols)
    cr.create(bind=engine, checkfirst=True)

    cols = [entity.c.entity_id]
    col_names = [c for c in proc_table.columns.keys() if c != 'record_id']
    for name in col_names:
        cols.append(label(name, func.array_agg(getattr(proc_table.c, name))))
    rows = worker_session.query(*cols)\
        .filter(entity.c.record_id == proc_table.c.record_id)\
        .group_by(entity.c.entity_id)
    names = cr.columns.keys()
    with open('/tmp/cr_{0}.csv'.format(session_id), 'wb') as f:
        writer = UnicodeCSVWriter(f)
        writer.writerow(names)
        for row in rows:
            r = [row.entity_id]
            dicts = [dict(**{n:None for n in col_names}) for i in range(len(row[1]))]
            for idx, dct in enumerate(dicts):
                for name in col_names:
                    dicts[idx][name] = getattr(row, name)[idx]
            canon_form = dedupe.canonicalize(dicts)
            r.extend([canon_form[k] for k in names if canon_form.get(k) is not None])
            writer.writerow(r)
    canon_table_name = 'cr_{0}'.format(session_id)
    copy_st = 'COPY "{0}" ('.format(canon_table_name)
    for idx, name in enumerate(names):
        if idx < len(names) - 1:
            copy_st += '"{0}", '.format(name)
        else:
            copy_st += '"{0}")'.format(name)
    else:
        copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
    conn = engine.raw_connection()
    cur = conn.cursor()
    with open('/tmp/cr_{0}.csv'.format(session_id), 'rb') as f:
        cur.copy_expert(copy_st, f)
    conn.commit()
    writeProcessedTable(session_id, 
                        proc_table_format='processed_{0}_cr', 
                        raw_table_format='cr_{0}')
    entity_table_name = 'entity_{0}_cr'.format(session_id)
    entity_table = entity_map(entity_table_name, metadata, record_id_type=String)
    entity_table.create(bind=engine, checkfirst=True)
    blockDedupe(session_id, 
        table_name='processed_{0}_cr'.format(session_id), 
        canonical=True)
    clustered_dupes = clusterDedupe(session_id, canonical=True)
    cluster_count = updateEntityMap(clustered_dupes,
        session_id, 
        raw_table=canon_table_name, 
        entity_table=entity_table_name)
    dd_session.status = 'canon clustered'
    worker_session.add(dd_session)
    worker_session.commit()
    return 'ok'

@queuefunc
def retrain(session_id):
    sess = worker_session.query(DedupeSession).get(session_id)
    gaz = dedupe.Gazetteer(json.loads(sess.field_defs))
    gaz.readTraining(StringIO(sess.training_data))
    gaz.train()
    gaz_set = StringIO()
    gaz.writeSettings(gaz_set)
    s = gaz_set.getvalue()
    sess.gaz_settings_file = s
    worker_session.add(sess)
    worker_session.commit()
    return None

# @queuefunc
# def makeCanonicalTable(session_id):
#     engine = worker_session.bind
#     metadata = MetaData()
#     entity_table = Table('entity_%s' % session_id, metadata,
#         autoload=True, autoload_with=engine, extend_existing=True)
#     clusters = worker_session.query(entity_table.c.group_id, 
#             entity_table.c.record_id)\
#             .filter(entity_table.c.clustered == True)\
#             .order_by(entity_table.c.group_id)\
#             .all()
#     groups = {}
#     for k,g in groupby(clusters, key=itemgetter(0)):
#         groups[k] = [i[1] for i in list(g)]
#     dd_sess = worker_session.query(DedupeSession).get(session_id)
#     gaz = dedupe.Gazetteer(json.loads(dd_sess.field_defs))
#     gaz.readTraining(StringIO(dd_sess.training_data))
#     gaz.train()
#     gaz_set = StringIO()
#     gaz.writeTraining(gaz_set)
#     dd_sess.gaz_settings_file = gaz_set.getvalue()
#     worker_session.add(dd_sess)
#     worker_session.commit()
#     raw_table = Table('raw_%s' % session_id, metadata,
#         autoload=True, autoload_with=engine, extend_existing=True)
#     raw_fields = [c for c in raw_table.columns.keys()]
#     primary_key = [p.name for p in raw_table.primary_key][0]
#     canonical_rows = []
#     for key,value in groups.items():
#         cluster_rows = worker_session.query(raw_table)\
#             .filter(getattr(raw_table.c, primary_key).in_(value)).all()
#         rows_d = []
#         for row in cluster_rows:
#             d = {}
#             for k,v in zip(raw_fields, row):
#                 if k != 'record_id':
#                     d[k] = preProcess(unicode(v))
#             rows_d.append(d)
#         canonical_form = dedupe.canonicalize(rows_d)
#         canonical_form['entity_id'] = key
#         canonical_rows.append(canonical_form)
#     makeCanonTable(session_id)
#     canon_table = Table('canon_%s' % session_id, metadata,
#         autoload=True, autoload_with=engine, extend_existing=True)
#     engine.execute(canon_table.insert(), canonical_rows)

@queuefunc
def bulkMatchWorker(session_id, file_contents, field_map, filename):
    ftype = convert.guess_format(filename)
    s = StringIO(file_contents)
    result = {
        'status': 'ok',
        'result': '',
        'message': ''
    }
    try:
        converted = convert.convert(s, ftype)
    except UnicodeDecodeError:
        result['status'] = 'error'
        result['message'] = 'Problem decoding file'
        return result
    sess = worker_session.query(DedupeSession).get(session_id)
    model_fields = [f.get('field') for f in json.loads(sess.field_defs)]
    s = StringIO(converted)
    reader = UnicodeCSVDictReader(s)
    rows = []
    for row in reader:
        r = {k: row.get(k, '') for k in field_map.values() if k}
        e = {k: '' for k in model_fields}
        for k,v in field_map.items():
            e[k] = r.get(v, '')
        rows.append(e)
    # Need a thing that will make a data_d without a DB
    data_d = iterDataDict(rows)
    deduper = dedupe.StaticGazetteer(StringIO(sess.gaz_settings_file))
    trained_data_d = makeDataDict(session_id, worker=True)
    deduper.index(trained_data_d)
    linked = deduper.match(messy_data_d, threshold=0, n_matches=5)
    s.seek(0)
    reader = UnicodeCSVReader(s)
    raw_header = reader.next()
    raw_header.extend([f for f in field_map.keys()])
    raw_rows = list(reader)
    fname = '%s_%s' % (datetime.now().isoformat(), filename)
    fpath = join(DOWNLOAD_FOLDER, fname)
    with open(fpath, 'wb') as outp:
        writer = UnicodeCSVWriter(outp)
        writer.writerow(raw_header)
        for link in linked:
            for l in link:
                id_set, conf = l
                messy_id, trained_id = id_set
                messy_row = raw_rows[int(messy_id)]
                trained_row = trained_data_d[trained_id]
                for k in field_map.keys():
                    messy_row.append(trained_row.get(k))
                writer.writerow(messy_row)
    result['result'] = fname
    return result
