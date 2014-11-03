import dedupe
import json
import time
from cStringIO import StringIO
from api.queue import queuefunc
from api.app_config import DB_CONN, DOWNLOAD_FOLDER
from api.models import DedupeSession, User
from api.database import worker_session
from api.utils.helpers import preProcess, makeDataDict, clusterGen
from api.utils.db_functions import makeCanonTable, writeEntityMap, \
    rewriteEntityMap, writeBlockingMap, writeRawTable
from sqlalchemy import Table, MetaData
from itertools import groupby
from operator import itemgetter
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader, UnicodeCSVReader, \
    UnicodeCSVWriter
from os.path import join, dirname, abspath
from datetime import datetime
import cPickle

@queuefunc
def initializeSession(session_id, filename, file_contents):
    file_obj = StringIO(file_contents)
    writeRawTable(session_id=session_id,
        filename=filename,
        file_obj=file_obj)
    del file_obj
    sess = worker_session.query(DedupeSession).get(session_id)
    data_d = None
    while True:
        worker_session.refresh(sess, ['field_defs', 'sample'])
        if not sess.field_defs:
            time.sleep(3)
        else:
            field_defs = json.loads(sess.field_defs)
            fields = [f['field'] for f in field_defs]
            d = dedupe.Dedupe(field_defs)
            data_d = makeDataDict(sess.id, fields=fields)
            if len(data_d) <= 50000:
                sample_size = 5000
            else:
                sample_size = round(int(len(data_d) * 0.01), -3)
            print 'sample size: %s' % sample_size
            start = time.time()
            d.sample(data_d, sample_size=sample_size, blocked_proportion=1)
            end = time.time()
            print 'sample time %s' % (end - start)
            sess.sample = cPickle.dumps(d.data_sample)
            worker_session.add(sess)
            worker_session.commit()
            break
    del data_d
    return 'woo'

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

def trainDedupe(dd_session, deduper):
    training_data = StringIO(dd_session.training_data)
    deduper.readTraining(training_data)
    start = time.time()
    deduper.train()
    end = time.time()
    print 'training %s' % (end - start)
    settings_file_obj = StringIO()
    deduper.writeSettings(settings_file_obj)
    dd_session.settings_file = settings_file_obj.getvalue()
    worker_session.add(dd_session)
    worker_session.commit()
    deduper.cleanupTraining()
    
def blockDedupe(session_id, deduper):
    engine = worker_session.bind
    metadata = MetaData()
    proc_table = Table('processed_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    for field in deduper.blocker.tfidf_fields:
        fd = worker_session.query(proc_table.c.record_id, 
            getattr(proc_table.c, field)).yield_per(50000)
        field_data = (row for row in fd)
        deduper.blocker.tfIdfBlock(field_data, field)
    proc_records = worker_session.query(proc_table)\
        .yield_per(50000)
    fields = proc_table.columns.keys()
    full_data = ((getattr(row, 'record_id'), dict(zip(fields, row))) \
        for row in proc_records)
    start = time.time()
    blocked_data = deduper.blocker(full_data)
    end = time.time()
    print 'blocking took %s' % (end - start)
    start = time.time()
    writeBlockingMap(session_id, blocked_data)
    end = time.time()
    print 'wrote blocking map in %s' % (end - start)
    
def findClusters(session_id, deduper):
    engine = worker_session.bind
    metadata = MetaData()
    small_cov = Table('small_cov_%s' % session_id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    proc = Table('processed_%s' % session_id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    rows = worker_session.query(small_cov, proc)\
        .join(proc, small_cov.c.record_id == proc.c.record_id)\
        .order_by(small_cov.c.block_id)\
        .yield_per(50000)
    fields = small_cov.columns.keys() + proc.columns.keys()
    clustered_dupes = deduper.matchBlocks(clusterGen(rows, fields), threshold=0.75)
    return clustered_dupes

@queuefunc
def dedupeRaw(session_id, data_sample):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    deduper = dedupe.Dedupe(json.loads(dd_session.field_defs), 
        data_sample=data_sample)
    trainDedupe(dd_session, deduper)
    blockDedupe(session_id, deduper)
    clustered_dupes = findClusters(session_id, deduper)
    makeCanonTable(session_id)
    review_count = writeEntityMap(clustered_dupes, session_id)
    dd_session.status = 'entity map created'
    worker_session.add(dd_session)
    worker_session.commit()
    print review_count
   #if not review_count:
   #    dd_session.status = 'first pass review complete'
   #    worker_session.add(dd_session)
   #    worker_session.commit()
   #    dedupeCanon(session_id)
    return 'ok'

@queuefunc
def dedupeCanon(session_id):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    deduper = dedupe.StaticDedupe(StringIO(dd_session.settings_file))
    data_d = makeDataDict(dd_session.id, 
        worker=True, table_name='canon_%s' % session_id, sample=True)
    deduper.sample(data_d, sample_size=5000, blocked_proportion=1)
    sample = deduper.data_sample
    clustered_dupes = runDedupe(dd_session, deduper, data_d)
    review_count = rewriteEntityMap(clustered_dupes, session_id, data_d)
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
