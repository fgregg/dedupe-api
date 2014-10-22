import dedupe
import json
from cStringIO import StringIO
from api.queue import queuefunc
from api.app_config import DB_CONN, DOWNLOAD_FOLDER
from api.models import DedupeSession
from api.database import worker_session
from api.utils.helpers import preProcess, makeDataDict
from api.utils.db_functions import makeCanonTable, writeEntityMap, \
    rewriteEntityMap
from sqlalchemy import Table, MetaData
from itertools import groupby
from operator import itemgetter
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader, UnicodeCSVReader, \
    UnicodeCSVWriter
from os.path import join, dirname, abspath
from datetime import datetime

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
    table_name = None
    trained_data_d = makeDataDict(session_id, table_name=table_name, worker=True)
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

def runDedupe(dd_session, deduper, data_d):
    training_data = StringIO(dd_session.training_data)
    deduper.readTraining(training_data)
    deduper.train()
    settings_file_obj = StringIO()
    deduper.writeSettings(settings_file_obj)
    dd_session.settings_file = settings_file_obj.getvalue()
    worker_session.add(dd_session)
    worker_session.commit()
    threshold = deduper.threshold(data_d, recall_weight=1)
    clustered_dupes = deduper.match(data_d, threshold)
    return clustered_dupes

@queuefunc
def dedupeRaw(session_id, data_sample):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    deduper = dedupe.Dedupe(json.loads(dd_session.field_defs), 
        data_sample=data_sample)
    data_d = makeDataDict(dd_session.id, worker=True)
    clustered_dupes = runDedupe(dd_session, deduper, data_d)
    review_count = writeEntityMap(clustered_dupes, session_id, data_d)
    if not review_count:
        dedupeCanon(session_id)
   #dd_tuples = ((k,v) for k,v in data_d.items())
   #block_data = deduper.blocker(dd_tuples)
   #writeBlockingMap(session_id, block_data)
    return 'ok'

@queuefunc
def dedupeCanon(session_id):
    dd_session = worker_session.query(DedupeSession)\
        .get(session_id)
    deduper = dedupe.Dedupe(json.loads(dd_session.field_defs))
    data_d = makeDataDict(dd_session.id, 
        worker=True, table_name='canon_%s' % session_id)
    deduper.sample(data_d, sample_size=5000, rand_p=0)
    sample = deduper.data_sample
    clustered_dupes = runDedupe(dd_session, deduper, data_d)
    review_count = rewriteEntityMap(clustered_dupes, session_id, data_d)
    return session_id

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
