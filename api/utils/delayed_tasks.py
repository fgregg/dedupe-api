import dedupe
import json
from cStringIO import StringIO
from api.queue import queuefunc
from api.app_config import DB_CONN, DOWNLOAD_FOLDER
from api.models import DedupeSession
from api.database import worker_session
from api.utils.helpers import preProcess, makeDataDict
from api.utils.dedupe_functions import WebDeduper
from api.utils.db_functions import writeCanonTable
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

@queuefunc
def dedupeit(**kwargs):
    dd_session = worker_session.query(DedupeSession).get(kwargs['session_id'])
    d = dedupe.Dedupe(json.loads(dd_session.field_defs), 
        data_sample=kwargs['data_sample'])
    deduper = WebDeduper(d, session_id=dd_session.id)
    dd_session.status = 'dedupe started'
    worker_session.add(dd_session)
    worker_session.commit()
    files = deduper.dedupe()
    dd_session.status = 'review queue ready'
    worker_session.add(dd_session)
    worker_session.commit()
    del d
    return files

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

@queuefunc
def makeCanonicalTable(session_id):
    engine = worker_session.bind
    metadata = MetaData()
    entity_table = Table('entity_%s' % session_id, metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    clusters = worker_session.query(entity_table.c.group_id, 
            entity_table.c.record_id)\
            .filter(entity_table.c.clustered == True)\
            .order_by(entity_table.c.group_id)\
            .all()
    groups = {}
    for k,g in groupby(clusters, key=itemgetter(0)):
        groups[k] = [i[1] for i in list(g)]
    dd_sess = worker_session.query(DedupeSession).get(session_id)
    gaz = dedupe.Gazetteer(json.loads(dd_sess.field_defs))
    gaz.readTraining(StringIO(dd_sess.training_data))
    gaz.train()
    gaz_set = StringIO()
    gaz.writeTraining(gaz_set)
    dd_sess.gaz_settings_file = gaz_set.getvalue()
    worker_session.add(dd_sess)
    worker_session.commit()
    raw_table = Table(dd_sess.table_name, metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    raw_fields = [c for c in raw_table.columns.keys()]
    primary_key = [p.name for p in raw_table.primary_key][0]
    canonical_rows = []
    for key,value in groups.items():
        cluster_rows = worker_session.query(raw_table)\
            .filter(getattr(raw_table.c, primary_key).in_(value)).all()
        rows_d = []
        for row in cluster_rows:
            d = {}
            for k,v in zip(raw_fields, row):
                if k != 'record_id':
                    d[k] = preProcess(unicode(v))
            rows_d.append(d)
        canonical_form = dedupe.canonicalize(rows_d)
        canonical_form['entity_id'] = key
        canonical_rows.append(canonical_form)
    writeCanonTable(session_id)
    canon_table = Table('canon_%s' % session_id, metadata,
        autoload=True, autoload_with=engine, extend_existing=True)
    engine.execute(canon_table.insert(), canonical_rows)
