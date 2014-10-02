import dedupe
from api.queue import queuefunc
from api.utils.helpers import createSession, preProcess
from api.utils.dedupe import writeCanonTable, WebDeduper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table
from itertools import groupby
from operator import itemgetter

@queuefunc
def dedupeit(**kwargs):
    app_session = createSession(DB_CONN)
    dd_session = app_session.query(DedupeSession).get(kwargs['session_key'])
    d = dedupe.Dedupe(json.loads(dd_session.field_defs), 
        data_sample=kwargs['data_sample'])
    deduper = WebDeduper(d, 
        conn_string=DB_CONN,
        session_key=dd_session.id)
    app_session.close()
    files = deduper.dedupe()
    del d
    return files

@queuefunc
def retrain(session_key):
    db_session = createSession(DB_CONN)
    sess = db_session.query(DedupeSession).get(session_key)
    gaz = dedupe.Gazetteer(json.loads(sess.field_defs))
    gaz.readTraining(StringIO(sess.training_data))
    gaz.train()
    gaz_set = StringIO()
    gaz.writeSettings(gaz_set)
    s = gaz_set.getvalue()
    sess.gaz_settings_file = s
    db_session.add(sess)
    db_session.commit()
    db_session.close()
    return None

@queuefunc
def getSample(conn_string,
                session_key, 
                primary_key=None, 
                table_name=None,
                sample_size=100000):
    session = createSession(conn_string)
    engine = session.bind
    if not table_name:
        table_name = 'raw_%s' % session_key
    Base = declarative_base()
    table = Table(table_name, Base.metadata, 
        autoload=True, autoload_with=engine)
    if not primary_key:
        try:
            primary_key = [p.name for p in table.primary_key][0]
        except IndexError:
            # need to figure out what to do in this case
            print 'no primary key'
    fields = [str(s) for s in table.columns.keys()]
    temp_d = {}
    row_count = session.query(table).count()
    if row_count < sample_size:
        sample_size = row_count
    random_pairs = dedupe.randomPairs(sample_size, 500000)
    data_rows = session.query(table).limit(sample_size).all()
    for i, row in enumerate(data_rows):
        d_row = {k: unicode(v) for (k,v) in zip(fields, row)}
        clean_row = [(k, preProcess(v)) for (k,v) in d_row.items()]
        temp_d[i] = dedupe.core.frozendict(clean_row)
    pair_sample = [(temp_d[k1], temp_d[k2])
                    for k1, k2 in random_pairs]
    session.close()
    return pair_sample, fields

@queuefunc
def makeCanonicalTable(session_id):
    app_session = createSession(DB_CONN)
    app_engine = app_session.bind
    Base = declarative_base()
    entity_table = Table('entity_%s' % session_id, Base.metadata,
        autoload=True, autoload_with=app_engine, extend_existing=True)
    clusters = app_session.query(entity_table.c.group_id, 
            entity_table.c.record_id)\
            .filter(entity_table.c.clustered == True)\
            .order_by(entity_table.c.group_id)\
            .all()
    groups = {}
    for k,g in groupby(clusters, key=itemgetter(0)):
        groups[k] = [i[1] for i in list(g)]
    dd_sess = app_session.query(DedupeSession).get(session_id)
    gaz = dedupe.Gazetteer(json.loads(dd_sess.field_defs))
    gaz.readTraining(StringIO(dd_sess.training_data))
    gaz.train()
    gaz_set = StringIO()
    gaz.writeTraining(gaz_set)
    dd_sess.gaz_settings_file = gaz_set.getvalue()
    app_session.add(dd_sess)
    app_session.commit()
    raw_session = createSession(dd_sess.conn_string)
    raw_engine = raw_session.bind
    raw_base = declarative_base()
    raw_table = Table(dd_sess.table_name, raw_base.metadata,
        autoload=True, autoload_with=raw_engine, extend_existing=True)
    raw_fields = [c for c in raw_table.columns.keys()]
    primary_key = [p.name for p in raw_table.primary_key][0]
    canonical_rows = []
    for k,v in groups.items():
        cluster_rows = raw_session.query(raw_table)\
            .filter(getattr(raw_table.c, primary_key).in_(v)).all()
        rows_d = []
        for row in cluster_rows:
            d = {}
            for k,v in zip(raw_fields, row):
                if k != 'record_id':
                    d[k] = preProcess(unicode(v))
            rows_d.append(d)
        canonical_rows.append(dedupe.canonicalize(rows_d))
    writeCanonTable(session_id)
    canon_table = Table('canon_%s' % session_id, Base.metadata,
        autoload=True, autoload_with=app_engine, extend_existing=True)
    conn = app_engine.contextual_connect()
    conn.execute(canon_table.insert(), canonical_rows)
    app_session.close()
    raw_session.close()