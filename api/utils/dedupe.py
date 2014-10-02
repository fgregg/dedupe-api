import os
from api.utils.helpers import getEngine, createSession, preProcess
from api.models import entity_map, DedupeSession
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader
from sqlalchemy import Table, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from hashlib import md5

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')

class DedupeFileError(Exception): 
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message
class WebDeduper(object):
    
    def __init__(self, deduper,
            conn_string=None,
            recall_weight=2,
            session_key=None):
        self.deduper = deduper
        self.recall_weight = float(recall_weight)
        self.conn_string = conn_string
        self.session_key = session_key
        self.db_session = createSession(conn_string)
        self.dd_session = self.db_session.query(DedupeSession).get(session_key)
        self.training_data = StringIO(self.dd_session.training_data)
        # Will need to figure out static dedupe, maybe
        self.deduper.readTraining(self.training_data)
        self.deduper.train()
        settings_file_obj = StringIO()
        self.deduper.writeSettings(settings_file_obj)
        self.dd_session.settings_file = settings_file_obj.getvalue()
        self.db_session.add(self.dd_session)
        self.db_session.commit()

    def dedupe(self):
        data_d = makeDataDict(self.dd_session.conn_string, 
            self.dd_session.id, table_name=self.dd_session.table_name)
        threshold = self.deduper.threshold(data_d, recall_weight=self.recall_weight)
        clustered_dupes = self.deduper.match(data_d, threshold)
        writeEntityMap(clustered_dupes, self.session_key, self.conn_string, data_d)
        dd_tuples = ((k,v) for k,v in data_d.items())
        block_data = self.deduper.blocker(dd_tuples)
        writeBlockingMap(self.conn_string, self.session_key, block_data)
        return 'ok'

def writeRawTable(conn_string=None, 
              filename=None,
              session_key=None,
              file_obj=None):
    """ 
    Create a table from incoming tabular data
    """
    file_format = convert.guess_format(filename)
    converted = StringIO(convert.convert(file_obj, file_format))
    fieldnames = converted.next().strip('\r\n').split(',')
    converted.seek(0)
    cols = []
    for field in fieldnames:
        cols.append(Column(field, String))
    engine = getEngine(conn_string)
    conn = engine.connect()
    trans = conn.begin()
    Base = declarative_base()
    sql_table = Table('raw_%s' % session_key, Base.metadata, *cols)
    sql_table.append_column(Column('record_id', Integer, primary_key=True))
    sql_table.create(bind=engine, checkfirst=True)
    reader = UnicodeCSVDictReader(converted)
    for row in reader:
        conn.execute(sql_table.insert(), **row)
    trans.commit()
    conn.close()

def writeEntityMap(clustered_dupes, session_key, conn_string, data_d):
    """ 
    Write entity map table
    """
    Base = declarative_base()
    dt = entity_map('entity_%s' % session_key, Base.metadata)
    engine = getEngine(conn_string)
    dt.create(bind=engine, checkfirst=True)
    rows = []
    db_session = createSession(conn_string)
    sess = db_session.query(DedupeSession).get(session_key)
    field_defs = json.loads(sess.field_defs)
    model_fields = [f['field'] for f in field_defs]
    raw_session = createSession(sess.conn_string)
    raw_engine = raw_session.bind
    raw_base = declarative_base()
    raw_table = Table(sess.table_name, raw_base.metadata, 
        autoload=True, autoload_with=raw_engine)
    raw_cols = [getattr(raw_table.c, f) for f in model_fields]
    pk_col = [p for p in raw_table.primary_key][0]
    for cluster_id, cluster in enumerate(clustered_dupes):
        id_set, confidence_score = cluster
        cluster_list = [{'row_id': c, 'row': data_d[c]} for c in id_set]
        for member in cluster_list:
            obj = raw_session.query(*raw_cols)\
                .filter(pk_col == member['row_id'])\
                .first()
            hash_me = ';'.join([preProcess(unicode([i][0])) for i in obj])
            md5_hash = md5(hash_me).hexdigest()
            m = {
                'group_id': cluster_id,
                'confidence': float(confidence_score),
                'record_id': member['row_id'],
                'clustered': False,
                'source_hash': md5_hash,
            }
            rows.append(m)
    conn = engine.contextual_connect()
    conn.execute(dt.insert(), rows)

def writeBlockingMap(conn_string, session_key, block_data):
    bkm = block_map_table('block_%s' % session_key, Base.metadata)
    engine = getEngine(conn_string)
    bkm.create(bind=engine, checkfirst=True)
    conn = engine.contextual_connect()
    insert_data = []
    for key, record_id in block_data:
        insert_data.append({'block_key': key, 'record_id': record_id})
    conn.execute(bkm.insert(), insert_data)

def writeCanonTable(session_id):
    cols = []
    app_session = createSession(DB_CONN)
    app_engine = app_session.bind
    dd_sess = app_session.query(DedupeSession).get(session_id)
    raw_engine = getEngine(dd_sess.conn_string)
    raw_base = declarative_base()
    raw_table = Table(dd_sess.table_name, raw_base.metadata,
        autoload=True, autoload_with=raw_engine)
    for col in raw_table.columns:
        kwargs = {}
        if col.type == Integer:
            kwargs['default'] = 0
        if col.type == Float:
            kwargs['default'] = 0.0
        if col.type == Boolean:
            kwargs['default'] = None
        cols.append(Column(col.name, col.type, **kwargs))
    canon_table = Table('canon_%s' % session_id, Base.metadata,
        *cols, extend_existing=True)
    canon_table.append_column(Column('canon_record_id', Integer, primary_key=True))
    canon_table.create(bind=app_engine)
    app_session.close()

