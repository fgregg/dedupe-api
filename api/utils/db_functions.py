import json
from cStringIO import StringIO
from hashlib import md5
from api.database import app_session, worker_session
from api.models import DedupeSession, entity_map, block_map_table
from api.utils.helpers import preProcess
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader
from sqlalchemy import MetaData, Table, Column, Integer, String, \
    create_engine, Float, Boolean
from unidecode import unidecode

def writeRawTable(filename=None,
              session_id=None,
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
    engine = app_session.bind
    metadata = MetaData()
    sql_table = Table('raw_%s' % session_id, metadata, *cols)
    sql_table.append_column(Column('record_id', Integer, primary_key=True))
    sql_table.create(engine)
    names = [c.name for c in sql_table.columns if c.name != 'record_id']
    copy_st = 'COPY "raw_%s" (' % session_id
    for idx, name in enumerate(names):
        if idx < len(names) - 1:
            copy_st += '"%s", ' % name
        else:
            copy_st += '"%s")' % name
    else:
        copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
    converted.seek(0)
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.copy_expert(copy_st, converted)
    conn.commit()
    return fieldnames

def writeEntityMap(clustered_dupes, session_id, data_d):
    """ 
    Write entity map table
    """
    engine = worker_session.bind
    metadata = MetaData()
    dt = entity_map('entity_%s' % session_id, metadata)
    dt.create(engine, checkfirst=True)
    rows = []
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs)
    model_fields = [f['field'] for f in field_defs]
    raw_table = Table(sess.table_name, metadata, 
        autoload=True, autoload_with=engine)
    raw_cols = [getattr(raw_table.c, f) for f in model_fields]
    pk_col = [p for p in raw_table.primary_key][0]
    raw_cols.append(pk_col)
    for cluster_id, cluster in enumerate(clustered_dupes):
        id_set, confidence_score = cluster
        members = worker_session.query(*raw_cols).filter(pk_col.in_(id_set)).all()
        for member in members:
            keys = member.keys()
            keys.remove(pk_col.name)
            hash_me = ';'.join([preProcess(unicode(getattr(member, i))) for i in keys])
            md5_hash = md5(unidecode(hash_me)).hexdigest()
            m = {
                'group_id': cluster_id,
                'confidence': float(confidence_score),
                'record_id': getattr(member, pk_col.name),
                'clustered': False,
                'source_hash': md5_hash,
            }
            rows.append(m)
    engine.execute(dt.insert(), rows)

def writeBlockingMap(session_id, block_data):
    metadata = MetaData()
    bkm = block_map_table('block_%s' % session_id, metadata)
    engine = worker_session.bind
    bkm.create(engine)
    insert_data = []
    for key, record_id in block_data:
        insert_data.append({'block_key': key, 'record_id': record_id})
    engine.execute(bkm.insert(), insert_data)

def writeCanonTable(session_id):
    dd_sess = worker_session.query(DedupeSession).get(session_id)
    engine = worker_session.bind
    metadata = MetaData()
    raw_table = Table(dd_sess.table_name, metadata,
        autoload=True, autoload_with=engine)
    cols = []
    for col in raw_table.columns:
        if col.name != 'record_id':
            kwargs = {}
            if col.type == Integer:
                kwargs['default'] = 0
            if col.type == Float:
                kwargs['default'] = 0.0
            if col.type == Boolean:
                kwargs['default'] = None
            cols.append(Column(col.name, col.type, **kwargs))
    canon_table = Table('canon_%s' % session_id, metadata,
        *cols, extend_existing=True)
    canon_table.append_column(Column('canon_record_id', Integer, primary_key=True))
    canon_table.append_column(Column('entity_id', Integer, primary_key=True))
    canon_table.create(engine)
