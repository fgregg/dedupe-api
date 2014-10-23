import json
import dedupe
from cStringIO import StringIO
from hashlib import md5
from api.database import app_session, worker_session
from api.models import DedupeSession, entity_map, block_map_table, get_uuid
from api.utils.helpers import preProcess
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader
from sqlalchemy import MetaData, Table, Column, Integer, String, \
    create_engine, Float, Boolean, BigInteger, distinct
from unidecode import unidecode
from uuid import uuid4

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
    sql_table.append_column(Column('record_id', BigInteger, primary_key=True))
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
    Write entity map table the first time (after training)
    """
    engine = worker_session.bind
    metadata = MetaData()
    entity_table = entity_map('entity_%s' % session_id, metadata)
    entity_table.create(engine, checkfirst=True)
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs)
    model_fields = [f['field'] for f in field_defs]
    raw_table = Table('raw_%s' % session_id, metadata, 
        autoload=True, autoload_with=engine)
    pk_col = [p for p in raw_table.primary_key][0]
    makeCanonTable(session_id)
    for cluster in clustered_dupes:
        id_set, confidence_score = cluster
        # leaving out low confidence clusters
        # This is a non-scientificly proven threshold
        if confidence_score > 0.2:
            members = worker_session.query(raw_table).filter(pk_col.in_(id_set)).all()
            entity_id = unicode(uuid4())
            for member in members:
                hash_me = ';'.join([preProcess(unicode(getattr(member, i))) for i in model_fields])
                md5_hash = md5(unidecode(hash_me)).hexdigest()
                m = {
                    'confidence': float(confidence_score),
                    'record_id': member.record_id,
                    'source_hash': md5_hash,
                    'entity_id': entity_id,
                    'source': 'raw_%s' % session_id,
                    'checked_out': False,
                    'clustered': False,
                }
                # auto accepting clsuters with higher confidence.
                # Also a non-scientificly proven threshold
                #if confidence_score >= 0.28:
                #    m['clustered'] = True
                #else:
                #    m['clustered'] = False
                engine.execute(entity_table.insert(), m)
            canonicalizeEntity(session_id, entity_id)
    review_count = worker_session.query(distinct(entity_table.c.entity_id))\
        .filter(entity_table.c.clustered == False)\
        .count()
    return review_count

def rewriteEntityMap(clustered_dupes, session_id, data_d):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs)
    model_fields = [f['field'] for f in field_defs]
    engine = worker_session.bind
    metadata = MetaData()
    entity_table = Table('entity_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    canon_table = Table('canon_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    raw_table = Table('raw_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    for cluster in clustered_dupes:
        id_set, confidence_score = cluster
        members = worker_session.query(raw_table.c.record_id, entity_table.c.entity_id)\
            .join(entity_table, raw_table.c.record_id == entity_table.c.record_id)\
            .join(canon_table, entity_table.c.entity_id == canon_table.c.entity_id)\
            .filter(canon_table.c.canon_record_id.in_(id_set))\
            .all()
        entity_id = members[0][1]
        for member in members:
            upd = entity_table.update()\
                .where(entity_table.c.record_id == member[0])\
                .values(entity_id=entity_id, clustered=False, 
                    former_entity_id=member[1])
            engine.execute(upd)
    review_count = worker_session.query(entity_table.c.entity_id)\
        .filter(entity_table.c.clustered == False)\
        .count()
    return review_count


def canonicalizeEntity(session_id, entity_id):
    engine = worker_session.bind
    metadata = MetaData()
    entity_table = Table('entity_%s' % session_id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    raw_table = Table('raw_%s' % session_id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    raw_rows = worker_session.query(raw_table, entity_table.c.canon_record_id)\
        .join(entity_table, raw_table.c.record_id == entity_table.c.record_id)\
        .filter(entity_table.c.entity_id == entity_id)\
        .all()
    raw_fields = [c for c in raw_table.columns.keys()]
    rows_d = []
    for row in raw_rows:
        d = {}
        for k,v in zip(raw_fields, row[:-1]):
            if k != 'record_id':
                d[k] = preProcess(unicode(v))
        rows_d.append(d)
    canonical_form = dedupe.canonicalize(rows_d)
    canonical_form['entity_id'] = entity_id
    canon_table = Table('canon_%s' % session_id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    canon_row = engine.execute(canon_table.insert(), canonical_form)
    upd = entity_table.update()\
        .where(entity_table.c.entity_id == entity_id)\
        .values(canon_record_id=canon_row.inserted_primary_key[0])
    engine.execute(upd)

def makeCanonTable(session_id):
    engine = worker_session.bind
    metadata = MetaData()
    raw_table = Table('raw_%s' % session_id, metadata,
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
    canon_table.append_column(Column('entity_id', String))
    canon_table.create(engine, checkfirst=True)

def writeBlockingMap(session_id, block_data):
    metadata = MetaData()
    bkm = block_map_table('block_%s' % session_id, metadata)
    engine = worker_session.bind
    bkm.create(engine)
    insert_data = []
    for key, record_id in block_data:
        insert_data.append({'block_key': key, 'record_id': record_id})
    engine.execute(bkm.insert(), insert_data)

