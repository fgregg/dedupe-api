import os
import json
import dedupe
from cStringIO import StringIO
from hashlib import md5
from api.database import app_session, worker_session
from api.models import DedupeSession, entity_map, block_map_table, get_uuid
from api.utils.helpers import preProcess, slugify
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader
from sqlalchemy import MetaData, Table, Column, Integer, String, \
    create_engine, Float, Boolean, BigInteger, distinct, text, select, \
    Text, func, Index
from unidecode import unidecode
from uuid import uuid4
from csvkit.unicsv import UnicodeCSVWriter

def writeRawTable(filename=None,
              session_id=None,
              file_obj=None):
    """ 
    Create a table from incoming tabular data
    """
    fieldnames = file_obj.next().strip('\r\n').split(',')
    file_obj.seek(0)
    cols = []
    for field in fieldnames:
        cols.append(Column(slugify(unicode(field)), String))
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
    file_obj.seek(0)
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.copy_expert(copy_st, file_obj)
    conn.commit()
    writeProcessedTable(session_id)
    return fieldnames

def writeProcessedTable(session_id):
    engine = app_session.bind
    metadata = MetaData()
    raw_table = Table('raw_%s' % session_id, metadata, 
        autoload=True, autoload_with=engine)
    raw_fields = [f for f in raw_table.columns.keys() if f != 'record_id']
    create = 'CREATE TABLE "processed_%s" AS (SELECT record_id, ' % session_id
    for idx, field in enumerate(raw_fields):
        if idx < len(raw_fields) - 1:
            create += 'TRIM(COALESCE(LOWER("%s"), \'\')) AS %s, ' % (field, field)
        else:
            create += 'TRIM(COALESCE(LOWER("%s"), \'\')) AS %s ' % (field, field)
    else:
        create += 'FROM "raw_%s")' % session_id
    create_stmt = text(create)
    engine.execute(create_stmt)
    engine.execute('ALTER TABLE "processed_%s" ADD PRIMARY KEY (record_id)' % session_id)

def writeEntityMap(clustered_dupes, session_id):
    """ 
    Write entity map table the first time (after training)
    """
    engine = worker_session.bind
    metadata = MetaData()
    entity_table = entity_map('entity_%s' % session_id, metadata)
    entity_table.create(engine, checkfirst=True)
    with open('/tmp/%s.csv' % session_id, 'wb') as s:
        writer = UnicodeCSVWriter(s)
        for cluster, score in clustered_dupes:
            # leaving out low confidence clusters
            # This is a non-scientificly proven threshold
            if score > 0.2:
                entity_id = unicode(uuid4())
                for record_id in cluster:
                    writer.writerow([
                        record_id, 
                        entity_id, 
                        score, 
                        'raw_%s' % session_id,
                        'FALSE',
                        'FALSE',
                    ])
    conn = engine.raw_connection()
    cur = conn.cursor()
    with open('/tmp/%s.csv' % session_id, 'rb') as s:
        cur.copy_expert('''
            COPY "entity_%s" (
                record_id, 
                entity_id, 
                confidence,
                source,
                clustered,
                checked_out
            ) 
            FROM STDIN CSV''' % session_id, s)
    conn.commit()
    
    os.remove('/tmp/%s.csv' % session_id)

    dd = worker_session.query(DedupeSession).get(session_id)
    fields = [f['field'] for f in json.loads(dd.field_defs)]
    upd = 'UPDATE "entity_%s" SET source_hash=s.source_hash \
        FROM (SELECT MD5(CONCAT(' % session_id
    for idx, field in enumerate(fields):
        if idx < len(fields) - 1:
            upd += '%s,' % field
        else:
            upd += '%s))' % field
    else:
        upd += 'AS source_hash, record_id FROM "raw_%s") AS s \
            WHERE "entity_%s".record_id=s.record_id' % (session_id, session_id)
    engine.execute(upd)
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
    engine = worker_session.bind
    bkm = Table('block_%s' % session_id, metadata,
        Column('block_key', Text),
        Column('record_id', Integer)
    )
    bkm.create(engine, checkfirst=True)
    with open('/tmp/%s.csv' % session_id, 'wb') as s:
        writer = UnicodeCSVWriter(s)
        writer.writerows(block_data)
    conn = engine.raw_connection()
    cur = conn.cursor()
    with open('/tmp/%s.csv' % session_id, 'rb') as s:
        cur.copy_expert('COPY "block_%s" FROM STDIN CSV' % session_id, s)
    conn.commit()
    
    os.remove('/tmp/%s.csv' % session_id)

    block_key_idx = Index('bk_%s_idx' % session_id, bkm.c.block_key)
    block_key_idx.create(engine)

    plural_key = Table('plural_key_%s' % session_id, metadata,
        Column('block_key', Text),
        Column('block_id', Integer, primary_key=True)
    )
    plural_key.create(engine, checkfirst=True)
    bkm_sel = select([bkm.c.block_key], from_obj=bkm)\
        .group_by(bkm.c.block_key)\
        .having(func.count(bkm.c.block_key) > 1)
    pl_ins = plural_key.insert()\
        .from_select([plural_key.c.block_key], bkm_sel)
    engine.execute(pl_ins)
    
    pl_key_idx = Index('pk_%s_idx' % session_id, plural_key.c.block_key)
    pl_key_idx.create(engine)

    pl_bk_stmt = '''
        CREATE TABLE "plural_block_%s" AS (
            SELECT p.block_id, b.record_id 
                FROM "block_%s" AS b
                INNER JOIN "plural_key_%s" AS p
                USING (block_key)
            )''' % (session_id, session_id, session_id)
    engine.execute(pl_bk_stmt)
    engine.execute('''
        CREATE INDEX "pl_bk_idx_%s" 
        ON "plural_block_%s" (record_id)''' % (session_id, session_id)
    )
    engine.execute(''' 
        CREATE UNIQUE INDEX "pl_bk_id_idx_%s" on "plural_block_%s" 
        (block_id, record_id) ''' % (session_id, session_id)
    )

    cov_bks_stmt = ''' 
        CREATE TABLE "covered_%s" AS (
            SELECT record_id, 
            string_agg(CAST(block_id AS TEXT), ',' ORDER BY block_id) 
                AS sorted_ids
            FROM "plural_block_%s"
            GROUP BY record_id
        )
    ''' % (session_id, session_id)
    engine.execute(cov_bks_stmt)
    engine.execute(''' 
        CREATE UNIQUE INDEX "cov_bks_id_idx_%s" ON "covered_%s" (record_id)
        ''' % (session_id, session_id)
    )

    small_cov = ''' 
        CREATE TABLE "small_cov_%s" AS (
            SELECT record_id, 
                   block_id,
                   TRIM(',' FROM split_part(sorted_ids, CAST(block_id AS TEXT), 1))
                       AS smaller_ids
            FROM "plural_block_%s"
            INNER JOIN "covered_%s"
            USING (record_id)
        )
    ''' % (session_id, session_id, session_id)
    engine.execute(small_cov)
    engine.execute('''
        CREATE INDEX "sc_idx_%s" 
        ON "small_cov_%s" (record_id)''' % (session_id, session_id)
    )
    engine.execute('''
        CREATE INDEX "sc_bk_idx_%s" 
        ON "small_cov_%s" (block_id)''' % (session_id, session_id)
    )


