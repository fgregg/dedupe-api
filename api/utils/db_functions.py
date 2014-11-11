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
    engine = worker_session.bind
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
    engine = worker_session.bind
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

def initializeEntityMap(session_id, fields):
    engine = worker_session.bind
    metadata = MetaData()
    proc_table = Table('processed_%s' % session_id, metadata, 
        autoload=True, autoload_with=engine)
    gb_cols = [getattr(proc_table.c, f) for f in fields]
    rows = worker_session.query(func.array_agg(proc_table.c.record_id))\
        .group_by(*gb_cols)\
        .having(func.array_length(func.array_agg(proc_table.c.record_id),1) > 1)
    entity_table = entity_map('entity_%s' % session_id, metadata)
    entity_table.create(engine, checkfirst=True)
    s = StringIO()
    writer = UnicodeCSVWriter(s)
    for row in rows:
        king, members = row[0][0], row[0][1:]
        entity_id = unicode(uuid4())
        writer.writerow([
            king, 
            None, 
            entity_id, 
            100.0,
            'raw_%s' % session_id,
            'TRUE',
            'FALSE',
        ])
        for member in members:
            writer.writerow([
                member,
                king,
                entity_id,
                100.0,
                'raw_%s' % session_id,
                'TRUE',
                'FALSE',
            ])
    s.seek(0)
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.copy_expert('''
        COPY "entity_%s" (
            record_id, 
            target_record_id, 
            entity_id, 
            confidence,
            source,
            clustered,
            checked_out
        ) 
        FROM STDIN CSV''' % session_id, s)
    conn.commit()


def updateEntityMap(clustered_dupes, session_id, raw_table=None, entity_table=None):
    """ 
    Add to entity map table after training
    """
    engine = worker_session.bind
    metadata = MetaData()
    if not entity_table:
        entity_table = 'entity_{0}'.format(session_id)
    if not raw_table:
        raw_table = 'raw_{0}'.format(session_id)
    entity = Table(entity_table, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    for ids, score in clustered_dupes:
        # leaving out low confidence clusters
        # This is a non-scientificly proven threshold
        if score > 0.2:
            new_ent = unicode(uuid4())
            existing = worker_session.query(entity.c.record_id)\
                .filter(entity.c.record_id.in_(ids))\
                .all()
            if existing:
                existing_ids = [unicode(i[0]) for i in existing]
                new_ids = list(set(ids).difference(set(existing_ids)))
                upd = {
                    'entity_id': new_ent,
                    'clustered': False,
                    'confidence': float(score),
                }
                engine.execute(entity.update()\
                    .where(entity.c.record_id.in_(existing_ids))\
                    .values(**upd))
                if new_ids:
                    king = existing_ids[0]
                    vals = []
                    for i in new_ids:
                        d = {
                            'entity_id': new_ent,
                            'record_id': i,
                            'target_record_id': king,
                            'clustered': False,
                            'checked_out': False,
                            'confidence': float(score),
                        }
                        vals.append(d)
                    engine.execute(entity.insert(), vals)
            else:
                king = ids.pop(0)
                vals = [{
                    'entity_id': new_ent,
                    'record_id': king,
                    'target_record_id': None,
                    'clustered': False,
                    'checked_out': False,
                    'confidence': float(score),
                }]
                for i in ids:
                    d = {
                        'entity_id': new_ent,
                        'record_id': i,
                        'target_record_id': king,
                        'clustered': False,
                        'checked_out': False,
                        'confidence': score
                    }
                    vals.append(d)
                engine.execute(entity.insert(), vals)

    dd = worker_session.query(DedupeSession).get(session_id)
    fields = [f['field'] for f in json.loads(dd.field_defs)]
    upd = 'UPDATE "{0}" SET source_hash=s.source_hash \
        FROM (SELECT MD5(CONCAT('.format(entity_table)
    for idx, field in enumerate(fields):
        if idx < len(fields) - 1:
            upd += '{0},'.format(field)
        else:
            upd += '{0}))'.format(field)
    else:
        upd += 'AS source_hash, record_id FROM "{0}") AS s \
            WHERE "{0}".record_id=s.record_id'.format(raw_table, entity_table)
    engine.execute(upd)
    review_count = worker_session.query(distinct(entity.c.entity_id))\
        .filter(entity.c.clustered == False)\
        .count()
    return review_count

# def rewriteEntityMap(clustered_dupes, session_id, data_d):
#     sess = worker_session.query(DedupeSession).get(session_id)
#     field_defs = json.loads(sess.field_defs)
#     model_fields = [f['field'] for f in field_defs]
#     engine = worker_session.bind
#     metadata = MetaData()
#     entity_table = Table('entity_%s' % session_id, metadata,
#         autoload=True, autoload_with=engine)
#     canon_table = Table('canon_%s' % session_id, metadata,
#         autoload=True, autoload_with=engine)
#     raw_table = Table('raw_%s' % session_id, metadata,
#         autoload=True, autoload_with=engine)
#     for cluster in clustered_dupes:
#         id_set, confidence_score = cluster
#         members = worker_session.query(raw_table.c.record_id, entity_table.c.entity_id)\
#             .join(entity_table, raw_table.c.record_id == entity_table.c.record_id)\
#             .join(canon_table, entity_table.c.entity_id == canon_table.c.entity_id)\
#             .filter(canon_table.c.canon_record_id.in_(id_set))\
#             .all()
#         entity_id = members[0][1]
#         for member in members:
#             upd = entity_table.update()\
#                 .where(entity_table.c.record_id == member[0])\
#                 .values(entity_id=entity_id, clustered=False, 
#                     former_entity_id=member[1])
#             engine.execute(upd)
#     review_count = worker_session.query(entity_table.c.entity_id)\
#         .filter(entity_table.c.clustered == False)\
#         .count()
#     return review_count

def writeBlockingMap(session_id, block_data, canonical=False):
    pk_type = Integer
    if canonical:
        session_id = '{0}_cr'.format(session_id)
        pk_type = String
    metadata = MetaData()
    engine = worker_session.bind
    bkm = Table('block_{0}'.format(session_id), metadata,
        Column('block_key', Text),
        Column('record_id', pk_type)
    )
    bkm.create(engine, checkfirst=True)
    with open('/tmp/{0}.csv'.format(session_id), 'wb') as s:
        writer = UnicodeCSVWriter(s)
        writer.writerows(block_data)
    conn = engine.raw_connection()
    cur = conn.cursor()
    with open('/tmp/{0}.csv'.format(session_id), 'rb') as s:
        cur.copy_expert('COPY "block_{0}" FROM STDIN CSV'.format(session_id), s)
    conn.commit()
    
    os.remove('/tmp/{0}.csv'.format(session_id))

    block_key_idx = Index('bk_{0}_idx'.format(session_id), bkm.c.block_key)
    block_key_idx.create(engine)

    plural_key = Table('plural_key_{0}'.format(session_id), metadata,
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
    
    pl_key_idx = Index('pk_{0}_idx'.format(session_id), plural_key.c.block_key)
    pl_key_idx.create(engine)

    pl_bk_stmt = '''
        CREATE TABLE "plural_block_{0}" AS (
            SELECT p.block_id, b.record_id 
                FROM "block_{0}" AS b
                INNER JOIN "plural_key_{0}" AS p
                USING (block_key)
            )'''.format(session_id)
    engine.execute(pl_bk_stmt)
    engine.execute('''
        CREATE INDEX "pl_bk_idx_{0}" 
        ON "plural_block_{0}" (record_id)'''.format(session_id)
    )
    engine.execute(''' 
        CREATE UNIQUE INDEX "pl_bk_id_idx_{0}" on "plural_block_{0}" 
        (block_id, record_id) '''.format(session_id)
    )

    cov_bks_stmt = ''' 
        CREATE TABLE "covered_{0}" AS (
            SELECT record_id, 
            string_agg(CAST(block_id AS TEXT), ',' ORDER BY block_id) 
                AS sorted_ids
            FROM "plural_block_{0}"
            GROUP BY record_id
        )
    '''.format(session_id)
    engine.execute(cov_bks_stmt)
    engine.execute(''' 
        CREATE UNIQUE INDEX "cov_bks_id_idx_{0}" ON "covered_{0}" (record_id)
        '''.format(session_id)
    )

    small_cov = ''' 
        CREATE TABLE "small_cov_{0}" AS (
            SELECT record_id, 
                   block_id,
                   TRIM(',' FROM split_part(sorted_ids, CAST(block_id AS TEXT), 1))
                       AS smaller_ids
            FROM "plural_block_{0}"
            INNER JOIN "covered_{0}"
            USING (record_id)
        )
    '''.format(session_id)
    engine.execute(small_cov)
    engine.execute('''
        CREATE INDEX "sc_idx_{0}" 
        ON "small_cov_{0}" (record_id)'''.format(session_id)
    )
    engine.execute('''
        CREATE INDEX "sc_bk_idx_{0}" 
        ON "small_cov_{0}" (block_id)'''.format(session_id)
    )


