import os
import json
import dedupe
from cStringIO import StringIO
from hashlib import md5
from api.database import app_session, worker_session
from api.models import DedupeSession, entity_map, block_map_table, get_uuid
from api.utils.helpers import preProcess, slugify
from api.app_config import TIME_ZONE
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader
from sqlalchemy import MetaData, Table, Column, Integer, String, \
    create_engine, Float, Boolean, BigInteger, distinct, text, select, \
    Text, func, Index
from sqlalchemy.sql import label
from sqlalchemy.exc import ProgrammingError
from unidecode import unidecode
from uuid import uuid4
from csvkit.unicsv import UnicodeCSVWriter
from datetime import datetime

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

def writeProcessedTable(session_id, 
                        raw_table_format='raw_{0}', 
                        proc_table_format='processed_{0}'):

    engine = worker_session.bind
    metadata = MetaData()
    proc_table_name = proc_table_format.format(session_id)
    raw_table_name = raw_table_format.format(session_id)
    raw_table = Table(raw_table_name, metadata, 
        autoload=True, autoload_with=engine)
    raw_fields = [f for f in raw_table.columns.keys() if f != 'record_id']
    create = 'CREATE TABLE "{0}" AS (SELECT record_id, '.format(proc_table_name)
    for idx, field in enumerate(raw_fields):
        if idx < len(raw_fields) - 1:
            create += 'TRIM(COALESCE(LOWER("{0}"), \'\')) AS {0}, '.format(field)
        else:
            create += 'TRIM(COALESCE(LOWER("{0}"), \'\')) AS {0} '.format(field)
    else:
        create += 'FROM "{0}")'.format(raw_table_name)
    create_stmt = text(create)
    with engine.begin() as c:
        c.execute('DROP TABLE IF EXISTS "{0}"'.format(proc_table_name))
    with engine.begin() as c:
        c.execute(create_stmt)
    with engine.begin() as c:
        c.execute('ALTER TABLE "{0}" ADD PRIMARY KEY (record_id)'.format(proc_table_name))

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
    entity_table.drop(engine, checkfirst=True)
    entity_table.create(engine)
    s = StringIO()
    writer = UnicodeCSVWriter(s)
    for row in rows:
        king, members = row[0][0], row[0][1:]
        entity_id = unicode(uuid4())
        writer.writerow([
            king, 
            None, 
            entity_id, 
            1.0,
            'raw_{0}'.format(session_id),
            'TRUE',
            'FALSE',
            'exact',
        ])
        for member in members:
            writer.writerow([
                member,
                king,
                entity_id,
                1.0,
                'raw_{0}'.format(session_id),
                'TRUE',
                'FALSE',
                'exact',
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
            checked_out,
            match_type
        ) 
        FROM STDIN CSV''' % session_id, s)
    conn.commit()

def updateEntityMap(clustered_dupes,
                    session_id,
                    raw_table=None,
                    entity_table=None):
    
    """ 
    Add to entity map table after training
    """
    fname = '/tmp/clusters_{0}.csv'.format(session_id)
    with open(fname, 'wb') as f:
        writer = UnicodeCSVWriter(f)
        for ids, scores in clustered_dupes:
            new_ent = unicode(uuid4())
            writer.writerow([
                new_ent,
                ids[0],
                scores[0],
                None,
            ])
            for id, score in zip(ids[1:], scores[1:]):
                writer.writerow([
                    new_ent,
                    id,
                    score,
                    ids[0],
                ])
    engine = worker_session.bind
    metadata = MetaData()
    if not entity_table:
        entity_table = 'entity_{0}'.format(session_id)
    entity = Table(entity_table, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    record_id_type = entity.c.record_id.type
    temp_table = Table('temp_{0}'.format(session_id), metadata,
                       Column('entity_id', String),
                       Column('record_id', record_id_type),
                       Column('target_record_id', record_id_type),
                       Column('confidence', Float))
    temp_table.drop(bind=engine, checkfirst=True)
    temp_table.create(bind=engine)
    with open(fname, 'rb') as f:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(''' 
            COPY "temp_{0}" (
                entity_id,
                record_id,
                confidence,
                target_record_id
            ) 
            FROM STDIN CSV'''.format(session_id), f)
        conn.commit()

    upd = text(''' 
        WITH upd AS (
          UPDATE "{0}" AS e 
            SET entity_id = temp.entity_id, 
              confidence = temp.confidence, 
              clustered = FALSE,
              checked_out = FALSE,
              last_update = :last_update,
              target_record_id = temp.target_record_id
            FROM "temp_{1}" temp 
          WHERE e.record_id = temp.record_id 
          RETURNING temp.record_id
        ) 
        INSERT INTO "{0}" (record_id, entity_id, confidence, clustered, checked_out, target_record_id) 
          SELECT 
            record_id, 
            entity_id, 
            confidence, 
            FALSE AS clustered, 
            FALSE AS checked_out,
            target_record_id
          FROM "temp_{1}" temp 
          LEFT JOIN upd USING(record_id) 
          WHERE upd.record_id IS NULL
          RETURNING record_id
    '''.format(entity_table, session_id))
    with engine.begin() as c:
        ids = c.execute(upd, last_update=datetime.now().replace(tzinfo=TIME_ZONE))
    temp_table.drop(bind=engine)
    os.remove(fname)

def writeCanonRep(session_id, name_pattern='cr_{0}'):
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
    cr = Table(name_pattern.format(session_id), metadata, *cr_cols)
    cr.drop(bind=engine, checkfirst=True)
    cr.create(bind=engine)

    cols = [entity.c.entity_id]
    col_names = [c for c in proc_table.columns.keys() if c != 'record_id']
    for name in col_names:
        cols.append(label(name, func.array_agg(getattr(proc_table.c, name))))
    rows = worker_session.query(*cols)\
        .filter(entity.c.record_id == proc_table.c.record_id)\
        .group_by(entity.c.entity_id)
    names = cr.columns.keys()
    with open('/tmp/{0}.csv'.format(name_pattern.format(session_id)), 'wb') as f:
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
    canon_table_name = name_pattern.format(session_id)
    copy_st = 'COPY "{0}" ('.format(canon_table_name)
    for idx, name in enumerate(names):
        if idx < len(names) - 1:
            copy_st += '"{0}", '.format(name)
        else:
            copy_st += '"{0}")'.format(name)
    else:
        copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL ' ')"
    conn = engine.raw_connection()
    cur = conn.cursor()
    with open('/tmp/{0}.csv'.format(name_pattern.format(session_id)), 'rb') as f:
        cur.copy_expert(copy_st, f)
    conn.commit()

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
    bkm.drop(engine, checkfirst=True)
    bkm.create(engine)
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
    plural_key.drop(engine, checkfirst=True)
    plural_key.create(engine)
    bkm_sel = select([bkm.c.block_key], from_obj=bkm)\
        .group_by(bkm.c.block_key)\
        .having(func.count(bkm.c.block_key) > 1)
    pl_ins = plural_key.insert()\
        .from_select([plural_key.c.block_key], bkm_sel)
    with engine.begin() as c:
        c.execute(pl_ins)
    
    pl_key_idx = Index('pk_{0}_idx'.format(session_id), plural_key.c.block_key)
    pl_key_idx.create(engine)

    with engine.begin() as c:
        c.execute('DROP TABLE IF EXISTS "plural_block_{0}"'.format(session_id))
    pl_bk_stmt = '''
        CREATE TABLE "plural_block_{0}" AS (
            SELECT p.block_id, b.record_id 
                FROM "block_{0}" AS b
                INNER JOIN "plural_key_{0}" AS p
                USING (block_key)
            )'''.format(session_id)
    with engine.begin() as c:
        c.execute(pl_bk_stmt)
    with engine.begin() as c:
        c.execute('''
            CREATE INDEX "pl_bk_idx_{0}" 
            ON "plural_block_{0}" (record_id)'''.format(session_id)
        )
    with engine.begin() as c:
        c.execute('DROP INDEX IF EXISTS "pl_bk_id_idx_{0}"'.format(session_id))
    with engine.begin() as c:
        c.execute(''' 
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
    with engine.begin() as c:
        c.execute('DROP TABLE IF EXISTS "covered_{0}"'.format(session_id))
    with engine.begin() as c:
        c.execute(cov_bks_stmt)
    with engine.begin() as c:
        c.execute(''' 
            CREATE UNIQUE INDEX "cov_bks_id_idx_{0}" ON "covered_{0}" (record_id)
            '''.format(session_id)
        )

    with engine.begin() as c:
        c.execute('DROP TABLE IF EXISTS "small_cov_{0}"'.format(session_id))
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
    with engine.begin() as c:
        c.execute(small_cov)
    with engine.begin() as c:
        c.execute('''
            CREATE INDEX "sc_idx_{0}" 
            ON "small_cov_{0}" (record_id)'''.format(session_id)
        )
    with engine.begin() as c:
        c.execute('''
            CREATE INDEX "sc_bk_idx_{0}" 
            ON "small_cov_{0}" (block_id)'''.format(session_id)
        )


