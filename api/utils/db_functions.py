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
from operator import itemgetter
from itertools import groupby

try: # pragma: no cover
    from raven import Client as Sentry
    from api.app_config import SENTRY_DSN
    sentry = Sentry(dsn=SENTRY_DSN) 
except ImportError:
    sentry = None
except KeyError: #pragma: no cover
    sentry = None

def addRowHash(session_id):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs)
    fields = sorted(list(set([f['field'] for f in field_defs])))
    engine = worker_session.bind
    fields = ["COALESCE(r.{0}, '')".format(f) for f in fields]
    fields = " || ';' || ".join(fields)
    upd = ''' 
      UPDATE "entity_{0}" SET
        source_hash=s.source_hash 
        FROM (
          SELECT 
            MD5({1}) as source_hash,
            r.record_id
          FROM "entity_{0}" as e
          JOIN "raw_{0}" as r
            ON e.record_id = r.record_id
        ) AS s
        WHERE "entity_{0}".record_id = s.record_id
    '''.format(session_id, fields)
    with engine.begin() as conn:
        conn.execute(upd)

def writeRawTable(session_id=None,
              file_path=None):
    """ 
    Create a table from incoming tabular data
    """
    file_obj = open(file_path, 'rb')
    fieldnames = [slugify(unicode(f)) for f in file_obj.next().strip('\r\n').split(',')]
    file_obj.seek(0)
    cols = []
    for field in fieldnames:
        cols.append(Column(field, String))
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
    try:
        cur.copy_expert(copy_st, file_obj)
        conn.commit()
        os.remove(file_path)
    except Exception, e:
        print e
        conn.rollback()
        raise e
    return fieldnames

def writeProcessedTable(session_id, 
                        raw_table_format='raw_{0}', 
                        proc_table_format='processed_{0}'):
    dd = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(dd.field_defs)
    fds = {}
    for fd in field_defs:
        try:
            fds[fd['field']].append(fd['type'])
        except KeyError:
            fds[fd['field']] = [fd['type']]
    engine = worker_session.bind
    metadata = MetaData()
    proc_table_name = proc_table_format.format(session_id)
    raw_table_name = raw_table_format.format(session_id)
    raw_table = Table(raw_table_name, metadata, 
        autoload=True, autoload_with=engine)
    raw_fields = [f for f in raw_table.columns.keys() if f != 'record_id']
    create = 'CREATE TABLE "{0}" AS (SELECT record_id, '.format(proc_table_name)
    for idx, field in enumerate(raw_fields):
        try:
            field_types = fds[field]
        except KeyError:
            field_types = ['String']
        # TODO: Need to figure out how to parse a LatLong field type
        if 'Price' in field_types:
            col_def = 'COALESCE(CAST("{0}" AS DOUBLE PRECISION), 0.0) AS {0}'.format(field)
        else:
            col_def = 'CAST(TRIM(COALESCE(LOWER("{0}"), \'\')) AS VARCHAR) AS {0}'.format(field)
        if idx < len(raw_fields) - 1:
            create += '{0}, '.format(col_def)
        else:
            create += '{0} '.format(col_def)
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
    create = '''
        CREATE TABLE "exact_match_{0}" AS (
          SELECT 
            s.record_id,
            UNNEST(s.members) as match
          FROM (
            SELECT 
              MIN(record_id) AS record_id, 
              (array_agg(record_id ORDER BY record_id))
                [2:array_upper(array_agg(record_id), 1)] AS members
            FROM "processed_{0}" 
            GROUP BY {1} 
            HAVING (array_length(array_agg(record_id), 1) > 1)
          ) AS s
        )
        '''.format(session_id, ', '.join(fields))
    with engine.begin() as conn:
        conn.execute('DROP TABLE IF EXISTS "exact_match_{0}"'.format(session_id))
        conn.execute(create)
    exact_table = Table('exact_match_{0}'.format(session_id), metadata,
                  autoload=True, autoload_with=engine, keep_existing=True)
    rows = worker_session.query(exact_table)
    entity_table = entity_map('entity_%s' % session_id, metadata)
    entity_table.drop(engine, checkfirst=True)
    entity_table.create(engine)
    s = StringIO()
    writer = UnicodeCSVWriter(s)
    now = datetime.now().replace(tzinfo=TIME_ZONE).isoformat()
    rows = sorted(rows, key=itemgetter(0))
    grouped = {}
    for k, g in groupby(rows, key=itemgetter(0)):
        rs = [r[1] for r in g]
        grouped[k] = rs
    for king,serfs in grouped.items():
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
            now,
        ])
        for serf in serfs:
            writer.writerow([
                serf,
                king,
                entity_id,
                1.0,
                'raw_{0}'.format(session_id),
                'TRUE',
                'FALSE',
                'exact',
                now,
            ])
    s.seek(0)
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.copy_expert('''
        COPY "entity_{0}" (
            record_id, 
            target_record_id, 
            entity_id, 
            confidence,
            source,
            clustered,
            checked_out,
            match_type,
            last_update
        ) 
        FROM STDIN CSV'''.format(session_id), s)
    conn.commit()

def addToEntityMap(session_id, new_entity, match_ids=None, reviewer=None):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs)
    fds = {}
    for fd in field_defs:
        try:
            fds[fd['field']].append(fd['type'])
        except KeyError:
            fds[fd['field']] = [fd['type']]
    else:
        engine = worker_session.bind
        sel = text(''' 
            SELECT * from "processed_{0}" 
            WHERE record_id = :record_id 
            LIMIT 1
        '''.format(session_id))
        row = engine.execute(sel, record_id=new_entity['record_id'])

        # If this is an entirely new record, we need to add it to the processed table
        if not row: # pragma: no cover
            raw_table = Table('raw_{0}'.format(session_id), Base.metadata, 
                autoload=True, autoload_with=engine, keep_existing=True)
            proc_ins = 'INSERT INTO "processed_{0}" (SELECT record_id, '\
                .format(proc_table_name)
            for idx, field in enumerate(fds.keys()):
                try:
                    field_types = fds[field]
                except KeyError:
                    field_types = ['String']
                # TODO: Need to figure out how to parse a LatLong field type
                if 'Price' in field_types:
                    col_def = 'COALESCE(CAST("{0}" AS DOUBLE PRECISION), 0.0) AS {0}'.format(field)
                else:
                    col_def = 'CAST(TRIM(COALESCE(LOWER("{0}"), \'\')) AS VARCHAR) AS {0}'.format(field)
                if idx < len(fds.keys()) - 1:
                    proc_ins += '{0}, '.format(col_def)
                else:
                    proc_ins += '{0} '.format(col_def)
            else:
                proc_ins += 'FROM "raw_{0}" WHERE record_id = :record_id)'\
                    .format(session_id)

            with engine.begin() as conn:
                record_id = conn.execute(raw_table.insert()\
                    .returning(raw_table.c.record_id) , **new_entity)
                conn.execute(text(proc_ins), record_id=record_id)

        # Add to entity map
        hash_me = ';'.join([preProcess(unicode(new_entity[i])) for i in fds.keys()])
        md5_hash = md5(unidecode(hash_me)).hexdigest()
        last_update = datetime.now().replace(tzinfo=TIME_ZONE)
        entity = {
            'entity_id': unicode(uuid4()),
            'record_id': new_entity['record_id'],
            'source_hash': md5_hash,
            'clustered': True,
            'checked_out': False,
            'last_update': last_update,
            'match_type': 'match'
        }
        if match_ids:
            entity['target_record_id'] = match_ids[0]
            sel = text(''' 
                SELECT entity_id
                FROM "entity_{0}"
                WHERE record_id = :record_id
                LIMIT 1
            '''.format(session_id))
            entity_id = list(engine.execute(sel, record_id=match_ids[0]))[0].entity_id
            entity['entity_id'] = entity_id
            if len(match_ids) > 1:
                upd_args = {
                    'entity_id': entity_id,
                    'clustered': True,
                    'checked_out': False,
                    'last_update': last_update,
                    'reviewer': reviewer,
                    'match_ids': tuple([m for m in match_ids]),
                    'match_type': 'merge from match'
                }
                upd = text('''
                    UPDATE "entity_{0}" SET 
                        entity_id = :entity_id,
                        clustered = :clustered,
                        checked_out = :checked_out,
                        last_update = :last_update,
                        reviewer = :reviewer,
                        match_type = :match_type
                    WHERE entity_id IN (
                        SELECT entity_id
                        FROM "entity_{0}"
                        WHERE record_id IN :match_ids
                    )
                    '''.format(session_id))
                with engine.begin() as conn:
                    conn.execute(upd, **upd_args)
        ins = text(''' 
            INSERT INTO "entity_{0}" ({1}) VALUES ({2})
        '''.format(session_id, 
                   ','.join(entity.keys()), 
                   ','.join([':{0}'.format(f) for f in entity.keys()])))
        with engine.begin() as conn:
            conn.execute(ins, **entity)

        # Update block table
        deduper = dedupe.StaticGazetteer(StringIO(sess.gaz_settings_file))
        field_types = {}
        for field in field_defs:
            if field_types.get(field['field']):
                field_types[field['field']].append(field['type'])
            else:
                field_types[field['field']] = [field['type']]
        for k,v in new_entity.items():
            if field_types.get(k):
                if 'Price' in field_types[k]:
                    if v:
                        new_entity[k] = float(v)
                    else:
                        new_entity[k] = 0
                else:
                    new_entity[k] = preProcess(unicode(v))
        block_keys = [{'record_id': b[1], 'block_key': b[0]} \
                for b in list(deduper.blocker([(new_entity['record_id'], new_entity)]))]
        if block_keys:
            with engine.begin() as conn:
                conn.execute(text(''' 
                    INSERT INTO "match_blocks_{0}" (
                        block_key,
                        record_id
                    ) VALUES (:block_key, :record_id)
                '''.format(sess.id)), *block_keys)
        else:
            if sentry:
                sentry.captureMessage('Unable to block record', extra=new_entity)

        # Update match_review table
        upd = ''' 
            UPDATE "match_review_{0}" SET
              reviewed = TRUE,
              reviewer = :reviewer
            WHERE record_id = :record_id
        '''.format(session_id)
        with engine.begin() as conn:
            conn.execute(text(upd), 
                         record_id=new_entity['record_id'], 
                         reviewer=reviewer)

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
        UPDATE "{0}" 
          SET entity_id = temp.entity_id, 
            confidence = temp.confidence, 
            clustered = FALSE,
            checked_out = FALSE,
            last_update = :last_update,
            target_record_id = temp.target_record_id
          FROM "temp_{1}" temp 
        WHERE "{0}".record_id = temp.record_id 
    '''.format(entity_table, session_id))
    ins = text('''
        INSERT INTO "{0}" (record_id, entity_id, confidence, clustered, checked_out, target_record_id) 
          SELECT 
            record_id, 
            entity_id, 
            confidence, 
            FALSE AS clustered, 
            FALSE AS checked_out,
            target_record_id
          FROM "temp_{1}" temp 
          LEFT JOIN (
            SELECT record_id 
            FROM "{0}"
            WHERE last_update = :last_update
          ) AS s USING(record_id) 
          WHERE s.record_id IS NULL
          RETURNING record_id
    '''.format(entity_table, session_id))
    last_update = datetime.now().replace(tzinfo=TIME_ZONE)
    with engine.begin() as c:
        c.execute(upd, last_update=last_update)
        c.execute(ins, last_update=last_update)
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
                    dicts[idx][name] = unicode(getattr(row, name)[idx])
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
    
    with engine.begin() as conn:
        conn.execute('DROP TABLE IF EXISTS "plural_key_{0}"'.format(session_id))

    create = ''' 
        CREATE TABLE "plural_key_{0}" (
          block_key VARCHAR,
          block_id SERIAL PRIMARY KEY
        ) 
    '''.format(session_id) 
    insert = '''
        INSERT INTO "plural_key_{0}" ( 
          SELECT 
            MAX(b.block_key) as block_key 
          FROM (
            SELECT 
              block_key,
              string_agg(record_id::text, ',' ORDER BY record_id) AS block
            FROM "block_{0}"
            GROUP BY block_key HAVING COUNT(*) > 1
          ) AS b
          GROUP BY b.block
        )
    '''.format(session_id)
    idx = ''' 
        CREATE INDEX "pk_{0}_idx" ON "plural_key_{0}" (block_key) 
    '''.format(session_id)
    with engine.begin() as conn:
        conn.execute(create)
        conn.execute(insert)
        conn.execute(idx)

    with engine.begin() as c:
        c.execute('DROP TABLE IF EXISTS "plural_block_{0}"'.format(session_id))
    pl_bk_stmt = '''
        CREATE TABLE "plural_block_{0}" AS (
            SELECT p.block_id, b.record_id 
                FROM "block_{0}" AS b
                JOIN "plural_key_{0}" AS p
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


