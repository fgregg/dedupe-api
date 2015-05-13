import os
import simplejson as json
import dedupe
from dedupe.serializer import _from_json, _to_json
import csv
from io import StringIO, BytesIO
from hashlib import md5
from api.database import app_session, worker_session
from api.models import DedupeSession, entity_map, block_map_table, get_uuid
from api.utils.helpers import preProcess, slugify, updateEntityCount, \
    RetrainGazetteer, getFieldsByType, readFieldDefs
from api.app_config import TIME_ZONE
from csvkit import convert
from sqlalchemy import MetaData, Table, Column, Integer, String, \
    create_engine, Float, Boolean, BigInteger, distinct, text, select, \
    Text, func, Index
from sqlalchemy.sql import label
from sqlalchemy.exc import ProgrammingError, IntegrityError, NoSuchTableError
from sqlalchemy.dialects.postgresql.base import ARRAY
from unidecode import unidecode
from uuid import uuid4
import csv
from datetime import datetime
from operator import itemgetter
import itertools

try: # pragma: no cover
    from raven import Client as Sentry
    from api.app_config import SENTRY_DSN
    sentry = Sentry(dsn=SENTRY_DSN) 
except ImportError:
    sentry = None
except KeyError: #pragma: no cover
    sentry = None


def updateTrainingFromCluster(session_id, 
                              distinct_ids=[], 
                              match_ids=[],
                              trainer=None):
    ''' 
    Update the sessions training data with the given record_ids
    '''
    all_ids = tuple(distinct_ids + match_ids)
    all_records = castRecords(session_id, all_ids)

    training = examplesFromCluster(distinct_ids,
                                   match_ids,
                                   all_records)

    saveTraining(session_id, training, trainer)


def examplesFromCluster(distinct_ids, match_ids, records) :
    training = {'distinct': [], 'match': []}

    if len(distinct_ids) == 1 :
        for id_pair in itertools.product(distinct_ids, match_ids) :
            training['distinct'].append(recordPair(records, 
                                                   id_pair))
    elif len(distinct_ids) == 2 and not match_ids :
        training['distinct'].append(recordPair(records,
                                               distinct_ids))

    for id_pair in itertools.combinations(match_ids, 2) :
        training['match'].append(recordPair(records,
                                            id_pair))

    return training

def updateTrainingFromMatch(session_id,
                            target_id,
                            distinct_ids=[], 
                            match_ids=[],
                            trainer=None):
    ''' 
    Update the sessions training from a match call
    '''
    ids = tuple(distinct_ids + match_ids + [target_id])
    records = castRecords(session_id, ids)

    training = {'distinct': [], 'match': []}

    for id_pair in itertools.product([target_id], distinct_ids) :
        training['distinct'].append(recordPair(records,
                                            id_pair))

    for id_pair in itertools.combinations(match_ids + [target_id], 2) :
        training['match'].append(recordPair(records,
                                               id_pair))
        

    saveTraining(session_id, training, trainer)

    

def recordPair(records, id_pair) :
    id_1, id_2 = id_pair
    return (records[int(id_1)], records[int(id_2)])

def castRecords(session_id, ids) :
    sess = worker_session.query(DedupeSession).get(session_id)
    worker_session.refresh(sess)
    engine = worker_session.bind
    
    raw_table = Table('raw_{0}'.format(session_id), 
                      MetaData(),
                      autoload=True, 
                      autoload_with=engine)
    raw_fields = [r.name for r in raw_table.columns]

    sel_clause = ', '.join(castFields(session_id, raw_fields))

    sel = text(''' 
    SELECT {1} FROM "processed_{0}" 
    WHERE record_id IN :record_ids
    '''.format(session_id, sel_clause))

    all_records = {r.record_id: dict(r)
                   for r 
                   in engine.execute(sel, record_ids=ids)}

    return all_records

    

def castFields(session_id, fields) :
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs.decode('utf-8'))
    fields_by_type = getFieldsByType(field_defs)

    sel_clauses = set()
    for field in fields:
        if fields_by_type.get(field):
            if 'Price' in fields_by_type[field]:
                sel_clauses.add('"{0}"::double precision'.format(field))
            else:
                sel_clauses.add('"{0}"'.format(field))
        else:
            sel_clauses.add('"{0}"'.format(field))

    return sel_clauses

def saveTraining(session_id, training_data, trainer):
    engine = worker_session.bind
    ins = ''' 
        WITH upsert AS (
          UPDATE dedupe_training_data SET
            pair_type = :pair_type,
            date_added = NOW()
          WHERE session_id = :session_id
            AND left_record = :left_record
            AND right_record = :right_record
          RETURNING *
        )
        INSERT INTO dedupe_training_data (
          trainer, 
          left_record,
          right_record,
          pair_type, 
          session_id
        ) SELECT 
            :trainer,
            :left_record,
            :right_record,
            :pair_type,
            :session_id
          WHERE NOT EXISTS (SELECT * FROM upsert)
    '''
    
    for pair_type, pairs in training_data.items():
        for left, right in pairs:
            left = json.dumps(left, default=_to_json, tuple_as_array=False)
            right = json.dumps(right, default=_to_json, tuple_as_array=False)
            row = {
                'trainer': trainer,
                'left_record': left,
                'right_record': right,
                'pair_type': pair_type,
                'session_id': session_id
            }
            row_ins = ins.format(**row)
            with engine.begin() as conn:
                conn.execute(text(row_ins), **row)

def migrateTraining(session_id):
    engine = worker_session.bind
    sel = ''' 
        SELECT training_data
        FROM dedupe_session
        WHERE id = :session_id
    '''
    td = engine.execute(text(sel), session_id=session_id).first()
    if td.training_data:
        training_data = json.loads(td.training_data.tobytes().decode('utf-8'))
        saveTraining(session_id, training_data, 'migrated')
        with engine.begin() as conn:
            conn.execute(text('''
                UPDATE dedupe_session SET 
                  training_data = NULL 
                WHERE id = :session_id
                '''), session_id=session_id)

def readTraining(session_id):
    engine = worker_session.bind
    
    training = {'distinct': [], 'match': []}
    
    migrateTraining(session_id)

    for pair_type in ['match', 'distinct']:
        sel = ''' 
          SELECT 
            json_agg(pairs) AS pairs
          FROM (
            SELECT 
              json_build_array(left_record, right_record) AS pair
            FROM dedupe_training_data 
            WHERE pair_type = :pair_type
              AND session_id = :session_id
            ORDER BY date_added
            LIMIT 300
          ) AS pairs
        '''
        pairs = engine.execute(text(sel), 
                            pair_type=pair_type, 
                            session_id=session_id).first()
        if pairs.pairs:
            for pair in pairs.pairs:
                for idx, record in enumerate(pair['pair']):
                    pair['pair'][idx] = record
                training[pair_type].append(pair['pair'])
    
    return training

def addRowHash(session_id):
    sess = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(sess.field_defs.decode('utf-8'))
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
              file_path=None,
              fieldnames=None):
    """ 
    Create a table from incoming tabular data
    """
    cols = []
    for field in fieldnames:
        if field == 'record_id':
            field = 'user_record_id'
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
    conn = engine.raw_connection()
    cur = conn.cursor()
    file_obj = open(file_path, 'r', encoding='utf-8')
    try:
        cur.copy_expert(copy_st, file_obj)
        conn.commit()
        os.remove(file_path)
    except (ProgrammingError, IntegrityError) as e:
        conn.rollback()
        raise e
    return fieldnames

def writeProcessedTable(session_id, 
                        raw_table_format='raw_{0}', 
                        proc_table_format='processed_{0}'):
    dd = worker_session.query(DedupeSession).get(session_id)
    field_defs = json.loads(dd.field_defs.decode('utf-8'))
    fds = getFieldsByType(field_defs)
    
    engine = worker_session.bind
    metadata = MetaData()
    proc_table_name = proc_table_format.format(session_id)
    raw_table_name = raw_table_format.format(session_id)
    while True:
        try:
            raw_table = Table(raw_table_name, metadata, 
                autoload=True, autoload_with=engine)
            break
        except NoSuchTableError:
            continue
    raw_fields = [f for f in raw_table.columns.keys() if f != 'record_id']
    create = 'CREATE TABLE "{0}" AS (SELECT record_id, '.format(proc_table_name)
    for idx, field in enumerate(raw_fields):
        try:
            field_types = fds[field]
        except KeyError:
            field_types = ['String']
        # TODO: Need to figure out how to parse a LatLong field type
        if 'Price' in field_types:
            col_def = 'COALESCE("{0}"::DOUBLE PRECISION, 0.0) AS {0}'.format(field)
        elif 'Set' in field_types:
            col_def = 'COALESCE(string_to_array("{0}"::VARCHAR, \',\'), ARRAY[]::VARCHAR[]) AS {0}'.format(field)
        else:
            col_def = 'TRIM(COALESCE(LOWER("{0}"), \'\'))::VARCHAR AS {0}'.format(field)
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
    
    entity_table = entity_map('entity_%s' % session_id, metadata)
    with engine.begin() as conn:
        conn.execute('DROP TABLE IF EXISTS "entity_{0}" CASCADE'.format(session_id))
    entity_table.create(engine)
    
    
    rows = worker_session.query(exact_table)
    rows = sorted(rows, key=itemgetter(0))
    
    s = StringIO()
    writer = csv.writer(s)
    now = datetime.now().replace(tzinfo=TIME_ZONE).isoformat()
    grouped = {}
    
    for k, g in itertools.groupby(rows, key=itemgetter(0)):
        rs = [r[1] for r in g]
        grouped[k] = rs
    
    for king,serfs in grouped.items():
        entity_id = str(uuid4())
        writer.writerow([
            int(king), 
            None, 
            entity_id, 
            1.0,
            'TRUE',
            'FALSE',
            'root_exact',
            now,
            'auto_exact'
        ])
        for serf in serfs:
            writer.writerow([
                int(serf),
                int(king),
                entity_id,
                1.0,
                'TRUE',
                'FALSE',
                'exact',
                now,
                'auto_exact'
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
            reviewed,
            checked_out,
            match_type,
            last_update,
            reviewer
        ) 
        FROM STDIN CSV'''.format(session_id), s)
    conn.commit()
    updateEntityCount(session_id)

def addToEntityMap(session_id, 
                   new_entity, 
                   match_ids=None, 
                   reviewer=None, 
                   block_keys=None):

    field_defs = readFieldDefs(session_id)
    fds = getFieldsByType(field_defs)

    engine = worker_session.bind
    sel = text(''' 
        SELECT 
          p.*,
          e.entity_id
        FROM "processed_{0}" AS p
        LEFT JOIN "entity_{0}" AS e
          ON p.record_id = e.record_id
        WHERE p.record_id = :record_id 
    '''.format(session_id))
    row = engine.execute(sel, record_id=new_entity['record_id']).first()
    if row.entity_id is None: # Record does not exist in entity map
        # If this is an entirely new record, we need to add it to the processed table
        if row is None: # pragma: no cover
            proc_ins = 'INSERT INTO "processed_{0}" (SELECT record_id, '\
                .format(proc_table_name)
            for idx, field in enumerate(fds.keys()):
                try:
                    field_types = fds[field]
                except KeyError:
                    field_types = ['String']
                # TODO: Need to figure out how to parse a LatLong field type
                if 'Price' in field_types:
                    col_def = 'COALESCE("{0}"::DOUBLE PRECISION, 0.0) AS {0}'.format(field)
                elif 'Set' in field_types:
                    col_def = 'COALESCE(string_to_array("{0}"::VARCHAR, \',\'), ARRAY[]::VARCHAR[]) AS {0}'.format(field)
                else:
                    col_def = 'TRIM(COALESCE(LOWER("{0}"), \'\')::VARCHAR) AS {0}'.format(field)
                if idx < len(fds.keys()) - 1:
                    proc_ins += '{0}, '.format(col_def)
                else:
                    proc_ins += '{0} '.format(col_def)
            else:
                proc_ins += 'FROM "raw_{0}" WHERE record_id = :record_id)'\
                    .format(session_id)
     
            meta = MetaData()
            raw_table = Table('raw_{0}'.format(session_id), meta, 
                autoload=True, autoload_with=engine)
            raw_fields = raw_table.columns.keys()
            with engine.begin() as conn:
                record_id = conn.execute(raw_table.insert()\
                    .returning(raw_table.c.record_id) , **new_entity)
                conn.execute(text(proc_ins), record_id=record_id)
     
        # Add to entity map
        hash_me = ';'.join([preProcess(str(new_entity[i]), ['String']) \
                for i in fds.keys()])
        md5_hash = md5(hash_me.encode('utf-8')).hexdigest()
        last_update = datetime.now().replace(tzinfo=TIME_ZONE)
        entity = {
            'entity_id': str(uuid4()),
            'record_id': new_entity['record_id'],
            'source_hash': md5_hash,
            'reviewed': True,
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
            entity_id = engine.execute(sel, record_id=match_ids[0])\
                        .first()\
                        .entity_id
            entity['entity_id'] = entity_id
            if len(match_ids) > 1:
                match_ids = tuple([m for m in match_ids])
                
                entity_ids = ''' 
                    SELECT entity_id
                    FROM "entity_{0}"
                    WHERE record_id IN :match_ids
                '''.format(session_id)

                entity_ids = tuple(r.entity_id for r in \
                             engine.execute(text(entity_ids), match_ids=match_ids))

                root_args = {
                    'entity_id': entity_id,
                    'reviewed': True,
                    'checked_out': False,
                    'last_update': last_update,
                    'reviewer': reviewer,
                    'entity_ids': entity_ids,
                    'match_type': 'merge from match'
                }
                
                update_roots = '''
                    UPDATE "entity_{0}" SET 
                        entity_id = :entity_id,
                        reviewed = :reviewed,
                        checked_out = :checked_out,
                        last_update = :last_update,
                        reviewer = :reviewer,
                        match_type = :match_type
                    WHERE entity_id IN :entity_ids
                      AND target_record_id IS NULL
                    '''.format(session_id)
                
                branch_args = {
                    'entity_id': entity_id,
                    'last_update': last_update,
                    'entity_ids': entity_ids,
                }
                update_branches = ''' 
                    UPDATE "entity_{0}" SET 
                        entity_id = :entity_id,
                        last_update = :last_update
                    WHERE entity_id IN :entity_ids
                      AND target_record_id IS NOT NULL
                '''.format(session_id)

                with engine.begin() as conn:
                    conn.execute(text(update_roots), **root_args)
                    conn.execute(text(update_branches), **branch_args)
        ins = text(''' 
            INSERT INTO "entity_{0}" ({1}) VALUES ({2})
        '''.format(session_id, 
                   ','.join(entity.keys()), 
                   ','.join([':{0}'.format(f) for f in entity.keys()])))
        with engine.begin() as conn:
            conn.execute(ins, **entity)
     
        # Update block table
        if block_keys:
            block_keys = [{'record_id': new_entity['record_id'], 'block_key': b} \
                          for b in block_keys]
        else:
            settings = ''' 
                SELECT gaz_settings_file AS sf
                FROM dedupe_session
                WHERE id = :session_id
            '''
            settings = engine.execute(text(settings), session_id=session_id).first().sf
            deduper = RetrainGazetteer(BytesIO(settings))
            field_types = {}
            for field in field_defs:
                if field_types.get(field['field']):
                    field_types[field['field']].append(field['type'])
                else:
                    field_types[field['field']] = [field['type']]
            for k,v in new_entity.items():
                if field_types.get(k):
                    new_entity[k] = preProcess(v, field_types[k])
            block_keys = [{'record_id': b[1], 'block_key': b[0]} \
                    for b in list(deduper.blocker([(new_entity['record_id'], new_entity)]))]
        if block_keys:
            with engine.begin() as conn:
                conn.execute(text(''' 
                    INSERT INTO "match_blocks_{0}" (
                        block_key,
                        record_id
                    ) VALUES (:block_key, :record_id)
                '''.format(session_id)), *block_keys)
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
    if not entity_table:
        entity_table = 'entity_{0}'.format(session_id)

    engine = worker_session.bind
    metadata = MetaData()

    entity = Table(entity_table, metadata,
                   autoload=True, autoload_with=engine, 
                   keep_existing=True)
    record_id_type = entity.c.record_id.type
    
    temp_table = Table('temp_{0}'.format(session_id), metadata,
                       Column('entity_id', String),
                       Column('record_id', record_id_type),
                       Column('confidence', Float))
    temp_table.drop(bind=engine, checkfirst=True)
    temp_table.create(bind=engine)

    rows = []

    for record_ids, scores in clustered_dupes:
        assert len(record_ids) > 1
        new_ent = str(uuid4())

        for record_id, score in zip(record_ids, scores) :

            # if numpy int64 cast to python int
            try:
                record_id = int(record_id)
            except ValueError:
                record_id = str(record_id)

            rows.append({'entity_id': new_ent,
                         'record_id': record_id,
                         'confidence': float(score)})

            if len(rows) % 50000 == 0:
                with engine.begin() as conn:
                    conn.execute(temp_table.insert(), rows)

                rows = []

    if rows:
        with engine.begin() as conn:
            conn.execute(temp_table.insert(), rows)

    upd = text(''' 
        UPDATE "{0}" 
          SET entity_id = temp.entity_id, 
            confidence = temp.confidence, 
            reviewed = FALSE,
            checked_out = FALSE,
            last_update = :last_update
          FROM "temp_{1}" temp 
        WHERE "{0}".record_id = temp.record_id 
    '''.format(entity_table, session_id))

    # http://stackoverflow.com/questions/2686254/how-to-select-all-records-from-one-table-that-do-not-exist-in-another-table
    ins = text('''
        INSERT INTO "{0}" 
        (record_id, entity_id, confidence, 
         reviewed, checked_out) 
          SELECT 
            record_id, 
            entity_id, 
            confidence, 
            FALSE AS reviewed, 
            FALSE AS checked_out
          FROM "temp_{1}" temp 
          LEFT JOIN (
            SELECT record_id 
            FROM "{0}"
          ) AS entity USING(record_id) 
          WHERE entity.record_id IS NULL
    '''.format(entity_table, session_id))

    last_update = datetime.now().replace(tzinfo=TIME_ZONE)

    with engine.begin() as c:
        c.execute(upd, last_update=last_update)
        c.execute(ins)

    temp_table.drop(bind=engine)

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

    array_cols = [c.name for c in cr.columns if str(c.type).endswith('[]')]
    col_names = [c for c in proc_table.columns.keys() if c != 'record_id']
    cols = ['array_agg(p."{0}"::VARCHAR) AS {0}'.format(c) for c in col_names]
    rows = ''' 
        SELECT 
          e.entity_id,
          {0},
          COUNT(*) AS member_count
        FROM "entity_{1}" AS e
        JOIN "processed_{1}" AS p
          ON e.record_id = p.record_id
        GROUP BY e.entity_id
    '''.format(','.join(cols), session_id)
    names = cr.columns.keys()
    canon_file = StringIO()
    writer = csv.writer(canon_file)
    writer.writerow(names)
    for row in engine.execute(rows):
        r = [row.entity_id]
        dicts = [dict(**{n:None for n in col_names}) for i in range(row.member_count)]
        for idx, dct in enumerate(dicts):
            for name in col_names:
                val = getattr(row, name)[idx]
                if name in array_cols:
                    val = ','.join(val)
                dicts[idx][name] = val
        canon_form = dedupe.canonicalize(dicts)
        for name in names:
            if canon_form.get(name) is not None:
                if name in array_cols:
                    r.append('{"%s"}' % canon_form[name])
                else:
                    r.append(canon_form[name])
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
    canon_file.seek(0)
    cur.copy_expert(copy_st, canon_file)
    conn.commit()

def writeBlockingMap(session_id, block_data, canonical=False):

    if canonical:
        session_id = '{0}_cr'.format(session_id)
        pk_type = String
    else :
        pk_type = Integer

    metadata = MetaData()
    engine = worker_session.bind
    
    bkm = Table('block_{0}'.format(session_id), 
                metadata,
                Column('block_key', Text),
                Column('record_id', pk_type)
            )

    bkm.drop(engine, checkfirst=True)
    bkm.create(engine)


    # Blocking map
    rows = []
    for row in block_data:
        rows.append(dict(zip(['block_key', 'record_id'], row)))
        if len(rows) % 50000 is 0:
            with engine.begin() as conn:
                conn.execute(bkm.insert(), rows)
            rows = []

    if rows:
        with engine.begin() as conn:
            conn.execute(bkm.insert(), *rows)

    with engine.begin() as conn:
        conn.execute('DROP INDEX IF EXISTS "bk_{0}_idx"'.format(session_id))

    with engine.begin() as conn:
        conn.execute(''' 
            CREATE INDEX "bk_{0}_idx" ON "block_{0}" (block_key)
        '''.format(session_id))

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
            array_agg(block_id ORDER BY block_id) 
                AS sorted_id
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
          SELECT 
            record_id, 
            block_id,
            sorted_id[1:(array_upper(sorted_id, 1) - 1)]
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


