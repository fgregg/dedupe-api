import pickle
from uuid import uuid4
from datetime import datetime
import sys
import os
from api.app_config import REDIS_QUEUE_KEY, DB_CONN, WORKER_SENTRY, TIME_ZONE
from api.models import DedupeSession, WorkTable
import traceback
from sqlalchemy.exc import ProgrammingError, InternalError
from sqlalchemy import text
import time


try:
    from raven import Client
    client = Client(dsn=WORKER_SENTRY)
except ImportError: # pragma: no cover
    client = None
except KeyError:
    client = None

def queuefunc(f):

    def delay(*args, **kwargs):
        
        from api.database import engine
        
        pickled_task = pickle.dumps((f, args, kwargs))
        key = str(uuid4())

        if args:
            session_id = args[0]
        else :
            session_id = None

        task_name = f.__name__

        with engine.begin() as conn:
            conn.execute(text(''' 
                INSERT INTO work_table
                    (key, work_value, session_id, task_name) 
                SELECT :key, :value, :session_id, :task_name
                WHERE NOT EXISTS (
                    SELECT * FROM work_table WHERE 
                         work_value = :value AND
                         session_id = :session_id
                         AND completed = FALSE)
            '''), 
                         key=key, value=pickled_task, session_id=session_id, 
                         task_name = task_name)
        return key

    f.delay = delay
    return f

def processMessage(db_conn=DB_CONN):
    
    from api.database import engine
    
    with engine.begin() as conn:
        upd = '''
            UPDATE work_table set claimed = TRUE FROM (
                SELECT * FROM work_table WHERE claimed = FALSE LIMIT 1
            ) AS s
            WHERE work_table.key = s.key
            RETURNING work_table.*
        '''
        work = conn.execute(upd).first()

    if not work:
        time.sleep(1)
        engine.dispose()
        return
    
    func, args, kwargs = pickle.loads(work.work_value)

    upd_args = {
        'key': work.key,
        'updated': datetime.now().replace(tzinfo=TIME_ZONE),
        'completed': True,
    }

    try:
        upd_args['return_value'] = func(*args, **kwargs)
        upd_args['cleared'] = True
        upd_args['tb'] = None
    except Exception as e:
        if client: # pragma: no cover
            client.captureException()
        upd_args['tb'] = traceback.format_exc()
        print(upd_args['tb'])

        upd_args['return_value'] = str(e)
        
        if func.__name__ in ['initializeModel', 'initializeSession']:
            upd_args['cleared'] = False
        else :
            upd_args['cleared'] = True

    upd = ''' 
           UPDATE work_table SET
                traceback = :tb,
                return_value = :return_value,
                updated = :updated,
                completed = :completed,
                cleared = :cleared
                WHERE key = :key
          '''
    with engine.begin() as conn:
        conn.execute(text(upd), **upd_args)

    if work.session_id :
        with engine.begin() as conn:
            conn.execute(text('''
            UPDATE dedupe_session SET
                processing = FALSE
            WHERE id = :id
            '''), id=work.session_id)

    del args
    del kwargs

    engine.dispose()

def queue_daemon(db_conn=DB_CONN): # pragma: no cover
    # import logging
    # logging.getLogger().setLevel(logging.WARNING)

    from api.database import init_engine
    
    engine = init_engine(DB_CONN)
    
    work_table = WorkTable.__table__
    work_table.create(engine, checkfirst=True)
    
    print('Listening for messages...')
    while 1:
        processMessage()
