from pickle import loads, dumps
from uuid import uuid4
from datetime import datetime
import sys
import os
from api.app_config import REDIS_QUEUE_KEY, DB_CONN, WORKER_SENTRY, TIME_ZONE
from api.database import init_engine
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
        s = dumps((f, args, kwargs))
        key = str(uuid4())
        engine = init_engine(DB_CONN)
        with engine.begin() as conn:
            conn.execute(text(''' 
                INSERT INTO work_table(key, work_value) 
                VALUES (:key, :value)
            '''), key=key, value=s)
        return key
    f.delay = delay
    return f

def processMessage(db_conn=DB_CONN):
    engine = init_engine(db_conn)
    conn = engine.connect()
    trans = conn.begin()
    sel = "SELECT * FROM work_table WHERE claimed = FALSE LIMIT 1 FOR UPDATE"
    work = conn.execute(sel).first()
    if not work:
        time.sleep(1)
        trans.rollback()
        conn.close()
    else:
        func, args, kwargs = loads(work.work_value)
        sess = None
        if args:
            sel = text('SELECT id from dedupe_session WHERE id = :id')
            sess = engine.execute(sel, id=str(args[0])).first()
        upd = """ 
            UPDATE work_table SET
                claimed = TRUE
        """
        upd_args = {'key': work.key}
        if sess:
            upd = '{0}, session_id = :session_id'.format(upd)
            upd_args['session_id'] = sess.id
        upd = '{0} WHERE key = :key'.format(upd)
        conn.execute(text(upd), **upd_args)
        trans.commit()
        
        upd_args = {
            'tb': None,
            'return_value': None,
            'key': work.key,
            'updated': datetime.now().replace(tzinfo=TIME_ZONE),
            'cleared': True,
        }
        try:
            upd_args['return_value'] = func(*args, **kwargs)
        except Exception as e:
            if client: # pragma: no cover
                client.captureException()
            upd_args['tb'] = traceback.format_exc()
            upd_args['return_value'] = str(e)
            upd_args['cleared'] = False
            print(upd_args['tb'])
        upd = ''' 
                UPDATE work_table SET
                    traceback = :tb,
                    return_value = :return_value,
                    updated = :updated,
                    cleared = :cleared
            '''
        if sess:
            upd = '{0}, session_id = :sess_id'.format(upd)
            upd_args['sess_id'] = sess.id
            with engine.begin() as conn:
                conn.execute(text('''
                    UPDATE dedupe_session SET
                        processing = FALSE
                    WHERE id = :id
                    '''), id=sess.id)
        upd = text('{0} WHERE key = :key'.format(upd))
        with engine.begin() as conn:
            conn.execute(upd, **upd_args)
        del args
        del kwargs
    engine.dispose()

def queue_daemon(db_conn=DB_CONN): # pragma: no cover
    # import logging
    # logging.getLogger().setLevel(logging.DEBUG)
    global engine
    engine = init_engine(DB_CONN)
    work_table = WorkTable.__table__
    work_table.create(engine, checkfirst=True)
    print('Listening for messages...')
    while 1:
        processMessage()
