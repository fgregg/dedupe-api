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

engine = init_engine(DB_CONN)

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
        key = unicode(uuid4())
        with engine.begin() as conn:
            conn.execute(text(''' 
                INSERT INTO work_table(key, value) 
                VALUES (:key, :value)
            '''), key=key, value=s)
        return key
    f.delay = delay
    return f

def processMessage():
    sel = "SELECT * FROM work_table WHERE claimed = FALSE LIMIT 1"
    work = engine.execute(sel).first()
    if not work:
        time.sleep(1)
    else:
        func, args, kwargs = loads(work.value)
        try:
            sel = text('SELECT id from dedupe_session WHERE id = :id')
            sess = engine.execute(sel, id=args[0]).first()
        except (IndexError, ProgrammingError, InternalError):
            sess = None
            pass
        upd = """ 
            UPDATE work_table SET
                claimed = TRUE
        """
        upd_args = {'key': work.key}
        if sess:
            upd = '{0}, session_id = :session_id'.format(upd)
            upd_args['session_id'] = sess.id
        upd = '{0} WHERE key = :key'.format(upd)
        with engine.begin() as conn:
            conn.execute(text(upd), **upd_args)
        
        upd_args = {
            'tb': None,
            'value': None,
            'key': work.key,
            'updated': datetime.now().replace(tzinfo=TIME_ZONE),
            'cleared': True,
        }
        try:
            return_value = func(*args, **kwargs)
            if return_value:
                with engine.begin() as conn:
                    conn.execute(text(''' 
                        UPDATE work_table SET value = :value WHERE key = :key
                    '''), key=work.key, value=return_value)
            else:
                with engine.begin() as conn:
                    conn.execute(text(''' 
                        DELETE FROM work_table WHERE key = :key
                    '''), key=work.key)
        except Exception, e:
            if client: # pragma: no cover
                client.captureException()
            upd_args['tb'] = traceback.format_exc()
            upd_args['value'] = e.message
            upd_args['cleared'] = False
            print upd_args['tb']
        upd = ''' 
                UPDATE work_table SET
                    traceback = :tb,
                    value = :value,
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

def queue_daemon(db_conn=DB_CONN): # pragma: no cover
    import logging
    logging.getLogger().setLevel(logging.DEBUG)
    work_table = WorkTable.__table__
    work_table.create(engine, checkfirst=True)
    print 'Listening for messages...'
    while 1:
        processMessage()
