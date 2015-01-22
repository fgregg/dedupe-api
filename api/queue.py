from pickle import loads, dumps
from uuid import uuid4
import sys
import os
from api.app_config import REDIS_QUEUE_KEY, DB_CONN
from api.database import init_engine, worker_session
from api.models import DedupeSession, WorkTable
import traceback
from sqlalchemy.exc import ProgrammingError, InternalError
from sqlalchemy import text

try:
    from raven import Client
    client = Client(os.environ['DEDUPE_WORKER_SENTRY_URL'])
except ImportError: # pragma: no cover
    client = None
except KeyError:
    client = None

def queuefunc(f):
    def delay(*args, **kwargs):
        engine = worker_session.bind
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
    engine = worker_session.bind
    sel = "SELECT * FROM work_table LIMIT 1"
    work = engine.execute(sel).first()
    if work:
        func, args, kwargs = loads(work.value)
        try:
            try:
                sel = text('SELECT id from dedupe_session WHERE id = :id')
                sess = engine.execute(sel, id=args[0]).first()
                if sess:
                    with engine.begin() as conn:
                        conn.execute(text('''
                            UPDATE dedupe_session SET
                                processing = TRUE
                            WHERE id = :id
                            '''), id=args[0])
            except (IndexError, ProgrammingError, InternalError):
                sess = None
                pass
            func(*args, **kwargs)
            if sess:
                with engine.begin() as conn:
                    conn.execute(text('''
                        UPDATE dedupe_session SET
                            processing = FALSE
                        WHERE id = :id
                        '''), id=args[0])
            with engine.begin() as conn:
                conn.execute(text(''' 
                    DELETE FROM work_table WHERE key = :key
                '''), key=work.key)
        except Exception, e:
            if client: # pragma: no cover
                client.captureException()
            tb = traceback.format_exc()
            print tb
            with engine.begin() as conn:
                conn.execute(text(''' 
                    UPDATE work_table SET
                        traceback = :tb,
                        value = :value
                    WHERE key = :key
                '''), tb=tb, value=e.message, key=work.key)
        del args
        del kwargs

def queue_daemon(db_conn=DB_CONN): # pragma: no cover
    engine = init_engine(db_conn)
    work_table = WorkTable.__table__
    work_table.create(engine, checkfirst=True)
    print 'Listening for messages...'
    while 1:
        processMessage()
