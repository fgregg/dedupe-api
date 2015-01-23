from pickle import loads, dumps
from redis import Redis
from uuid import uuid4
import sys
import os
from api.app_config import REDIS_QUEUE_KEY, DB_CONN, WORKER_SENTRY
from api.database import init_engine, worker_session
from api.models import DedupeSession
import traceback
from sqlalchemy.exc import ProgrammingError, InternalError

try:
    from raven import Client
    client = Client(os.environ[''])
except ImportError: # pragma: no cover
    client = None
except KeyError:
    client = None

redis = Redis()

class DelayedResult(object):
    def __init__(self, key):
        self.key = key
        self._rv = None

    @property
    def return_value(self):
        if self._rv is None:
            rv = redis.get(self.key)
            if rv is not None:
                self._rv = loads(rv)
        return self._rv
    
def queuefunc(f):
    def delay(*args, **kwargs):
        qkey = kwargs.get('qkey', REDIS_QUEUE_KEY)
        try:
            del kwargs['qkey']
        except KeyError:
            pass
        key = '%s:result:%s' % (qkey, str(uuid4()))
        s = dumps((f, key, args, kwargs))
        redis.rpush(qkey, s)
        return DelayedResult(key)
    f.delay = delay
    return f

def processMessage(rv_ttl=5000, qkey=None):
    msg = redis.blpop(qkey)
    func, key, args, kwargs = loads(msg[1])
    try:
        try:
            sess = worker_session.query(DedupeSession).get(args[0])
            if sess:
                sess.processing = True
                worker_session.add(sess)
                worker_session.commit()
        except (IndexError, ProgrammingError, InternalError):
            sess = None
            pass
        rv = {
            'value': func(*args, **kwargs),
            'status': 'ok',
        }
        if sess:
            sess.processing = False
            worker_session.add(sess)
            worker_session.commit()
    except Exception, e:
        if client: # pragma: no cover
            client.captureException()
        tb = traceback.format_exc()
        print tb
        rv = {
            'value': 'Exc: %s' % (e.message),
            'status': 'error',
            'traceback': tb
        }
    if rv is not None:
        redis.set(key, dumps(rv))
        redis.expire(key, rv_ttl)
        del args
        del kwargs
        del rv
        del msg

def queue_daemon(db_conn=DB_CONN, qkey=REDIS_QUEUE_KEY): # pragma: no cover
    init_engine(db_conn)
    print 'Listening for messages...'
    while 1:
        processMessage(qkey=qkey)
