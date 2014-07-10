from pickle import loads, dumps
from redis import Redis
from uuid import uuid4
import sys
import os

REDIS_Q_KEY = 'deduper'

try:
    from raven import Client
    client = Client(os.environ['DEDUPE_WORKER_SENTRY_URL'])
except ImportError:
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
        qkey = REDIS_Q_KEY
        key = '%s:result:%s' % (qkey, str(uuid4()))
        s = dumps((f, key, args, kwargs))
        redis.rpush(REDIS_Q_KEY, s)
        return DelayedResult(key)
    f.delay = delay
    return f

def queue_daemon(rv_ttl=500):
    while 1:
        msg = redis.blpop(REDIS_Q_KEY)
        func, key, args, kwargs = loads(msg[1])
        try:
            rv = func(*args, **kwargs)
        except Exception, e:
            client.captureException()
            rv = 'Exc: %s' % (e.message)
        if rv is not None:
            redis.set(key, dumps(rv))
            redis.expire(key, rv_ttl)
