import unittest
import json
from api.queue import queuefunc, DelayedResult, processMessage
from uuid import uuid4
from api import create_app
import time
from api.database import app_session, worker_session, init_engine
from api.models import User
from test_config import DEFAULT_USER, DB_CONN
from sqlalchemy.orm import sessionmaker, scoped_session
from redis import Redis

redis = Redis()

@queuefunc
def add(a, b):
    return a + b

@queuefunc
def error():
    raise Exception('Test Exception')

class QueueTest(unittest.TestCase):
    ''' 
    Test the queue module
    '''
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(config='tests.test_config')
        cls.client = cls.app.test_client()
        cls.qkey = cls.app.config['REDIS_QUEUE_KEY']
        keys = redis.keys(pattern='{0}*'.format(cls.qkey))
        redis.delete(keys)
        cls.engine = init_engine(cls.app.config['DB_CONN'])
   
        cls.session = scoped_session(sessionmaker(bind=cls.engine, 
                                              autocommit=False, 
                                              autoflush=False))
        cls.user = cls.session.query(User).first()
        cls.user_pw = DEFAULT_USER['user']['password']

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        app_session.close()
        worker_session.close()
        worker_session.bind.dispose()
        cls.engine.dispose()

    def login(self, email=None, pw=None):
        if not email: 
            email = self.user.email
        if not pw:
            pw = self.user_pw
        return self.client.post('/login/', data=dict(
                    email=email,
                    password=pw,
                ), follow_redirects=True)

    def test_queuefunc(self):
        key = add.delay(1,3, qkey=self.qkey).key
        rv = DelayedResult(key)
        while not rv.return_value:
            processMessage(qkey=self.qkey)
            time.sleep(1)
        assert rv.return_value == 4

    def test_exception(self):
        key = error.delay(qkey=self.qkey).key
        rv = DelayedResult(key)
        while not rv.return_value:
            processMessage(qkey=self.qkey)
            time.sleep(1)
        assert rv.return_value == 'Exc: Test Exception'

    def test_working_nokey(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get('/working/')
                assert json.loads(rv.data)['ready'] == False
        
    def test_working(self):
        key = add.delay(1,3,qkey=self.qkey).key
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['deduper_key'] = key
                processMessage(qkey=self.qkey)
                time.sleep(1)
                rv = c.get('/working/')
                assert json.loads(rv.data)['ready'] == True
                assert json.loads(rv.data)['result'] == 4

