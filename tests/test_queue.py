import unittest
import json
from api.queue import queuefunc, processMessage
from uuid import uuid4
from api import create_app
import time
from api.database import app_session, worker_session, init_engine
from api.models import User
from .test_config import DEFAULT_USER, DB_CONN
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, scoped_session

import logging
logging.getLogger('dedupe').setLevel(logging.WARNING)

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
        cls.engine = init_engine(cls.app.config['DB_CONN'])
        cls.session = scoped_session(sessionmaker(bind=cls.engine, 
                                              autocommit=False, 
                                              autoflush=False))
        cls.user = cls.session.query(User).first()
        cls.user_pw = DEFAULT_USER['user']['password']
        with cls.engine.begin() as conn:
            conn.execute('delete from work_table')

    def setUp(self):
        processMessage(db_conn=self.app.config['DB_CONN'])
        with self.engine.begin() as conn:
            conn.execute('delete from work_table')

    @classmethod
    def tearDownClass(cls):
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
        key = add.delay(1,3)
        processMessage(db_conn=self.app.config['DB_CONN'])
        time.sleep(1)
        work = self.engine.execute(
                text('SELECT return_value FROM work_table where key = :key'), 
                key=key).first()
        assert int(work.return_value) == 4

    def test_exception(self):
        key = error.delay()
        processMessage(db_conn=self.app.config['DB_CONN'])
        time.sleep(1)
        work = self.engine.execute(
                text('SELECT * FROM work_table where key = :key'), 
                key=key).first()
        assert work.return_value == 'Test Exception'

    def test_clear_error(self):
        key = error.delay()
        processMessage(db_conn=self.app.config['DB_CONN'])
        time.sleep(1)
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = ['poo']
                rv = c.get('/clear-error/?work_id=' + key, follow_redirects=True)
                mess = ''' 
                    SELECT cleared 
                    FROM work_table 
                    WHERE key = :key
                '''
                cleared = self.engine.execute(text(mess), key=key).first()
                assert cleared.cleared == True
