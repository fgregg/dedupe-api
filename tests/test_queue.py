import unittest
import json
from api.queue import queuefunc, processMessage
from uuid import uuid4
from api import create_app
import time
from api.database import app_session, worker_session, init_engine
from api.models import WorkTable
from .test_config import DEFAULT_USER, DB_CONN
from sqlalchemy.orm import sessionmaker, scoped_session

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
        cls.engine = init_engine(cls.app.config['DB_CONN'])
        with cls.engine.begin() as conn:
            conn.execute('delete from work_table')

    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute('delete from work_table')

    @classmethod
    def tearDownClass(cls):
        app_session.close()
        worker_session.close()
        worker_session.bind.dispose()
        cls.engine.dispose()

    def test_queuefunc(self):
        key = add.delay(1,3)
        processMessage()
        time.sleep(1)
        work = worker_session.query(WorkTable).get(key)
        assert work is None

    def test_exception(self):
        key = error.delay()
        processMessage()
        time.sleep(1)
        work = worker_session.query(WorkTable).get(key)
        worker_session.refresh(work)
        assert work.value == 'Test Exception'
