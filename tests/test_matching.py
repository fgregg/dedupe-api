import unittest
import json
import cPickle
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
from api.models import User, DedupeSession, Group
from api.database import init_engine, app_session, worker_session
from api.auth import check_sessions
from api.utils.helpers import STATUS_LIST
from test_config import DEFAULT_USER, DB_CONN
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlalchemy import text

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class MatchingTest(unittest.TestCase):
    ''' 
    Test the matching module
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
        cls.group = cls.user.groups[0]
        cls.user_pw = DEFAULT_USER['user']['password']

    def setUp(self):
        self.field_defs = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        settings = open(join(fixtures_path, 'settings_file.dedupe'), 'rb').read()
        training = open(join(fixtures_path, 'training_data.json'), 'rb').read()
        self.dd_sess = DedupeSession(
                        id=unicode(uuid4()), 
                        name='test_filename.csv',
                        group=self.group,
                        status=STATUS_LIST[0],
                        settings_file=settings,
                        field_defs=self.field_defs,
                        training_data=training
                      )
        self.session.add(self.dd_sess)
        self.session.commit()

    def tearDown(self):
        try:
            self.session.delete(self.dd_sess)
            self.session.commit()
        except Exception:
            self.session.rollback()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()
        app_session.close()
        worker_session.close()
        worker_session.bind.dispose()
        cls.engine.dispose()

