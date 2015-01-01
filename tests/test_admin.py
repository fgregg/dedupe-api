import unittest
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
from api.models import User, DedupeSession
from api.database import init_engine, app_session, worker_session
from api.auth import check_sessions
from api.utils.helpers import STATUS_LIST
from test_config import DEFAULT_USER, DB_CONN
from sqlalchemy.orm import sessionmaker, scoped_session

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class AdminTest(unittest.TestCase):
    ''' 
    Test the admin module
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
        field_defs = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        settings = open(join(fixtures_path, 'settings_file.dedupe'), 'rb').read()
        training = open(join(fixtures_path, 'training_data.json'), 'rb').read()
        cls.dd_sess = DedupeSession(
                        id=unicode(uuid4()), 
                        name='test_filename.csv',
                        group=cls.group,
                        status=STATUS_LIST[0],
                        settings_file=settings,
                        field_defs=field_defs,
                        training_data=training
                      )
        cls.session.add(cls.dd_sess)
        cls.session.commit()

    @classmethod
    def tearDownClass(cls):
        cls.session.delete(cls.dd_sess)
        cls.session.commit()
        cls.session.close()
        app_session.close()
        worker_session.close()
        worker_session.bind.dispose()
        cls.engine.dispose()

    def login(self):
        return self.client.post('/login/', data=dict(
                    email=self.user.email,
                    password=self.user_pw,
                ), follow_redirects=True)

    def logout(self):
        return self.client.get('/logout/')

    def add_user(self, data):
        return self.client.post('/add-user/', 
                                  data=data, 
                                  follow_redirects=True)

    def test_add_user(self):
        with self.app.test_request_context():
            self.login()
            rv = self.add_user({'name': 'harry',
                                'email': 'harry@harry.com',
                                'password': 'harryspw',
                                'roles': [1],
                                'groups': [self.group.id],}) 
            assert 'User harry added' in rv.data

    def test_duplicate_name(self):
        with self.app.test_request_context():
            self.login()
            rv = self.add_user({'name': 'eric',
                                'email': 'harry@harry.com',
                                'password': 'harryspw',
                                'roles': [1],
                                'groups': [self.group.id],})
            assert 'Name is already registered' in rv.data
    
    def test_duplicate_email(self):
        with self.app.test_request_context():
            self.login()
            rv = self.add_user({'name': 'joe',
                                'email': 'eric@eric.com',
                                'password': 'harryspw',
                                'roles': [1],
                                'groups': [self.group.id],})
            assert 'Email address is already registered' in rv.data

    def test_session_admin(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/session-admin/' + self.dd_sess.id + '/', follow_redirects=True)
                assert 'session-admin' in request.path
