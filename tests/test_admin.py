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

    def login(self, email=None, pw=None):
        if not email: 
            email = self.user.email
        if not pw:
            pw = self.user_pw
        return self.client.post('/login/', data=dict(
                    email=email,
                    password=pw,
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

    def no_access(self, path):
        user = self.session.query(User)\
            .filter(User.name == 'harry')\
            .first()
        dummy_group = self.session.query(Group)\
            .filter(Group.name == 'dummy')\
            .first()
        user.groups = [dummy_group]
        self.session.add(user)
        self.session.commit()
        with self.app.test_request_context():
            self.login(email=user.email, pw='harryspw')
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = user.id
                return c.open(path + self.dd_sess.id + '/')

    def test_training_data(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/training-data/' + self.dd_sess.id + '/')
                assert json.loads(rv.data).keys() == ['distinct', 'match']

    def test_td_no_access(self):
        rv = self.no_access('/training-data/')
        assert json.loads(rv.data)['message'] == \
            "You don't have access to session {0}".format(self.dd_sess.id)
    
    def test_settings_file(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/settings-file/' + self.dd_sess.id + '/')
                assert str(type(cPickle.loads(rv.data))) == "<class 'dedupe.datamodel.DataModel'>"
    
    def test_sf_no_access(self):
        rv = self.no_access('/settings-file/')
        assert json.loads(rv.data)['message'] == \
            "You don't have access to session {0}".format(self.dd_sess.id)
    
    def test_field_defs(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/field-definitions/' + self.dd_sess.id + '/')
                fds = set([f['field'] for f in json.loads(rv.data)])
                expected = set([f['field'] for f in json.loads(self.field_defs)])
                assert fds == expected
    
    def test_fd_no_access(self):
        rv = self.no_access('/field-definitions/')
        assert json.loads(rv.data)['message'] == \
            "You don't have access to session {0}".format(self.dd_sess.id)

    def test_delete_model(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/delete-data-model/' + self.dd_sess.id + '/')
                self.session.refresh(self.dd_sess)
                assert self.dd_sess.field_defs is None
                assert self.dd_sess.training_data is None
    
    def test_delete_no_access(self):
        rv = self.no_access('/delete-data-model/')
        assert json.loads(rv.data)['message'] == \
            "You don't have access to session {0}".format(self.dd_sess.id)

    def test_delete_session(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/delete-session/' + self.dd_sess.id + '/')
        conn = self.engine.connect()
        rows = conn.execute(text('select * from dedupe_session where id = :id'), id=self.dd_sess.id)
        assert list(rows) == []
    
    def test_delete_sess_no_access(self):
        rv = self.no_access('/delete-session/')
        assert json.loads(rv.data)['message'] == \
            "You don't have access to session {0}".format(self.dd_sess.id)

    def test_session_list(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.open('/session-list/')
                assert json.loads(rv.data)['status'] == 'ok'
    
    def test_session_list_with_param(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.open('/session-list/?session_id=' + self.dd_sess.id)
                assert json.loads(rv.data)['status'] == 'ok'

