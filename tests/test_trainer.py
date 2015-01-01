import unittest
import json
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
from api.utils.helpers import slugify
from api.database import init_engine, app_session, worker_session
from api.models import User
from api.utils.db_functions import writeRawTable
from test_config import DEFAULT_USER
from sqlalchemy.orm import sessionmaker, scoped_session
from csvkit.unicsv import UnicodeCSVReader

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class TrainerTest(unittest.TestCase):
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
        cls.session_id = unicode(uuid4())

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

    def test_upload(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['session_id'] = self.session_id
                rv = c.post('/upload/', data={
                            'input_file': (open(join(fixtures_path, 
                                'csv_example_messy_input.csv'),'rb'), 
                                'csv_example_messy_input.csv')
                            })
                assert session.has_key('init_key')
                assert json.loads(rv.data)['ready']

    def test_train_start(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.get('/train-start/')
                assert session.has_key('session_id')
    
    def test_clear_session(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['fieldnames'] = ['this']
                    sess['session_name'] = 'test'
                    sess['training_data'] = {'test': 'thing'}
                    sess['user_id'] = self.user.id
                rv = c.get('/train-start/')
                assert 'fieldnames' not in session.keys()
                assert 'session_name' not in session.keys()
                assert 'training_data' not in session.keys()
    
    def test_select_fields(self):
        fieldnames = []
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as f:
            reader = UnicodeCSVReader(f)
            fieldnames = reader.next()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['fieldnames'] = fieldnames
                rv = c.get('/select-fields/')
                for field in fieldnames:
                    assert field in rv.data

    def test_select_fields_sid(self):
        fobj = open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb')
        fieldnames = writeRawTable(session_id=self.session_id, file_obj=fobj)
        fieldnames = [slugify(unicode(f)) for f in fieldnames]
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get('/select-fields/?session_id=' + self.session_id)
                assert set(session['fieldnames']) == set(fieldnames)
    
    def test_select_fields_post(self):
        fieldnames = []
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as f:
            reader = UnicodeCSVReader(f)
            fieldnames = reader.next()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['fieldnames'] = fieldnames
                post_data = {
                      'phone': ['on'],
                      'email': ['on'],
                      'site_name': ['on'],
                      'zip': ['on'],
                    }
                rv = c.post('/select-fields/', data=post_data)
                assert set(session['field_list']) == set(post_data.keys())

    def test_select_fields_nothing(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.post('/select-fields/', data={})
                assert 'You must select at least one field to compare on.' in rv.data
