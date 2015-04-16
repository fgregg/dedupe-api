import unittest
import json
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
from api.models import User, DedupeSession, Group
from api.database import init_engine, app_session as db_session
from .test_config import DEFAULT_USER, DB_CONN
from sqlalchemy import text
from api.utils.helpers import STATUS_LIST, slugify
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw, dedupeCanon, bulkMarkClusters, bulkMarkCanonClusters
from api.utils.db_functions import saveTraining

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

import logging
logging.getLogger('dedupe').setLevel(logging.WARNING)

class TrackUsageTest(unittest.TestCase):
    ''' 
    Test the track_usage module
    '''
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(config='tests.test_config')
        cls.client = cls.app.test_client()
        cls.engine = init_engine(cls.app.config['DB_CONN'])
   
        cls.user = db_session.query(User).first()
        cls.group = cls.user.groups[0]
        cls.user_pw = DEFAULT_USER['user']['password']
        cls.field_defs = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        settings = open(join(fixtures_path, 'settings_file.dedupe'), 'rb').read()
        cls.dd_sess = DedupeSession(
                        id=str(uuid4()), 
                        filename='test_filename.csv',
                        name='Test Session',
                        group=cls.group,
                        status=STATUS_LIST[0]['machine_name'],
                        settings_file=settings,
                        field_defs=cls.field_defs,
                      )
        db_session.add(cls.dd_sess)
        db_session.commit()
        
        training = json.load(open(join(fixtures_path, 'training_data.json'), 'r'))
        saveTraining(cls.dd_sess.id, training, cls.user.name)
        
        # Go through dedupe process
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'r') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(cls.dd_sess.id)), 'w') as outp:
                raw_fieldnames = next(inp)
                inp.seek(0)
                outp.write(inp.read())
        fieldnames = [slugify(c).strip('\n') for c in raw_fieldnames.split(',')]
        initializeSession(cls.dd_sess.id, fieldnames)
        initializeModel(cls.dd_sess.id)
        dedupeRaw(cls.dd_sess.id)
        bulkMarkClusters(cls.dd_sess.id, user=cls.user.name)
        bulkMarkCanonClusters(cls.dd_sess.id, user=cls.user.name)

    @classmethod
    def tearDownClass(cls):
        db_session.close()
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
    
    def setUp(self):
        with self.engine.begin() as conn:
            conn.execute('DELETE FROM dedupe_usage')
    
    def add_user(self, data):
        return self.client.post('/add-user/', 
                                  data=data, 
                                  follow_redirects=True)

    def test_increment(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]

                i = 0
                while i < 10:
                    unmatched = c.get('/get-unmatched-record/?session_id=' + self.dd_sess.id)
                    obj = json.loads(unmatched.data.decode('utf-8'))['object']
                    post_data = {
                        'api_key': self.user.id,
                        'session_id': self.dd_sess.id,
                        'object': obj
                    }
                    rv = c.post('/match/', data=json.dumps(post_data))
                    i += 1
            rows = []
            with self.engine.begin() as conn:
                rows = list(conn.execute('SELECT count(*) FROM dedupe_usage'))
            assert int(rows[0][0]) == 10

    def test_user_increment(self):
        extra_user_id = None
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                data = {'name': 'harry',
                        'email': 'harry@harry.com',
                        'password': 'harryspw',
                        'roles': [1],
                        'groups': [self.group.id],
                    }
                c.post('/add-user/', 
                    data=data, 
                    follow_redirects=True)
                
                rows = []
                with self.engine.begin() as conn:
                    rows = list(conn.execute(
                                  text('select id from dedupe_user where name = :name limit 1'), 
                                  name='harry')
                                )
                extra_user_id = rows[0][0]

        with self.app.test_request_context():
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                    sess['user_id'] = extra_user_id
                i = 0
                while i < 10:
                    unmatched = c.get('/get-unmatched-record/?session_id=' + self.dd_sess.id)
                    obj = json.loads(unmatched.data.decode('utf-8'))['object']
                    post_data = {
                        'api_key': extra_user_id,
                        'session_id': self.dd_sess.id,
                        'object': obj
                    }
                    rv = c.post('/match/', data=json.dumps(post_data))
                    i += 1
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                i = 0
                while i < 10:
                    unmatched = c.get('/get-unmatched-record/?session_id=' + self.dd_sess.id)
                    obj = json.loads(unmatched.data.decode('utf-8'))['object']
                    post_data = {
                        'api_key': self.user.id,
                        'session_id': self.dd_sess.id,
                        'object': obj
                    }
                    rv = c.post('/match/', data=json.dumps(post_data))
                    i += 1
          
        rows = []
        with self.engine.begin() as conn:
            rows = list(conn.execute(''' 
                SELECT count(*), api_key
                FROM dedupe_usage
                GROUP BY api_key
                '''))

        assert int(rows[0][0]) == 10
        assert int(rows[1][0]) == 10
        api_keys = [r[1] for r in rows]
        assert set(api_keys) == set([extra_user_id, self.user.id])
