import unittest
import json
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
from api.models import User, DedupeSession, Group
from api.database import init_engine, app_session, worker_session
from .test_config import DEFAULT_USER, DB_CONN
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import text
from api.utils.helpers import STATUS_LIST, preProcess
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw, dedupeCanon, bulkMarkClusters, bulkMarkCanonClusters, \
    populateHumanReview

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

import logging
logging.getLogger('dedupe').setLevel(logging.WARNING)

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
        cls.field_defs = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        settings = open(join(fixtures_path, 'settings_file.dedupe'), 'rb').read()
        training = open(join(fixtures_path, 'training_data.json'), 'rb').read()
        cls.dd_sess = DedupeSession(
                        id=str(uuid4()), 
                        filename='test_filename.csv',
                        name='Test Session',
                        group=cls.group,
                        status=STATUS_LIST[0]['machine_name'],
                        settings_file=settings,
                        field_defs=cls.field_defs,
                        training_data=training,
                      )
        cls.session.add(cls.dd_sess)
        cls.session.commit()
        
        # Go through dedupe process
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(cls.dd_sess.id)), 'wb') as outp:
                outp.write(inp.read())
        initializeSession(cls.dd_sess.id)
        initializeModel(cls.dd_sess.id)
        dedupeRaw(cls.dd_sess.id)
        bulkMarkClusters(cls.dd_sess.id, user=cls.user.name)
        bulkMarkCanonClusters(cls.dd_sess.id, user=cls.user.name)

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

    def test_exact_match(self):
        model_fields = [f['field'] for f in json.loads(self.field_defs.decode('utf-8'))]
        fields = ','.join([u'r.{0}'.format(f) for f in model_fields])
        sel = ''' 
          SELECT 
            {0}
          FROM "raw_{1}" AS r
          JOIN "entity_{1}" AS e
            USING(record_id)
          LIMIT 1
        '''.format(fields, self.dd_sess.id)
        match_record = {}
        with self.engine.begin() as conn:
            match_record = dict(zip(model_fields, list(conn.execute(sel))[0]))
        post_data = {
            'api_key': self.user.id,
            'session_id': self.dd_sess.id,
            'object': match_record
        }
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                rv = c.post('/match/', data=json.dumps(post_data))
                matches = json.loads(rv.data.decode('utf-8'))['matches']
                assert max([float(m['match_confidence']) for m in matches]) == 1.0

    def test_bad_fields(self):
        post_data = {
            'api_key': self.user.id,
            'session_id': self.dd_sess.id,
            'object': {
                'boo': 'foo',
                'schmoo': 'schmoo'
            },
        }
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                rv = c.post('/match/', data=json.dumps(post_data))
                assert rv.status == '400 BAD REQUEST'
                assert json.loads(rv.data.decode('utf-8'))['status'] == 'error'
                assert 'schmoo' in json.loads(rv.data.decode('utf-8'))['message']

    def test_bad_post(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                rv = c.post('/match/', data='lalala')
                assert rv.status == '400 BAD REQUEST'
                assert json.loads(rv.data.decode('utf-8'))['status'] == 'error'
                assert 'JSON object' in json.loads(rv.data.decode('utf-8'))['message']
    
    def test_no_match(self):
        model_fields = [f['field'] for f in json.loads(self.field_defs.decode('utf-8'))]
        match_record = {a: str(i) for i,a in enumerate(model_fields)}
        post_data = {
            'api_key': self.user.id,
            'session_id': self.dd_sess.id,
            'object': match_record
        }
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                rv = c.post('/match/', data=json.dumps(post_data))
                assert json.loads(rv.data.decode('utf-8'))['status'] == 'ok'
                assert len(json.loads(rv.data.decode('utf-8'))['matches']) == 0

    def test_train(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]

                # First, get an unmatched record and try to find matches
                matches = []
                while not matches:
                    unmatched = c.get('/get-unmatched-record/?session_id=' + self.dd_sess.id)
                    obj = json.loads(unmatched.data.decode('utf-8'))['object']
                    post_data = {
                        'api_key': self.user.id,
                        'session_id': self.dd_sess.id,
                        'object': obj
                    }
                    rv = c.post('/match/', data=json.dumps(post_data))
                    matches = json.loads(rv.data.decode('utf-8'))['matches']

                matches[0]['match'] = 1
                del matches[0]['entity_id']
                for match in matches[1:]:
                    match['match'] = 0
                    del match['entity_id']
                post_data['matches'] = matches
                rv = c.post('/train/', data=json.dumps(post_data))
                self.session.refresh(self.dd_sess)
                td = json.loads(self.dd_sess.training_data.decode('utf-8'))
                del matches[0]['match']
                matched = {k:preProcess(str(v)) for k,v in matches[0].items()}
                record_ids = set()
                for left, right in td['match']:
                    record_ids.add(left['record_id'])
                    record_ids.add(right['record_id'])
                assert set([matched['record_id'], obj['record_id']]).intersection(record_ids)
                for match in matches[1:]:
                    m = {k:preProcess(str(v)) for k,v in match.items()}
                    del m['match']
                    assert [m, obj] in td['distinct']

    def test_matches_add_entity_getunmatched(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]

                # First, get an unmatched record and try to find matches
                matches = []
                while not matches:
                    unmatched = c.get('/get-unmatched-record/?session_id=' + self.dd_sess.id)
                    obj = json.loads(unmatched.data.decode('utf-8'))['object']
                    post_data = {
                        'api_key': self.user.id,
                        'session_id': self.dd_sess.id,
                        'object': obj
                    }
                    rv = c.post('/match/', data=json.dumps(post_data))
                    matches = json.loads(rv.data.decode('utf-8'))['matches']
                
                for match in matches:
                    match['match'] = 1
                post_data['matches'] = matches
                post_data['add_entity'] = True

                # Second, add a matched record to the entity map
                add_entity = c.post('/train/', data=json.dumps(post_data))
                rows = []
                with self.engine.begin() as conn:
                    rows = list(conn.execute(text(''' 
                        SELECT entity_id 
                          FROM "entity_{0}"
                        WHERE record_id = :record_id
                    '''.format(self.dd_sess.id)), record_id=obj['record_id']))
                entity_id = rows[0][0]

                # Check to see that the status is OK and that the new entry is
                # associated with the correct entity
                assert json.loads(add_entity.data.decode('utf-8'))['status'] == 'ok'
                assert entity_id == matches[0]['entity_id']

