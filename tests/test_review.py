import unittest
import json
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
from api.models import User, DedupeSession, Group
from api.database import init_engine, app_session, worker_session
from test_config import DEFAULT_USER, DB_CONN
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import text
from api.utils.helpers import STATUS_LIST
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw, dedupeCanon

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class ReviewTest(unittest.TestCase):
    ''' 
    Test the review module
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

    def test_session_review(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get('/session-review/' + self.dd_sess.id + '/')
                assert "var mark_cluster_url = '/mark-cluster/' + session_id + '/';" in rv.data
                rv = c.get('/session-review/' + self.dd_sess.id + '/?second_review=true')
                assert "var mark_cluster_url = '/mark-canon-cluster/' + session_id + '/';" in rv.data
    
    def review_wrapper(self, canonical=False):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(self.dd_sess.id)), 'wb') as outp:
                outp.write(inp.read())
        initializeSession(self.dd_sess.id)
        initializeModel(self.dd_sess.id)
        dedupeRaw(self.dd_sess.id)
        endpoints = {
            'get': '/get-review-cluster/{0}/'.format(self.dd_sess.id),
            'mark_one': '/mark-cluster/{0}/'.format(self.dd_sess.id),
            'mark_all': '/mark-all-clusters/{0}/'.format(self.dd_sess.id),
        }
        entity_table = 'entity_{0}'.format(self.dd_sess.id)
        if canonical:
            with self.app.test_request_context():
                self.login()
                with self.client as c:
                    c.get('/mark-all-clusters/{0}/'.format(self.dd_sess.id))
            dedupeCanon(self.dd_sess.id)
            endpoints = {
                'get': '/get-canon-review-cluster/{0}/'.format(self.dd_sess.id),
                'mark_one': '/mark-canon-cluster/{0}/'.format(self.dd_sess.id),
                'mark_all': '/mark-all-canon-clusters/{0}/'.format(self.dd_sess.id),
            }
            entity_table = 'entity_{0}_cr'.format(self.dd_sess.id)
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get(endpoints['get'])
                self.session.refresh(self.dd_sess)
                json_resp = json.loads(rv.data)
                assert json_resp['total_clusters'] == \
                        self.dd_sess.entity_count
                assert json_resp['review_remainder'] == \
                        self.dd_sess.review_count
                sel = '''
                    select checked_out 
                    from "{0}" 
                    where entity_id = :entity_id
                    '''.format(entity_table, self.dd_sess.id)
                with self.engine.begin() as conn:
                    rows = list(conn.execute(text(sel), 
                                entity_id=json_resp['entity_id']))
                assert rows[0][0] == True

                rv = c.get(endpoints['get'])
                json_resp_2 = json.loads(rv.data)
                assert json_resp_2['entity_id'] != json_resp['entity_id']

                matches = ','.join([unicode(r['record_id']) for r in \
                                    json_resp['objects']])
                params = '?match_ids={0}&entity_id={1}'\
                    .format(matches, json_resp['entity_id'])
                rv = c.get('{0}{1}'\
                    .format(endpoints['mark_one'],params))
                sel = '''
                    select clustered, record_id, reviewer 
                    from "entity_{0}" 
                    where entity_id = :entity_id
                    '''.format(self.dd_sess.id)
                with self.engine.begin() as conn:
                    rows = list(conn.execute(text(sel), 
                                entity_id=json_resp['entity_id']))
                assert list(set([r[0] for r in rows])) == [True]

                distinct = ','.join([unicode(r['record_id']) for r in \
                                     json_resp_2['objects']])
                params = '?distinct_ids={0}&entity_id={1}'\
                    .format(distinct, json_resp_2['entity_id'])
                rv = c.get('{0}{1}'.format(endpoints['mark_one'] ,params))
                sel = 'select record_id from "{0}"'\
                    .format(entity_table)
                with self.engine.begin() as conn:
                    rows = list(conn.execute(sel))
                rows = [r[0] for r in rows]
                assert set([r['record_id'] for r in json_resp_2['objects']])\
                    .isdisjoint(set(rows))

                rv = c.get(endpoints['mark_all'])
                sel = 'select clustered from "entity_{0}"'\
                    .format(self.dd_sess.id)
                with self.engine.begin() as conn:
                    rows = list(conn.execute(sel))
                assert list(set([r[0] for r in rows])) == [True]

    def test_review_clusters(self):
        self.review_wrapper()
    
    def test_review_canon_clusters(self):
        self.review_wrapper(canonical=True)
