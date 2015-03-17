import unittest
import json
import sys
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api.models import User, Group
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw, bulkMarkClusters, reDedupeRaw, reDedupeCanon
from api.queue import processMessage
from sqlalchemy import text
from io import StringIO
import csv
from tests import DedupeAPITestCase
from .test_config import DEFAULT_USER

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

import logging
logging.getLogger('dedupe').setLevel(logging.WARNING)

if sys.version_info[:2] == (2,7):
    import cPickle as pickle
else:
    import pickle

class AdminTest(DedupeAPITestCase):
    ''' 
    Test the admin module
    '''

    def add_user(self, data):
        with self.app.test_request_context():
            self.login()
            return self.client.post('/add-user/', 
                                  data=data, 
                                  follow_redirects=True)

    def test_add_user(self):
        rv = self.add_user({'name': 'harry',
                            'email': 'harry@harry.com',
                            'password': 'harryspw',
                            'roles': [1],
                            'groups': [self.group.id],}) 
        assert 'User harry added' in rv.data.decode('utf-8')

    def test_duplicate_name(self):
        rv = self.add_user({'name': DEFAULT_USER['user']['name'],
                            'email': 'harry@harry.com',
                            'password': 'harryspw',
                            'roles': [1],
                            'groups': [self.group.id],})
        assert 'Name is already registered' in rv.data.decode('utf-8')
    
    def test_duplicate_email(self):
        rv = self.add_user({'name': 'joe',
                            'email': DEFAULT_USER['user']['email'],
                            'password': 'harryspw',
                            'roles': [1],
                            'groups': [self.group.id],})
        assert 'Email address is already registered' in rv.data.decode('utf-8')

    def test_session_admin(self):
        with self.client as c:
            with c.session_transaction() as sess:
                sess['user_id'] = self.user.id
            rv = c.open('/session-admin/?session_id=' + self.dd_sess.id, follow_redirects=True)
            assert 'session-admin' in request.path
            assert self.dd_sess.name in rv.data.decode('utf-8')
            assert self.dd_sess.description in rv.data.decode('utf-8')

    def no_access(self, path):
        dummy_group = self.session.query(Group)\
            .filter(Group.name == 'dummy')\
            .first()
        self.add_user({'name': 'george',
                       'email': 'george@harry.com',
                       'password': 'harryspw',
                       'roles': [1],
                       'groups': [dummy_group.id],}) 
        with self.app.test_request_context():
            self.login(email='george@harry.com', pw='harryspw')
            with self.client as c:
               #with c.session_transaction() as sess:
               #    sess['user_id'] = user.id
                return c.open(path, follow_redirects=True)

    def test_training_data(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/training-data/?session_id=' + self.dd_sess.id)
                td = json.loads(rv.data.decode('utf-8'))
                assert set(td.keys()) == set(['distinct', 'match'])

    def test_td_no_access(self):
        rv = self.no_access('/training-data/?session_id=' + self.dd_sess.id)
        assert "Sorry, you don't have access to that session" in rv.data.decode('utf-8')
    
    def test_settings_file(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/settings-file/?session_id=' + self.dd_sess.id)
                assert str(type(pickle.loads(rv.data))) == \
                        "<class 'dedupe.datamodel.DataModel'>"
    
    def test_sf_no_access(self):
        rv = self.no_access('/settings-file/?session_id=' + self.dd_sess.id)
        assert "Sorry, you don't have access to that session" in rv.data.decode('utf-8')
    
    def test_field_defs(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/field-definitions/?session_id=' + self.dd_sess.id)
                fds = set([f['field'] for f in json.loads(rv.data.decode('utf-8'))])
                expected = set([f['field'] for f in \
                        json.loads(self.field_defs)])
                assert fds == expected
    
    def test_fd_no_access(self):
        rv = self.no_access('/field-definitions/?session_id=' + self.dd_sess.id)
        assert "Sorry, you don't have access to that session" in rv.data.decode('utf-8')

    def test_delete_model(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/delete-data-model/?session_id=' + self.dd_sess.id)
                self.session.refresh(self.dd_sess)
                assert self.dd_sess.field_defs is None
                assert self.dd_sess.status == 'dataset uploaded'
                removed_tables = [
                    'entity_{0}',
                    'block_{0}',
                    'plural_block_{0}',
                    'covered_{0}',
                    'plural_key_{0}',
                    'small_cov_{0}',
                ]
                tables = [t[0] for t in self.engine.execute('select tablename from pg_catalog.pg_tables')]
                for table in removed_tables:
                    assert table.format(self.dd_sess.id) not in tables
    
    def test_delete_no_access(self):
        rv = self.no_access('/delete-data-model/?session_id=' + self.dd_sess.id)
        assert "Sorry, you don't have access to that session" in rv.data.decode('utf-8')

    def test_delete_session(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                rv = c.open('/delete-session/?session_id=' + self.dd_sess.id)
        conn = self.engine.connect()
        rows = conn.execute(text('select * from dedupe_session where id = :id'), id=self.dd_sess.id)
        assert list(rows) == []
    
    def test_delete_sess_no_access(self):
        rv = self.no_access('/delete-session/?session_id=' + self.dd_sess.id)
        assert "Sorry, you don't have access to that session" in rv.data.decode('utf-8')

    def test_session_list(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                rv = c.open('/session-list/')
                assert json.loads(rv.data.decode('utf-8'))['status'] == 'ok'
    
    def test_session_list_with_param(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                rv = c.open('/session-list/?session_id=' + self.dd_sess.id)
                assert json.loads(rv.data.decode('utf-8'))['status'] == 'ok'
                assert len(json.loads(rv.data.decode('utf-8'))['objects']) == 1
    
    def test_dump_entity_map(self):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(self.dd_sess.id)), 'wb') as outp:
                outp.write(inp.read())
        initializeSession(self.dd_sess.id)
        initializeModel(self.dd_sess.id)
        dedupeRaw(self.dd_sess.id)
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                c.get('/mark-all-clusters/?session_id={0}'.format(self.dd_sess.id))
                rv = c.get('/dump-entity-map/?session_id=' + self.dd_sess.id)
                row_count = ''' 
                    SELECT count(*) 
                    FROM "raw_{0}" AS r
                    JOIN "entity_{0}" AS e
                      ON r.record_id = e.record_id
                    WHERE e.clustered = TRUE
                '''.format(self.dd_sess.id)
                with self.engine.begin() as conn:
                    row_count = list(conn.execute(row_count))
                row_count = row_count[0][0]
                s = StringIO(rv.data.decode('utf-8'))
                reader = csv.reader(s)
                next(reader)
                assert len([r for r in list(reader) if r[0]]) == row_count

    def test_rewind(self):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(self.dd_sess.id)), 'wb') as outp:
                outp.write(inp.read())
        initializeSession(self.dd_sess.id)
        initializeModel(self.dd_sess.id)
        dedupeRaw(self.dd_sess.id)

        self.session.refresh(self.dd_sess)
        assert self.dd_sess.status == 'entity map updated'
        
        bulkMarkClusters(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        assert self.dd_sess.status == 'canon clustered'
        
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                self.engine.execute('delete from work_table')
                c.get('/rewind/?session_id={0}&step=first&threshold=0.75'.format(self.dd_sess.id))
                reDedupeRaw(self.dd_sess.id, threshold=0.75)
                self.session.refresh(self.dd_sess)
                assert self.dd_sess.status == 'entity map updated'

    def test_bulk_training(self):
        self.dd_sess.training_data = None
        self.session.add(self.dd_sess)
        self.session.commit()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_sessions'] = [self.dd_sess.id]
                    sess['session_id'] = self.dd_sess.id
                rv = c.post('/add-bulk-training/', data={
                            'input_file': (open(join(fixtures_path, 
                                'training_data.json'),'rb'), 
                                'training_data.json')}, follow_redirects=True)
                self.session.refresh(self.dd_sess)
                assert self.dd_sess.training_data is not None

