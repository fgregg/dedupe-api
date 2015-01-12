import unittest
import json
import cPickle
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api.models import User, Group
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw
from sqlalchemy import text
from cStringIO import StringIO
from csvkit.unicsv import UnicodeCSVReader
from tests import DedupeAPITestCase

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class AdminTest(DedupeAPITestCase):
    ''' 
    Test the admin module
    '''

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
                c.get('/mark-all-clusters/{0}/'.format(self.dd_sess.id))
                rv = c.get('/dump-entity-map/' + self.dd_sess.id + '/')
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
                s = StringIO(rv.data)
                reader = UnicodeCSVReader(s)
                reader.next()
                assert len([r for r in list(reader) if r[0]]) == row_count
