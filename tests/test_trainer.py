import json
import pickle
import dedupe
import csv
from os.path import join, abspath, dirname, exists
from flask import request, session
from api.utils.helpers import slugify
from api.utils.db_functions import writeRawTable, writeProcessedTable
from api.utils.delayed_tasks import initializeSession, initializeModel
from api.database import app_session as db_session
from csvkit.unicsv import UnicodeCSVReader, UnicodeCSVWriter
from tests import DedupeAPITestCase
from operator import itemgetter
from api.trainer import getTrainingPair, getTrainingCounts

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

import logging
logging.getLogger('dedupe').setLevel(logging.WARNING)

class TrainerTest(DedupeAPITestCase):
    ''' 
    Test the training module
    '''
    
    @property
    def fieldnames(self):
        with open(join(fixtures_path, 
            'csv_example_messy_input.csv'),'r') as inp:
            reader = csv.reader(inp)
            fieldnames = [slugify(f) for f in next(reader)]
        return fieldnames

    def test_upload(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.post('/upload/', data={
                            'input_file': (open(join(fixtures_path, 
                                'csv_example_messy_input.csv'),'rb'), 
                                'csv_example_messy_input.csv'),
                            'name': 'Test Session'})
                sess_id = json.loads(rv.data.decode('utf-8'))['session_id']
                assert exists('/tmp/{0}_raw.csv'.format(sess_id))
                rv = c.get('/select-fields/')
                assert set(session['fieldnames']) == set(self.fieldnames)

    def test_clear_session(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['fieldnames'] = ['this']
                    sess['session_name'] = 'test'
                    sess['training_data'] = {'test': 'thing'}
                    sess['user_id'] = self.user.id
                rv = c.get('/new-session/')
                assert 'fieldnames' not in session.keys()
                assert 'session_name' not in session.keys()
                assert 'training_data' not in session.keys()
   
    def init_session(self):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as inp:
            with open('/tmp/example.csv', 'wb') as outp:
                outp.write(inp.read())
        writeRawTable(session_id=self.dd_sess.id, 
                      file_path='/tmp/example.csv', 
                      fieldnames=self.fieldnames)

    def test_select_fields_no_sid(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get('/select-fields/')
                assert rv.location == 'http://localhost/'

    def test_select_fields_sid(self):
        self.init_session()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    if sess.get('fieldnames'):
                        del sess['fieldnames']
                rv = c.get('/select-fields/?session_id=' + self.dd_sess.id)
                assert set(session['fieldnames']) == set(self.fieldnames)
    
    def test_select_fields_post(self):
        self.init_session()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['fieldnames'] = self.fieldnames
                    sess['session_id'] = self.dd_sess.id
                post_data = {
                      'phone': ['on'],
                      'email': ['on'],
                      'site_name': ['on'],
                      'zip': ['on'],
                    }
                rv = c.post('/select-fields/', data=post_data, follow_redirects=True)
                assert set(session['field_list']) == set(post_data.keys())

    def test_select_fields_nothing(self):
        self.init_session()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['session_id'] = self.dd_sess.id
                rv = c.post('/select-fields/', data={}, follow_redirects=True)
                assert 'You must select at least one field to compare on.' in rv.data.decode('utf-8')

    def test_select_field_type(self):
        field_list = [
            'phone',
            'address',
            'site_name',
            'zip',
        ]
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['field_list'] = field_list
                    sess['user_id'] = self.user.id
                    sess['session_id'] = self.dd_sess.id
                rv = c.get('/select-field-types/')
                for field in field_list:
                    assert field in rv.data.decode('utf-8')

    def test_select_field_type_post(self):
        field_list = [
            'phone',
            'address',
            'site_name',
            'zip',
        ]
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['field_list'] = field_list
                    sess['user_id'] = self.user.id
                    sess['session_id'] = self.dd_sess.id
                post_data = {
                    'phone_type': 'ShortString', 
                    'address_type': 'Address', 
                    'site_name_type': 'String', 
                    'zip_type': ['ShortString', 'Exact'], 
                }
                rv = c.post('/select-field-types/', data=post_data)
                db_session.refresh(self.dd_sess)
                fds_str = open(join(fixtures_path, 'field_defs.json'), 'r').read()
                fds = sorted(json.loads(fds_str), key=itemgetter('type'))
                expected = sorted(json.loads(self.dd_sess.field_defs.decode('utf-8')), key=itemgetter('type'))
                for idx, f in enumerate(fds):
                    e = expected[idx]
                    assert set([(f['field'], f['type'],)]) == \
                           set([(e['field'], e['type'],)])

    def test_training_run_redirect(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    if sess.get('session_id'):
                        del sess['session_id']
                rv = c.get('/training-run/', follow_redirects=False)
                rd_path = rv.location.split('http://localhost')[1]
                rd_path = rd_path.split('?')[0]
                assert rd_path == '/'
    
    def test_training_run(self):
        self.init_session()
        initializeModel(self.dd_sess.id)
        db_session.refresh(self.dd_sess)
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                    sess['session_id'] = self.dd_sess.id
                decisions = {'yes': [], 'no': [], 'unsure': []}
                for decision in decisions.keys():
                    for i in range(10):
                        rv = c.get('/training-run/', follow_redirects=False)
                        current_pair = session['current_pair']
                        training_ids = ','.join([str(r['record_id']) for r in current_pair])
                        data = {
                            'training_ids': training_ids, 
                            'decision': decision
                        }
                        rv = c.post('/training-run/', data=data, follow_redirects=True)
                        decisions[decision].append(training_ids)
                deduper = session['deduper']
                for decision, record_ids in decisions.items():
                    for pair in record_ids:
                        _, _, p_type, _, _ = getTrainingPair(self.dd_sess.id, deduper, pair)
                        if decision == 'yes':
                            assert p_type == 'match'
                        elif decision == 'no':
                            assert p_type == 'distinct'
                        else:
                            assert p_type == 'unsure'
