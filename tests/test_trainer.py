import json
import cPickle
import dedupe
from os.path import join, abspath, dirname
from flask import request, session
from api.utils.helpers import slugify
from api.utils.delayed_tasks import initializeSession
from api.utils.db_functions import writeRawTable
from csvkit.unicsv import UnicodeCSVReader, UnicodeCSVWriter
from tests import DedupeAPITestCase

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class TrainerTest(DedupeAPITestCase):
    ''' 
    Test the training module
    '''
    
    def test_upload(self):
        self.session.delete(self.dd_sess)
        self.session.commit()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['session_id'] = self.dd_sess.id
                rv = c.post('/upload/', data={
                            'input_file': (open(join(fixtures_path, 
                                'csv_example_messy_input.csv'),'rb'), 
                                'csv_example_messy_input.csv'),
                            'name': 'Test Session'})
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
   
    def init_session(self):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as f:
            reader = UnicodeCSVReader(f)
            fieldnames = reader.next()
            with open('/tmp/{0}_raw.csv'.format(self.dd_sess.id), 'wb') as outp:
                writer = UnicodeCSVWriter(outp)
                writer.writerow(fieldnames)
                writer.writerows(reader)
        initializeSession(self.dd_sess.id)
        return fieldnames

    def test_select_fields(self):
        fieldnames = self.init_session()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['fieldnames'] = fieldnames
                rv = c.get('/select-fields/?session_id=' + self.dd_sess.id)
                for field in fieldnames:
                    assert slugify(field) in rv.data

    def test_select_fields_sid(self):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as inp:
            with open('/tmp/example.csv', 'wb') as outp:
                outp.write(inp.read())
        fieldnames = writeRawTable(session_id=self.dd_sess.id, file_path='/tmp/example.csv')
        fieldnames = [slugify(unicode(f)) for f in fieldnames]
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get('/select-fields/?session_id=' + self.dd_sess.id)
                assert set(session['fieldnames']) == set(fieldnames)
    
    def test_select_fields_post(self):
        fieldnames = self.init_session()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['fieldnames'] = fieldnames
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
                assert 'You must select at least one field to compare on.' in rv.data

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
                    assert field in rv.data

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
                    'phone_missing': 'on',
                    'address_type': 'Address', 
                    'address_missing': 'on',
                    'site_name_type': 'String', 
                    'zip_type': ['ShortString', 'Exact'], 
                    'zip_missing': 'on',
                }
                rv = c.post('/select-field-types/', data=post_data)
                self.session.refresh(self.dd_sess)
                fds_str = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
                fds = sorted(json.loads(fds_str))
                expected = sorted(json.loads(self.dd_sess.field_defs))
                for idx, f in enumerate(fds):
                    assert set(f.items()) == set(expected[idx].items())

    def test_training_run_processing(self):
        fds = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        self.dd_sess.field_defs = fds
        self.session.add(self.dd_sess)
        self.session.commit()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['user_id'] = self.user.id
                    sess['session_id'] = self.dd_sess.id
                rv = c.get('/training-run/')
                assert 'still working on finishing up processing your upload' in rv.data
    
    def test_training_run_redirect(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    del sess['session_id']
                rv = c.get('/training-run/', follow_redirects=False)
                rd_path = rv.location.split('http://localhost')[1]
                rd_path = rd_path.split('?')[0]
                assert rd_path == '/'
    
    def test_training_run_qparam(self):
        fds = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        self.dd_sess.field_defs = fds
        self.session.add(self.dd_sess)
        self.session.commit()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    del sess['session_id']
                rv = c.get('/training-run/?session_id=' + self.dd_sess.id)
                assert 'still working on finishing up processing your upload' in rv.data
    
    def test_training_run_qparam(self):
        fds = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        sample = open(join(fixtures_path, 'sample.dump'), 'rb').read()
        self.dd_sess.field_defs = fds
        self.dd_sess.sample = sample
        self.session.add(self.dd_sess)
        self.session.commit()
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get('/training-run/')
                assert 'still working on finishing up processing your upload' not in rv.data

    def test_get_pair(self):
        fds = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        sample = open(join(fixtures_path, 'sample.dump'), 'rb').read()
        deduper = dedupe.Dedupe(json.loads(fds), cPickle.loads(sample))
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['deduper'] = deduper
                rv = c.get('/get-pair/')
                assert set(['left', 'right', 'field']) == set(json.loads(rv.data)[0].keys())
                assert session.get('current_pair') is not None
    
    def test_mark_pair(self):
        fds = open(join(fixtures_path, 'field_defs.json'), 'rb').read()
        sample = open(join(fixtures_path, 'sample.dump'), 'rb').read()
        self.dd_sess.training_data = None
        self.session.add(self.dd_sess)
        self.session.commit()
        deduper = dedupe.Dedupe(json.loads(fds), cPickle.loads(sample))
        record_pair = deduper.uncertainPairs()[0]
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                with c.session_transaction() as sess:
                    sess['session_id'] = self.dd_sess.id
                    sess['deduper'] = deduper
                    sess['current_pair'] = record_pair
                    sess['counter'] = {'yes':0,'no':0,'unsure':0}
                rv = c.get('/mark-pair/?action=yes')
                counter = json.loads(rv.data)['counter']
                assert counter['yes'] == 1
                with c.session_transaction() as sess:
                    sess['counter'] = counter
                self.session.refresh(self.dd_sess)
                
                rv = c.get('/mark-pair/?action=no')
                counter.update(json.loads(rv.data)['counter'])
                assert counter['yes'] == 1
                assert counter['no'] == 1
                with c.session_transaction() as sess:
                    sess['counter'] = counter
                
                rv = c.get('/mark-pair/?action=unsure')
                assert json.loads(rv.data)['counter']['yes'] == 1
                assert json.loads(rv.data)['counter']['no'] == 1
                assert json.loads(rv.data)['counter']['unsure'] == 1
                
                rv = c.get('/mark-pair/?action=finish')
                assert session.get('deduper_key') is not None
                assert json.loads(rv.data)['finished'] == True
