import json
from os.path import join, abspath, dirname
from flask import request, session
from sqlalchemy import text
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw, bulkMarkClusters, bulkMarkCanonClusters
from tests import DedupeAPITestCase
from api.utils.helpers import slugify

import logging
logging.getLogger('dedupe').setLevel(logging.WARNING)

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class ReviewTest(DedupeAPITestCase):
    ''' 
    Test the review module
    '''
    def test_session_review(self):
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get('/session-review/?session_id=' + self.dd_sess.id)
                assert "var mark_cluster_url = '/mark-cluster/?session_id=' + session_id;" in rv.data.decode('utf-8')
                rv = c.get('/session-review/?session_id=' + self.dd_sess.id + '&second_review=true')
                assert "var mark_cluster_url = '/mark-canon-cluster/?session_id=' + session_id;" in rv.data.decode('utf-8')
    
    def review_wrapper(self, canonical=False):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'r') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(self.dd_sess.id)), 'w') as outp:
                raw_fieldnames = next(inp)
                inp.seek(0)
                outp.write(inp.read())
        fieldnames = [slugify(c).strip('\n') for c in raw_fieldnames.split(',')]
        initializeSession(self.dd_sess.id, fieldnames)
        initializeModel(self.dd_sess.id)
        dedupeRaw(self.dd_sess.id)
        endpoints = {
            'get': '/get-review-cluster/?session_id={0}'.format(self.dd_sess.id),
            'mark_one': '/mark-cluster/?session_id={0}'.format(self.dd_sess.id),
            'mark_all': '/mark-all-clusters/?session_id={0}'.format(self.dd_sess.id),
        }
        entity_table = 'entity_{0}'.format(self.dd_sess.id)
        if canonical:
            bulkMarkClusters(self.dd_sess.id, user=self.user.name)
            endpoints = {
                'get': '/get-canon-review-cluster/?session_id={0}'.format(self.dd_sess.id),
                'mark_one': '/mark-canon-cluster/?session_id={0}'.format(self.dd_sess.id),
                'mark_all': '/mark-all-canon-clusters/?session_id={0}'.format(self.dd_sess.id),
            }
            entity_table = 'entity_{0}_cr'.format(self.dd_sess.id)
        with self.app.test_request_context():
            self.login()
            with self.client as c:
                rv = c.get(endpoints['get'])
                self.session.refresh(self.dd_sess)
                json_resp = json.loads(rv.data.decode('utf-8'))
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
                json_resp_2 = json.loads(rv.data.decode('utf-8'))
                assert json_resp_2['entity_id'] != json_resp['entity_id']

                matches = ','.join([str(r['record_id']) for r in \
                                    json_resp['objects']])
                params = '&match_ids={0}&entity_id={1}'\
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

                distinct = ','.join([str(r['record_id']) for r in \
                                     json_resp_2['objects']])
                params = '&distinct_ids={0}&entity_id={1}'\
                    .format(distinct, json_resp_2['entity_id'])
                rv = c.get('{0}{1}'.format(endpoints['mark_one'] ,params))
                sel = 'select record_id from "{0}"'\
                    .format(entity_table)
                with self.engine.begin() as conn:
                    rows = list(conn.execute(sel))
                rows = [r[0] for r in rows]
                assert set([r['record_id'] for r in json_resp_2['objects']])\
                    .isdisjoint(set(rows))

                if not canonical:
                    bulkMarkClusters(self.dd_sess.id, user=self.user.name)
                else:
                    bulkMarkCanonClusters(self.dd_sess.id, user=self.user.name)
                sel = 'select clustered from "entity_{0}"'\
                    .format(self.dd_sess.id)
                with self.engine.begin() as conn:
                    rows = list(conn.execute(sel))
                assert list(set([r[0] for r in rows])) == [True]

    def test_review_clusters(self):
        self.review_wrapper()
    
    def test_review_canon_clusters(self):
        self.review_wrapper(canonical=True)
