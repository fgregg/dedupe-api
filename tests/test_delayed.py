from os.path import join, abspath, dirname
from flask import request, session
from api import create_app
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw, dedupeCanon, getMatchingReady
from api.utils.helpers import slugify
from sqlalchemy import text
from tests import DedupeAPITestCase
from api.utils.helpers import STATUS_LIST

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

import logging
logging.getLogger('dedupe').setLevel(logging.WARNING)

class DelayedTest(DedupeAPITestCase):
    ''' 
    Test the matching module
    '''

    def get_table_names(self):
        tnames = text(''' 
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :table_schema
              AND table_type = :table_type
        ''')

        with self.engine.begin() as conn:
            table_names = list(conn.execute(tnames, 
                                            table_schema='public', 
                                            table_type='BASE TABLE'))
        self.table_names = [t[0] for t in table_names]

    def test_full_run(self):
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'r') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(self.dd_sess.id)), 'w') as outp:
                raw_fieldnames = next(inp)
                inp.seek(0)
                outp.write(inp.read())
        fieldnames = [slugify(c).strip('\n') for c in raw_fieldnames.split(',')]
        initializeSession(self.dd_sess.id, fieldnames)
        self.session.refresh(self.dd_sess)
        rows = []
        with self.engine.begin() as conn:
            rows = list(conn.execute('select count(*) from "raw_{0}"'\
                .format(self.dd_sess.id)))
        self.get_table_names()
        assert self.dd_sess.status == STATUS_LIST[0]['machine_name']
        assert self.dd_sess.record_count == int(rows[0][0])
        assert 'raw_{0}'.format(self.dd_sess.id) in self.table_names

        initializeModel(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        self.get_table_names()
        assert self.dd_sess.sample is not None
        assert 'processed_{0}'.format(self.dd_sess.id) in self.table_names
        assert 'entity_{0}'.format(self.dd_sess.id) in self.table_names

        dedupeRaw(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        self.get_table_names()
        assert self.dd_sess.entity_count > 0
        assert self.dd_sess.review_count > 0
        assert 'small_cov_{0}'.format(self.dd_sess.id) in self.table_names

        dedupeCanon(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        self.get_table_names()
        assert 'processed_{0}_cr'.format(self.dd_sess.id) in self.table_names
        assert 'entity_{0}_cr'.format(self.dd_sess.id) in self.table_names
        assert 'small_cov_{0}_cr'.format(self.dd_sess.id) in self.table_names

        getMatchingReady(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        self.get_table_names()
        removed_tables = set()
        table_patterns = [
            'entity_{0}_cr',
            'processed_{0}_cr',
            'block_{0}_cr',
            'plural_block_{0}_cr',
            'covered_{0}_cr',
            'plural_key_{0}_cr',
            'small_cov_{0}_cr',
            'cr_{0}',
            'block_{0}',
            'plural_block_{0}',
            'covered_{0}',
            'plural_key_{0}',
        ]
        table_names = [t.format(self.dd_sess.id) for t in table_patterns]
        assert 'match_blocks_{0}'.format(self.dd_sess.id) in self.table_names
        assert set(self.table_names).isdisjoint(removed_tables)
        assert self.dd_sess.gaz_settings_file is not None
