import unittest
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
from api.database import init_engine, app_session, worker_session
from api.models import DedupeSession, User
from sqlalchemy.orm import sessionmaker, scoped_session
from api.utils.delayed_tasks import initializeSession, initializeModel, \
    dedupeRaw, dedupeCanon, getMatchingReady
from api.utils.helpers import STATUS_LIST
from sqlalchemy import text

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class DelayedTest(unittest.TestCase):
    ''' 
    Test the matching module
    '''
    pass
    
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(config='tests.test_config')
        cls.engine = init_engine(cls.app.config['DB_CONN'])
        cls.session = scoped_session(sessionmaker(bind=cls.engine, 
                                              autocommit=False, 
                                              autoflush=False))
        cls.user = cls.session.query(User).first()
        cls.group = cls.user.groups[0]
    
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
        with open(join(fixtures_path, 'csv_example_messy_input.csv'), 'rb') as inp:
            with open(join('/tmp/{0}_raw.csv'.format(self.dd_sess.id)), 'wb') as outp:
                outp.write(inp.read())
        initializeSession(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        rows = []
        with self.engine.begin() as conn:
            rows = list(conn.execute('select count(*) from "raw_{0}"'\
                .format(self.dd_sess.id)))
        self.get_table_names()
        assert self.dd_sess.status == STATUS_LIST[1]
        assert self.dd_sess.record_count == int(rows[0][0])
        assert 'raw_{0}'.format(self.dd_sess.id) in self.table_names

        initializeModel(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        self.get_table_names()
        assert self.dd_sess.status == STATUS_LIST[2]
        assert self.dd_sess.sample is not None
        assert 'processed_{0}'.format(self.dd_sess.id) in self.table_names
        assert 'entity_{0}'.format(self.dd_sess.id) in self.table_names

        dedupeRaw(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        self.get_table_names()
        assert self.dd_sess.status == STATUS_LIST[3]
        assert self.dd_sess.entity_count > 0
        assert self.dd_sess.review_count > 0
        assert 'small_cov_{0}'.format(self.dd_sess.id) in self.table_names

        dedupeCanon(self.dd_sess.id)
        self.session.refresh(self.dd_sess)
        self.get_table_names()
        assert self.dd_sess.status == STATUS_LIST[4]
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
