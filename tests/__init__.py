import unittest
from uuid import uuid4
from api.database import init_db, app_session, worker_session, \
    init_engine, DEFAULT_USER
from api.models import User, Group, Role, DedupeSession
from api.utils.helpers import STATUS_LIST
from test_config import DB_CONFIG, DB_CONN
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from api import create_app
from os.path import join, abspath, dirname

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

host = DB_CONFIG['host']
port = DB_CONFIG['port']
dbname = DB_CONFIG['name']
dbuser = DB_CONFIG['user']
conn_str = 'user={dbuser} host={host} port={port} dbname=postgres'\
    .format(dbuser=dbuser, host=host, port=port)

engine = create_engine(DB_CONN, 
                       convert_unicode=True, 
                       server_side_cursors=True)

session = scoped_session(sessionmaker(bind=engine, 
                                      autocommit=False, 
                                      autoflush=False))

class DedupeAPITestCase(unittest.TestCase):
    
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
                        filename='test_filename.csv',
                        name='Test Session',
                        group=self.group,
                        status=STATUS_LIST[0]['machine_name'],
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

def setUpPackage():
    ''' 
    Do database setup
    '''
    conn = psycopg2.connect(conn_str)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor()
    try:
        curs.execute('CREATE DATABASE {0}'.format(dbname))
    except psycopg2.ProgrammingError:
        cstr = 'user={dbuser} host={host} port={port} dbname={dbname}'\
            .format(dbuser=dbuser, host=host, port=port, dbname=dbname)
        with psycopg2.connect(cstr) as conn:
            with conn.cursor() as curs:
                curs.execute('''
                                DROP TABLE 
                                  dedupe_user, 
                                  dedupe_group, 
                                  group_users, 
                                  dedupe_role, 
                                  dedupe_session, 
                                  role_users
                              ''')
    curs.close()
    conn.close()
    init_db(eng=engine, sess=session)
    user = User('bob', 'bobspw', 'bob@bob.com')
    group = session.query(Group).first()
    role = session.query(Role)\
        .filter(Role.name == 'reviewer').first()
    user.groups = [group]
    user.roles = [role]
    dummy_group = Group(name='dummy', description='dummy')
    session.add(dummy_group)
    session.add(user)
    session.commit()

def tearDownPackage():
    session.close_all()
    worker_session.close_all()
    engine.dispose()
    worker_session.bind.dispose()
