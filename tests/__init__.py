import unittest
import json
from uuid import uuid4
from api.database import init_db, app_session as db_session, \
    init_engine, DEFAULT_USER
from api.models import User, Group, Role, DedupeSession
from api.utils.helpers import STATUS_LIST
from api.utils.db_functions import saveTraining
from .test_config import DB_CONFIG, DB_CONN
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from api import create_app
from os.path import join, abspath, dirname

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

host = DB_CONFIG['host']
port = DB_CONFIG['port']
dbname = DB_CONFIG['name']
dbuser = DB_CONFIG['user']
conn_str = 'user={dbuser} host={host} port={port} dbname=postgres'\
    .format(dbuser=dbuser, host=host, port=port)


class DedupeAPITestCase(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.engine = init_engine(DB_CONN)
        cls.app = create_app(config='tests.test_config')
        cls.client = cls.app.test_client()
   
        cls.user = db_session.query(User).first()
        cls.group = cls.user.groups[0]
        cls.user_pw = DEFAULT_USER['user']['password']

    def setUp(self):
        self.field_defs = open(join(fixtures_path, 'field_defs.json'), 'r').read()
        settings = open(join(fixtures_path, 'settings_file.dedupe'), 'rb').read()
        self.dd_sess = DedupeSession(
                        id=str(uuid4()), 
                        filename='test_filename.csv',
                        name='Test Session',
                        description='Test Session description',
                        group=self.group,
                        status=STATUS_LIST[0]['machine_name'],
                        settings_file=settings,
                        field_defs=bytes(self.field_defs.encode('utf-8'))
                      )
        db_session.add(self.dd_sess)
        db_session.commit()
        
        training = json.load(open(join(fixtures_path, 'training_data.json'), 'r'))
        saveTraining(self.dd_sess.id, training, self.user.name)

    def tearDown(self):
        try:
            db_session.delete(self.dd_sess)
            db_session.commit()
        except Exception:
            db_session.rollback()

    @classmethod
    def tearDownClass(cls):
        db_session.close()
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
                                DROP TABLE IF EXISTS
                                  dedupe_user, 
                                  dedupe_group, 
                                  group_users, 
                                  dedupe_role, 
                                  dedupe_session, 
                                  role_users,
                                  work_table
                              ''')
    curs.close()
    conn.close()
    engine = init_engine(DB_CONN)
    init_db(eng=engine)
    user = User('bob', 'bobspw', 'bob@bob.com')
    group = db_session.query(Group).first()
    role = db_session.query(Role)\
        .filter(Role.name == 'reviewer').first()
    user.groups = [group]
    user.roles = [role]
    dummy_group = Group(name='dummy', description='dummy')
    db_session.add(dummy_group)
    db_session.add(user)
    db_session.commit()

def tearDownPackage():
    db_session.close_all()
    db_session.bind.dispose()
