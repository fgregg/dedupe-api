from api.database import init_db, app_session, worker_session
from api.models import User, Group, Role
from test_config import DB_CONFIG, DB_CONN
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

host = DB_CONFIG['host']
port = DB_CONFIG['port']
dbname = DB_CONFIG['name']
conn_str = 'user=postgres host={host} port={port}'\
    .format(host=host, port=port)

engine = create_engine(DB_CONN, 
                       convert_unicode=True, 
                       server_side_cursors=True)

session = scoped_session(sessionmaker(bind=engine, 
                                      autocommit=False, 
                                      autoflush=False))

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
        cstr = 'user=postgres host={host} port={port} dbname=dedupe_test'\
            .format(host=host, port=port)
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
