from api.database import init_db
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
    curs.execute('CREATE DATABASE {0}'.format(dbname))
    curs.close()
    conn.close()
    init_db(eng=engine, sess=session)
    user = User('bob', 'bobspw', 'bob@bob.com')
    group = session.query(Group).first()
    role = session.query(Role)\
        .filter(Role.name == 'reviewer').first()
    user.groups = [group]
    user.roles = [role]
    session.add(user)
    session.commit()

def tearDownPackage():
    session.close_all()
    engine.dispose()
    conn = psycopg2.connect(conn_str)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor()
    curs.execute('DROP DATABASE {0}'.format(dbname))
    curs.close()
    conn.close()
    
