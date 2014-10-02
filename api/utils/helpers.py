import re
import dedupe
from sqlalchemy import create_engine, Table
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from unidecode import unidecode
from itertools import count

try:
    import MySQLdb.cursors as mysql_cursors
except ImportError:
    mysql_cursors = None

def createSession(conn_string):
    if conn_string.startswith('mysql'):
        conn_args = {'connect_args': {'cursorclass': mysql_cursors.SSCursor}}
    elif conn_string.startswith('postgresql'):
        conn_args = {'server_side_cursors': True}
    engine = create_engine(
        conn_string,
        convert_unicode=True,
        poolclass=NullPool,
        **conn_args)
    return scoped_session(sessionmaker(bind=engine,
                                       autocommit=False, 
                                       autoflush=False))

def preProcess(column):
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def getEngine(conn_string):
    if conn_string.startswith('mysql'):
        conn_args = {'connect_args': {'cursorclass': mysql_cursors.SSCursor}}
    elif conn_string.startswith('postgresql'):
        conn_args = {'server_side_cursors': True}
    return create_engine(
        conn_string,
        convert_unicode=True,
        poolclass=NullPool,
        **conn_args)

def iterDataDict(rows, primary_key=None):
    data_d = {}
    c = count(start=1)
    for row in rows:
        clean_row = [(k, preProcess(v)) for (k,v) in row.items()]
        if primary_key:
            data_d[row[primary_key]] = dedupe.core.frozendict(clean_row)
        else:
            data_d[c.next()] = dedupe.core.frozendict(clean_row)
    return data_d

def makeDataDict(conn_string, session_key, primary_key=None, table_name=None):
    session = createSession(conn_string)
    engine = session.bind
    if not table_name:
        table_name = 'raw_%s' % session_key
    Base = declarative_base()
    table = Table(table_name, Base.metadata, 
        autoload=True, autoload_with=engine)
    fields = [str(s) for s in table.columns.keys()]
    if not primary_key:
        try:
            primary_key = [p.name for p in table.primary_key][0]
        except IndexError:
            # need to figure out what to do in this case
            print 'no primary key'
    rows = []
    for row in session.query(table).all():
        rows.append({k: unicode(v) for k,v in zip(fields, row)})
    data_d = iterDataDict(rows, primary_key=primary_key)
    session.close()
    return data_d
