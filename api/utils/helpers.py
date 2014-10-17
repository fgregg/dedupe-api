import re
import dedupe
from api.database import app_session, worker_session
from sqlalchemy import Table, MetaData
from unidecode import unidecode
from itertools import count

def preProcess(column):
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def makeDataDict(session_key, primary_key=None, table_name=None, worker=False):
    if worker:
        session = worker_session
    else:
        session = app_session
    engine = session.bind
    if not table_name:
        table_name = 'raw_%s' % session_key
    metadata = MetaData()
    table = Table(table_name, metadata, 
        autoload=True, autoload_with=engine)
    fields = [str(s) for s in table.columns.keys()]
    if not primary_key:
        try:
            primary_key = [p.name for p in table.primary_key][0]
        except IndexError:
            # need to figure out what to do in this case
            print 'no primary key'
    result = {}
    for row in session.query(table).yield_per(100):
        d = {k: preProcess(unicode(v)) for k,v in zip(fields, row)}
        result.update({int(d[primary_key]): d})
    return result
