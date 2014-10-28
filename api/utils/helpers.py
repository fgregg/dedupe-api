import re
import dedupe
from dedupe.core import frozendict
from api.database import app_session, worker_session
from sqlalchemy import Table, MetaData, distinct
from unidecode import unidecode
from unicodedata import normalize
from itertools import count

def slugify(text, delim=u'_'):
    if text:
        punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.:;]+')
        result = []
        for word in punct_re.split(text.lower()):
            word = normalize('NFKD', word).encode('ascii', 'ignore')
            if word:
                result.append(word)
        return unicode(delim.join(result))
    else:
        return text

def preProcess(column):
    if not column:
        column = u''
    if column == 'None':
        column = u''
    # column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def makeDataDict(session_id, sample=False, worker=False, table_name=None):
    if worker:
        session = worker_session
    else:
        session = app_session
    engine = session.bind
    metadata = MetaData()
    if not table_name:
        table_name = 'processed_%s' % session_id
    table = Table(table_name, metadata, 
        autoload=True, autoload_with=engine)
    fields = [unicode(s) for s in table.columns.keys()]
    try:
        primary_key = [p.name for p in table.primary_key][0]
    except IndexError:
        # need to figure out what to do in this case
        raise
    result = {}
    for idx,row in enumerate(session.query(table).yield_per(100)):
        d = {k: v for k,v in zip(fields, row)}
        if sample:
            result[idx] = d
        else:
            result[int(d[primary_key])] = d
    return result

def getDistinct(field_name, session_id):
    engine = app_session.bind
    metadata = MetaData()
    table = Table('raw_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    q = app_session.query(distinct(getattr(table.c, field_name)))
    distinct_values = [preProcess(unicode(v[0])) for v in q.all()]
    return distinct_values
