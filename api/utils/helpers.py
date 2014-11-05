import re
import dedupe
from dedupe.core import frozendict
from api.database import app_session, worker_session
from sqlalchemy import Table, MetaData, distinct, and_, func
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

def clusterGen(result_set, fields):
    lset = set
    block_id = None
    records = []
    for row in result_set:
        row = dict(zip(fields, row))
        if row['block_id'] != block_id:
            if records:
                yield records
            block_id = row['block_id']
            records = []
        smaller_ids = row['smaller_ids']
        if smaller_ids:
            smaller_ids = lset(smaller_ids.split(','))
        else:
            smaller_ids = lset([])
        records.append((row['record_id'], row, smaller_ids))
    if records:
        yield records

def makeDataDict(session_id, fields=None, sample=False):
    session = worker_session
    engine = session.bind
    metadata = MetaData()
    table_name = 'processed_%s' % session_id
    table = Table(table_name, metadata, 
        autoload=True, autoload_with=engine)
    if not fields:
        fields = [unicode(s) for s in table.columns.keys()]
    try:
        primary_key = [p.name for p in table.primary_key][0]
    except IndexError:
        # need to figure out what to do in this case
        raise
    result = {}
    cols = [getattr(table.c, f) for f in fields]
    cols.append(getattr(table.c, primary_key))
    curs = session.query(*cols)
    count = curs.count()
    print 'count %s' % count
    # Going to limit the size of this to half a million rows for the moment
    # Seems like this tends to take up a ton of RAM
    if count >= 500000:
        curs = curs.order_by(func.random()).limit(500000)
    if sample:
        result = dict((i, dedupe.frozendict(zip(fields, row))) 
                            for i, row in enumerate(curs))
    else:
        for row in curs:
            result[int(getattr(row, primary_key))] = dedupe.frozendict(zip(fields, row))
    return result

def getDistinct(field_name, session_id):
    engine = app_session.bind
    metadata = MetaData()
    table = Table('raw_%s' % session_id, metadata,
        autoload=True, autoload_with=engine)
    q = app_session.query(distinct(getattr(table.c, field_name)))
    distinct_values = [preProcess(unicode(v[0])) for v in q.all()]
    return distinct_values
