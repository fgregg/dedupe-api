import csv
import re
import os
import json
import time
from itertools import groupby
from dedupe import AsciiDammit
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from cStringIO import StringIO
from collections import defaultdict, OrderedDict
import logging
from datetime import datetime
from api.queue import queuefunc
from api.database import session as db_session, Base
from api.models import DedupeSession, User, entity_map
from operator import itemgetter
from csvkit import convert, sql, table
import xlwt
from openpyxl import Workbook
from openpyxl.cell import get_column_letter
from cPickle import dumps
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, Table
from unidecode import unidecode

try:
    from raven import Client
    client = Client(os.environ['DEDUPE_WORKER_SENTRY_URL'])
except ImportError:
    client = None
except KeyError:
    client = None

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')

def create_session(conn_string):
    engine = create_engine(
        conn_string,
        convert_unicode=True,
        poolclass=NullPool)
    return scoped_session(sessionmaker(bind=engine,
                                       autocommit=False, 
                                       autoflush=False))

class DedupeFileError(Exception): 
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

def make_raw_table(conn_string=None, 
              filename=None,
              session_key=None,
              file_obj=None):
    """ 
    Create a table from incoming tabular data
    """
    file_format = convert.guess_format(filename)
    converted = StringIO(convert.convert(file_obj, file_format))
    csv_table = table.Table.from_csv(converted, name='raw_%s' % session_key)
    fieldnames = csv_table.headers()
    engine = get_engine(conn_string)
    conn = engine.connect()
    trans = conn.begin()
    sql_table = sql.make_table(csv_table, 
        name='raw_%s' % session_key, 
        metadata=Base.metadata)
    sql_table.create(bind=engine, checkfirst=True)
    conn.execute(sql_table.insert(), [dict(zip(fieldnames, row)) for row in csv_table.to_rows()])
    trans.commit()
    conn.close()

def preProcess(column):
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def make_data_d(conn_string, session_key, primary_key=None):
    session = create_session(conn_string)
    engine = get_engine(conn_string)
    table = Table('raw_%s' % session_key, Base.metadata, 
        autoload=True, autoload_with=engine)
    fields = [str(s) for s in table.columns.keys()]
    rows = []
    for row in session.query(table).all():
        rows.append({k: unicode(v) for k,v in zip(fields, row)})
    data_d = {}
    for i, row in enumerate(rows):
        clean_row = [(k, preProcess(v)) for (k,v) in row.items()]
        data_d[i] = dedupe.core.frozendict(clean_row)
    return data_d, fields

def get_engine(conn_string):
    return create_engine(
        conn_string,
        convert_unicode=True,
        poolclass=NullPool)

def writeEntityMap(clustered_dupes, session_key, conn_string, data_d):
    """ 
    Write entity map table
    """
    dt = entity_map('data_%s' % session_key, Base.metadata)
    engine = get_engine(conn_string)
    dt.create(bind=engine, checkfirst=True)
    rows = []
    for cluster_id, cluster in enumerate(clustered_dupes):
        id_set, confidence_score = cluster
        cluster_list = [{'row_id': c, 'row': data_d[c]} for c in id_set]
        for member in cluster_list:
            m = {
                'group_id': cluster_id,
                'confidence': float(confidence_score),
                'record_id': member['row_id'],
                'clustered': False,
            }
            rows.append(m)
    conn = engine.contextual_connect()
    conn.execute(dt.insert(), rows)
    
class WebDeduper(object):
    
    def __init__(self, deduper,
            conn_string=None,
            data_d=None, 
            recall_weight=2,
            session_key=None,
            api_key=None):
        self.deduper = deduper
        self.data_d = data_d
        self.recall_weight = float(recall_weight)
        self.conn_string = conn_string
        self.session_key = session_key
        self.db_session = create_session(conn_string)
        self.dd_session = self.db_session.query(DedupeSession).get(session_key)
        self.training_data = StringIO(self.dd_session.training_data)
        self.api_key = api_key
        self.field_defs = json.loads(self.dd_session.field_defs)
        # Will need to figure out static dedupe, maybe
        self.deduper.readTraining(self.training_data)
        self.deduper.train()
        settings_file_obj = StringIO()
        self.deduper.writeSettings(settings_file_obj)
        self.dd_session.settings_file = settings_file_obj.getvalue()
        self.db_session.add(self.dd_session)
        self.db_session.commit()

    def dedupe(self):
        threshold = self.deduper.threshold(self.data_d, recall_weight=self.recall_weight)
        clustered_dupes = self.deduper.match(self.data_d, threshold)
        writeEntityMap(clustered_dupes, self.session_key, self.conn_string, self.data_d)
        return 'ok'

@queuefunc
def retrain(session_key, conn_string):
    db_session = create_session(conn_string)
    sess = db_session.query(DedupeSession).get(session_key)
    field_defs = json.loads(sess.field_defs)
    training = json.loads(sess.training_data)
    d = dedupe.Dedupe(field_defs)
    d.sample()
    d.readTraining(training)
    d.train()
    return None

@queuefunc
def dedupeit(**kwargs):
    data_d, _ = make_data_d(kwargs['conn_string'], kwargs['session_key'])
    d = dedupe.Dedupe(kwargs['field_defs'], kwargs['data_sample'])
    deduper = WebDeduper(d, 
        data_d=data_d,
        conn_string=kwargs['conn_string'],
        session_key=kwargs['session_key'],
        api_key=kwargs['api_key'])
    files = deduper.dedupe()
    del d
    return files

@queuefunc
def static_dedupeit(**kwargs):
    d = dedupe.StaticDedupe(kwargs['settings_path'])
    file_io = DedupeFileIO(kwargs['file_path'], kwargs['filename'])
    deduper = WebDeduper(d, 
        file_io=file_io,
        recall_weight=kwargs['recall_weight'])
    files = deduper.dedupe()
    del d
    return files
