import csv
import re
import os
import json
import time
from itertools import groupby
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from cStringIO import StringIO
from collections import defaultdict, OrderedDict
import logging
from datetime import datetime
from api.queue import queuefunc
from api.database import session as db_session, Base
from api.models import DedupeSession, User, entity_map, block_map_table
from api.app_config import DB_CONN
from operator import itemgetter
from csvkit import convert, sql, table
from csvkit.unicsv import UnicodeCSVDictReader
import xlwt
from openpyxl import Workbook
from openpyxl.cell import get_column_letter
from cPickle import dumps
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, Table, Column, Integer, Float, Boolean, \
    String, func
from unidecode import unidecode
from sqlalchemy.exc import NoSuchTableError, ProgrammingError
from sqlalchemy.ext.declarative import declarative_base

try:
    import MySQLdb.cursors as mysql_cursors
except ImportError:
    mysql_cursors = None

try:
    from raven import Client
    client = Client(os.environ['DEDUPE_WORKER_SENTRY_URL'])
except ImportError:
    client = None
except KeyError:
    client = None

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')

def create_session(conn_string):
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
    fieldnames = converted.next().strip('\r\n').split(',')
    converted.seek(0)
    cols = []
    for field in fieldnames:
        cols.append(Column(field, String))
    engine = get_engine(conn_string)
    conn = engine.connect()
    trans = conn.begin()
    sql_table = Table('raw_%s' % session_key, Base.metadata, *cols)
    sql_table.append_column(Column('record_id', Integer, primary_key=True))
    sql_table.create(bind=engine, checkfirst=True)
    reader = UnicodeCSVDictReader(converted)
    for row in reader:
        conn.execute(sql_table.insert(), **row)
    trans.commit()
    conn.close()

def preProcess(column):
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def get_engine(conn_string):
    if conn_string.startswith('mysql'):
        conn_args = {'connect_args': {'cursorclass': mysql_cursors.SSCursor}}
    elif conn_string.startswith('postgresql'):
        conn_args = {'server_side_cursors': True}
    return create_engine(
        conn_string,
        convert_unicode=True,
        poolclass=NullPool,
        **conn_args)

def writeEntityMap(clustered_dupes, session_key, conn_string, data_d):
    """ 
    Write entity map table
    """
    dt = entity_map('entity_%s' % session_key, Base.metadata)
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

def writeBlockingMap(conn_string, session_key, block_data):
    bkm = block_map_table('block_%s' % session_key, Base.metadata)
    engine = get_engine(conn_string)
    bkm.create(bind=engine, checkfirst=True)
    conn = engine.contextual_connect()
    insert_data = []
    for key, record_id in block_data:
        insert_data.append({'block_key': key, 'record_id': record_id})
    conn.execute(bkm.insert(), insert_data)

class WebDeduper(object):
    
    def __init__(self, deduper,
            conn_string=None,
            recall_weight=2,
            session_key=None):
        self.deduper = deduper
        self.recall_weight = float(recall_weight)
        self.conn_string = conn_string
        self.session_key = session_key
        self.db_session = create_session(conn_string)
        self.dd_session = self.db_session.query(DedupeSession).get(session_key)
        self.training_data = StringIO(self.dd_session.training_data)
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
        data_d = make_data_d(self.dd_session.conn_string, 
            self.dd_session.id, table_name=self.dd_session.table_name)
        threshold = self.deduper.threshold(data_d, recall_weight=self.recall_weight)
        clustered_dupes = self.deduper.match(data_d, threshold)
        writeEntityMap(clustered_dupes, self.session_key, self.conn_string, data_d)
        dd_tuples = ((k,v) for k,v in data_d.items())
        block_data = self.deduper.blocker(dd_tuples)
        writeBlockingMap(self.conn_string, self.session_key, block_data)
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
    d = dedupe.Dedupe(kwargs['field_defs'], kwargs['data_sample'])
    app_session = create_session(DB_CONN)
    dd_session = app_session.query(DedupeSession).get(kwargs['session_key'])
    deduper = WebDeduper(d, 
        conn_string=DB_CONN,
        session_key=dd_session.id)
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

def make_data_d(conn_string, session_key, primary_key=None, table_name=None):
    session = create_session(conn_string)
    engine = session.bind
    if not table_name:
        table_name = 'raw_%s' % session_key
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
    data_d = {}
    for row in rows:
        clean_row = [(k, preProcess(v)) for (k,v) in row.items()]
        data_d[row[primary_key]] = dedupe.core.frozendict(clean_row)
    return data_d

@queuefunc
def get_sample(conn_string,
                session_key, 
                primary_key=None, 
                table_name=None,
                sample_size=None):
    session = create_session(conn_string)
    engine = session.bind
    if not table_name:
        table_name = 'raw_%s' % session_key
    table = Table(table_name, Base.metadata, 
        autoload=True, autoload_with=engine)
    if not primary_key:
        try:
            primary_key = [p.name for p in table.primary_key][0]
        except IndexError:
            # need to figure out what to do in this case
            print 'no primary key'
    fields = [str(s) for s in table.columns.keys()]
    temp_d = {}
    random_pairs = dedupe.randomPairs(100000, 500000)
    data_rows = session.query(table).limit(100000).all()
    for i, row in enumerate(data_rows):
        d_row = {k: unicode(v) for (k,v) in zip(fields, row)}
        clean_row = [(k, preProcess(v)) for (k,v) in d_row.items()]
        temp_d[i] = dedupe.core.frozendict(clean_row)
    pair_sample = [(temp_d[k1], temp_d[k2])
                    for k1, k2 in random_pairs]
    return pair_sample, fields

def get_or_create_canon_table(session_id):
    cols = []
    app_session = create_session(DB_CONN)
    app_engine = app_session.bind
    try:
        canon_table = Table('canon_%s' % session_id, Base.metadata,
            autoload=True, autoload_with=app_engine, extend_existing=True)
    except NoSuchTableError:
        dd_sess = app_session.query(DedupeSession).get(session_id)
        raw_engine = get_engine(dd_sess.conn_string)
        raw_base = declarative_base()
        raw_table = Table('raw_%s' % session_id, raw_base.metadata,
            autoload=True, autoload_with=raw_engine)
        for col in raw_table.columns:
            kwargs = {}
            if col.type == Integer:
                kwargs['default'] = 0
            if col.type == Float:
                kwargs['default'] = 0.0
            if col.type == Boolean:
                kwargs['default'] = None
            cols.append(Column(col.name, col.type, **kwargs))
        canon_table = Table('canon_%s' % session_id, Base.metadata,
            *cols, extend_existing=True)
        canon_table.append_column(Column('canon_record_id', Integer, primary_key=True))
        canon_table.create(bind=app_engine)
    return canon_table

@queuefunc
def make_canonical_table(session_id):
    return 'yay'
