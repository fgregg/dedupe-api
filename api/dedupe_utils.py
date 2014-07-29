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
from api.models import DedupeSession, User, data_table
from operator import itemgetter
from csvkit import convert
import xlwt
from openpyxl import Workbook
from openpyxl.cell import get_column_letter
from cPickle import dumps
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool

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

class DedupeFileIO(object):
    
    def __init__(self, 
                 conn_string=None, 
                 session_key=None,
                 filename=None,
                 file_obj=None):
        self.conn_string = conn_string
        file_type = convert.guess_format(filename)
        self.converted = convert.convert(file_obj, file_type)
        self.data_table = data_table('%s_data' % session_key, Base.metadata)
        self.readData()
    
    @property
    def conn(self):
        return self.engine.contextual_connect()

    @property
    def engine(self):
        return create_engine(
            self.conn_string,
            convert_unicode=True,
            poolclass=NullPool)

    def writeDB(self, clustered_dupes):
        """ 
        Write clustered dupes with confidence score to DB table
        """
        self.data_table.create(bind=self.engine, checkfirst=True)
        rows = []
        for cluster_id, cluster in enumerate(clustered_dupes):
            id_set, confidence_score = cluster
            cluster_list = [{'row_id': c, 'row': self.data_d[c]} for c in id_set]
            for member in cluster_list:
                m = {
                    'group_id': cluster_id,
                    'confidence': confidence_score,
                    'blob': dumps(member['row']),
                    'id': member['row_id']
                }
                rows.append(m)
        self.conn.execute(self.data_table.insert(), rows)
    
    def preProcess(self, column):
        column = AsciiDammit.asciiDammit(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        return column
 
    def readData(self):
        self.data_d = {}
        f = StringIO(self.converted)
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = [(k, self.preProcess(v)) for (k,v) in row.items()]
            row_id = i
            self.data_d[row_id] = dedupe.core.frozendict(clean_row)

class WebDeduper(object):
    
    def __init__(self, deduper,
            file_io=None, 
            recall_weight=2,
            session_key=None,
            api_key=None):
        self.file_io = file_io
        self.deduper = deduper
        self.recall_weight = float(recall_weight)
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

    @property
    def db_session(self):
        return scoped_session(sessionmaker(bind=self.engine,
                                           autocommit=False, 
                                           autoflush=False))

    def dedupe(self):
        threshold = self.deduper.threshold(self.file_io.data_d, recall_weight=self.recall_weight)
        clustered_dupes = self.deduper.match(self.file_io.data_d, threshold)
        self.file_io.writeDB(clustered_dupes)
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
    deduper = WebDeduper(d, 
        file_io=kwargs['file_io'],
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
