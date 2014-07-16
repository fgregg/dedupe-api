import csv
import re
import os
import json
import time
from dedupe import AsciiDammit
import dedupe
from dedupe.serializer import _to_json, dedupe_decoder
from cStringIO import StringIO
from collections import defaultdict, OrderedDict
import logging
from datetime import datetime
from api.queue import queuefunc
from api.database import DedupeSession, ApiUser, canon_table
from operator import itemgetter
from csvkit import convert
import xlwt
from openpyxl import Workbook
from openpyxl.cell import get_column_letter
from cPickle import dumps

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool

db_path = os.path.abspath(os.path.dirname(__file__))

def create_session():
    engine = create_engine(
        'sqlite:///%s/dedupe.db' % db_path,
        convert_unicode=True,
        poolclass=NullPool)
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = Session()
    session._model_changes = {}
    return session

try:
    from raven import Client
    client = Client(os.environ['DEDUPE_WORKER_SENTRY_URL'])
except ImportError:
    client = None
except KeyError:
    client = None

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')

class DedupeFileError(Exception): 
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class DedupeFileIO(object):
    """ 
    Take an uploaded file, figure out what type it is, convert it to csv
    then save it back as the same format.
    """
    def __init__(self, file_path, filename):
        now = datetime.now().isoformat()
        self.file_path = file_path
        self.filename = filename
        self.file_type = convert.guess_format(self.filename)
        if self.file_type not in ['xls', 'csv', 'xlsx']:
            if client:
                client.captureMessage(' %s Unsupported Format: %s, (%s)' % (now, self.file_type, self.filename))
            raise DedupeFileError('%s is not a supported format' % self.file_type)
        try:
            self.converted = convert.convert(open(self.file_path, 'rb'), self.file_type)
        except UnicodeDecodeError:
            if client:
                client.captureException()
            raise DedupeFileError('We had a problem with the file you uploaded. \
                    This might be related to encoding or the file name having the wrong file extension.')
        self.line_count = self.converted.count('\n')
        if client:
            client.captureMessage(' %s Format: %s, Line Count: %s' % (now, self.file_type, self.line_count))

    def prepare(self, clustered_dupes):
        self.clustered_dupes = clustered_dupes
        self.cluster_count = self._prepareResults()
        self._prepareUniqueResults()

    def _prepareResults(self):
        """ 
        Prepare deduplicated file for writing to various formats with
        duplicates clustered. 
        """
        cluster_membership = {}
        for cluster_id, cluster in enumerate(self.clustered_dupes):
            id_set, confidence_score = cluster
            for record_id in id_set:
                cluster_membership[record_id] = cluster_id

        unique_record_id = cluster_id + 1
        
        f = StringIO(self.converted)
        reader = csv.reader(f)
 
        heading_row = reader.next()
        heading_row.insert(0, 'Group ID')
    
        rows = []

        for row_id, row in enumerate(reader):
            if row_id in cluster_membership:
                cluster_id = cluster_membership[row_id]
            else:
                cluster_id = unique_record_id
                unique_record_id += 1
            row.insert(0, cluster_id)
            rows.append(row)
        rows = sorted(rows, key=itemgetter(0))
        rows.insert(0, heading_row)
        self.clustered_rows = []
        for row in rows:
            d = OrderedDict()
            for k,v in zip(heading_row, row):
                d[k] = v
            self.clustered_rows.append(d)
        f.close()
        return unique_record_id
 
    def _prepareUniqueResults(self):
        """ """
        cluster_membership = {}
        for (cluster_id, cluster) in enumerate(self.clustered_dupes):
            for record_id in cluster:
                cluster_membership[record_id] = cluster_id
 
        f = StringIO(self.converted)
        reader = csv.reader(f)
 
        rows = [reader.next()]
        seen_clusters = set()
        for row_id, row in enumerate(reader):
            if row_id in cluster_membership: 
                cluster_id = cluster_membership[row_id]
                if cluster_id not in seen_clusters:
                    rows.append(row)
                    seen_clusters.add(cluster_id)
            else:
                rows.append(row)
        self.unique_rows = []
        for row in rows:
            d = OrderedDict()
            for k,v in zip(rows[0], row):
                d[k] = AsciiDammit.asciiDammit(v)
            self.unique_rows.append(d)
        f.close()
        return self.unique_rows
    
    def writeDB(self, session_key):
        # Create session specific table and write unique rows to it
        path = 'sqlite:///%s/dedupe.db' % db_path
        engine = create_engine(
            path,
            convert_unicode=True,
            poolclass=NullPool)
        metadata = MetaData()
        session_canon = canon_table('%s_canon' % session_key, metadata)
        session_canon.create(bind=engine, checkfirst=True)
        rows = [{'row_id': c_id, 'row_blob': dumps(c)} for c_id, c in enumerate(self.unique_rows)]
        conn = engine.contextual_connect()
        conn.execute(session_canon.insert(), rows)
        return None

class WebDeduper(object):
    
    def __init__(self, deduper,
            file_io=None, 
            recall_weight=2,
            session_key=None,
            api_key=None):
        self.file_io = file_io
        self.data_d = self.readData()
        self.deduper = deduper
        self.recall_weight = float(recall_weight)
        self.db_session = create_session()
        self.dd_session = self.db_session.query(DedupeSession).get(session_key)
        self.training_data = StringIO(self.dd_session.training_data)
        self.session_key = session_key
        self.api_key = api_key
        self.field_defs = json.loads(self.dd_session.field_defs)
        # Will need to figure out static dedupe, maybe
        self.deduper.readTraining(self.training_data)
        self.deduper.train()
        settings_file_obj = StringIO()
        self.deduper.writeSettings(settings_file_obj)
        settings_file_obj.seek(0)
        self.dd_session.settings_file = settings_file_obj.getvalue()
        self.db_session.add(self.dd_session)
        self.db_session.commit()


    def dedupe(self):
        threshold = self.deduper.threshold(self.data_d, recall_weight=self.recall_weight)
        clustered_dupes = self.deduper.match(self.data_d, threshold)
        self.file_io.prepare(clustered_dupes)
        self.file_io.writeDB(self.session_key)
        return 'ok'
    
    def preProcess(self, column):
        column = AsciiDammit.asciiDammit(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        return column
 
    def readData(self):
        data = {}
        f = StringIO(self.file_io.converted)
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = [(k, self.preProcess(v)) for (k,v) in row.items()]
            row_id = i
            data[row_id] = dedupe.core.frozendict(clean_row)
        return data

@queuefunc
def retrain(session_key):
    db_session = create_session()
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
