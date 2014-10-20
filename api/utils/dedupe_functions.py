import os
import time
import json
from api.models import entity_map, DedupeSession, block_map_table
from api.utils.db_functions import writeEntityMap, writeBlockingMap
from api.database import worker_session
from api.utils.helpers import makeDataDict
from csvkit import convert
from csvkit.unicsv import UnicodeCSVDictReader
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from hashlib import md5
from cStringIO import StringIO

class DedupeFileError(Exception): 
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

class WebDeduper(object):
    
    def __init__(self, deduper,
            recall_weight=1,
            session_key=None):
        self.deduper = deduper
        self.recall_weight = float(recall_weight)
        self.session_key = session_key
        self.dd_session = worker_session.query(DedupeSession).get(session_key)
        self.training_data = StringIO(self.dd_session.training_data)
        # Will need to figure out static dedupe, maybe
        self.deduper.readTraining(self.training_data)
        self.deduper.train()
        settings_file_obj = StringIO()
        self.deduper.writeSettings(settings_file_obj)
        self.dd_session.settings_file = settings_file_obj.getvalue()
        worker_session.add(self.dd_session)
        worker_session.commit()


    def dedupe(self):
        data_d = makeDataDict(self.dd_session.id, table_name=self.dd_session.table_name, worker=True)
        threshold = self.deduper.threshold(data_d, recall_weight=self.recall_weight)
        clustered_dupes = self.deduper.match(data_d, threshold)
        writeEntityMap(clustered_dupes, self.session_key, data_d)
        dd_tuples = ((k,v) for k,v in data_d.items())
        block_data = self.deduper.blocker(dd_tuples)
        writeBlockingMap(self.session_key, block_data)
        return 'ok'
