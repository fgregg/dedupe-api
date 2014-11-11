import dedupe
import json
from cStringIO import StringIO
from cPickle import loads
from api.database import worker_session
from api.models import DedupeSession
from api.utils.helpers import clusterGen, windowed_query
from api.utils.db_functions import writeBlockingMap, initializeEntityMap
from api.utils.delayed_tasks import dedupeCanon
from sqlalchemy import Table, MetaData, func, Column, String
from sqlalchemy.sql import label
from csvkit.unicsv import UnicodeCSVWriter, UnicodeCSVReader
from uuid import uuid4

if __name__ == '__main__':
    
    metadata = MetaData()
    engine = worker_session.bind
    
    sess = worker_session.query(DedupeSession).first()
    
   #settings = StringIO(sess.settings_file)

   #d = dedupe.StaticDedupe(settings)
   #proc_table = Table('processed_%s' % sess.id, metadata,
   #    autoload=True, autoload_with=engine, keep_existing=True)
   #
   #for field in d.blocker.tfidf_fields:
   #    fd = worker_session.query(proc_table.c.record_id, 
   #        getattr(proc_table.c, field))
   #    field_data = (row for row in fd.yield_per(50000))
   #    d.blocker.tfIdfBlock(field_data, field)
   #    del field_data
   #cols = [getattr(proc_table.c, f) for f in fields]
   #cols.append(proc_table.c.record_id)
   #proc_records = worker_session.query(*cols)
   #full_data = ((getattr(row, 'record_id'), dict(zip(fields, row))) \
   #    for row in proc_records.yield_per(50000))
   #blocked_data = d.blocker(full_data)
   #
   #writeBlockingMap(sess.id, blocked_data)

   #small_cov = Table('small_cov_%s' % sess.id, metadata,
   #    autoload=True, autoload_with=engine, keep_existing=True)
   #entity = Table('entity_%s' % sess.id, metadata,
   #    autoload=True, autoload_with=engine, keep_existing=True)

    dedupeCanon(sess.id)



   #rows = worker_session.query(small_cov, proc_table)\
   #    .join(proc_table, small_cov.c.record_id == proc_table.c.record_id)\
   #    .outerjoin(entity, small_cov.c.record_id == entity.c.record_id)\
   #    .filter(entity.c.target_record_id == None)
   #fields = small_cov.columns.keys() + proc_table.columns.keys()
   #clustered_dupes = d.matchBlocks(
   #    clusterGen(windowed_query(rows, small_cov.c.block_id, 50000), fields), 
   #    threshold=0.75
   #)
   
   # Write out clusters to CSV
   
   #with open('/tmp/cluster_%s.csv' % sess.id, 'wb') as f:
   #    writer = UnicodeCSVWriter(f)
   #    for cluster, score in clustered_dupes:
   #        # leaving out low confidence clusters
   #        # This is a non-scientificly proven threshold
   #        cluster_ids = ';'.join([unicode(i) for i in cluster])
   #        writer.writerow([cluster_ids, score])
   #
   #with open('/tmp/cluster_%s.csv' % sess.id, 'rb') as f:
   #    reader = UnicodeCSVReader(f)
   #    longest = 0
   #    for row in reader:
   #        ids, score = row
   #        length = len(ids.split(';'))
   #        if length > longest:
   #            longest = length
   #    print longest
    
   #makeCanonTable(session_id)
   #review_count = writeEntityMap(clustered_dupes, session_id)
