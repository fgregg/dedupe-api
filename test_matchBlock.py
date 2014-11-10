import dedupe
import json
from cStringIO import StringIO
from cPickle import loads
from api.database import worker_session
from api.models import DedupeSession
from api.utils.helpers import clusterGen, windowed_query
from api.utils.db_functions import makeCanonTable, \
    writeBlockingMap, initializeEntityMap
from sqlalchemy import Table, MetaData
from csvkit.unicsv import UnicodeCSVWriter, UnicodeCSVReader
from uuid import uuid4

if __name__ == '__main__':
    
    metadata = MetaData()
    engine = worker_session.bind
    
    sess = worker_session.query(DedupeSession)\
        .filter(DedupeSession.name == 'contributions.csv')\
        .first()
    
   #fields = [f['field'] for f in json.loads(sess.field_defs)]

    settings = StringIO(sess.settings_file)

    d = dedupe.StaticDedupe(settings)
    proc_table = Table('processed_%s' % sess.id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    
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
    entity = Table('entity_%s' % sess.id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
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
    
    with open('/tmp/cluster_%s.csv' % sess.id, 'rb') as f:
        reader = UnicodeCSVReader(f)
        for row in reader:
            cluster_ids, score = row
            ids = cluster_ids.split(';')
            new_ent = unicode(uuid4())
            existing = worker_session.query(entity.c.record_id)\
                .filter(entity.c.record_id.in_(ids))\
                .all()
            if existing:
                existing_ids = [unicode(i[0]) for i in existing]
                new_ids = list(set(ids).difference(set(existing_ids)))
                upd = {
                    'entity_id': new_ent,
                    'clustered': False,
                    'confidence': score,
                }
                engine.execute(entity.update()\
                    .where(entity.c.record_id.in_(existing_ids))\
                    .values(**upd))
                if new_ids:
                    king = existing_ids[0]
                    vals = []
                    for i in new_ids:
                        d = {
                            'entity_id': new_ent,
                            'record_id': i,
                            'target_record_id': king,
                            'clustered': False,
                            'checked_out': False,
                            'confidence': score
                        }
                        vals.append(d)
                    engine.execute(entity.insert(), vals)
            else:
                king = ids.pop(0)
                vals = [{
                    'entity_id': new_ent,
                    'record_id': king,
                    'target_record_id': None,
                    'clustered': False,
                    'checked_out': False,
                    'confidence': score
                }]
                for i in ids:
                    d = {
                        'entity_id': new_ent,
                        'record_id': i,
                        'target_record_id': king,
                        'clustered': False,
                        'checked_out': False,
                        'confidence': score
                    }
                    vals.append(d)
                engine.execute(entity.insert(), vals)




   #makeCanonTable(session_id)
   #review_count = writeEntityMap(clustered_dupes, session_id)
