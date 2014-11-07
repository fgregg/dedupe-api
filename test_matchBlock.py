import dedupe
import json
from cStringIO import StringIO
from cPickle import loads
from api.database import worker_session
from api.models import DedupeSession
from api.utils.helpers import clusterGen
from api.utils.db_functions import writeEntityMap, makeCanonTable
from sqlalchemy import Table, MetaData, and_, func

import psycopg2

def column_windows(session, column, windowsize):
    def int_for_range(start_id, end_id):
        if end_id:
            return and_(
                column>=start_id,
                column<end_id
            )
        else:
            return column>=start_id

    q = session.query(
                column, 
                func.row_number().\
                        over(order_by=column).\
                        label('rownum')
                ).\
                from_self(column)
    if windowsize > 1:
        q = q.filter("rownum %% %d=1" % windowsize)

    intervals = [id for id, in q]

    while intervals:
        start = intervals.pop(0)
        if intervals:
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)

def windowed_query(q, column, windowsize):
    
    for whereclause in column_windows(q.session, 
                                        column, windowsize):
        for row in q.filter(whereclause).order_by(column):
            yield row

if __name__ == '__main__':
    
    engine = worker_session.bind
    sess = worker_session.query(DedupeSession)\
        .filter(DedupeSession.name == 'contributions.csv')\
        .first()

    settings = StringIO(sess.settings_file)

    d = dedupe.StaticDedupe(settings)
    
    metadata = MetaData()

    small_cov = Table('small_cov_%s' % sess.id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    proc = Table('processed_%s' % sess.id, metadata,
        autoload=True, autoload_with=engine, keep_existing=True)
    rows = worker_session.query(small_cov, proc)\
        .join(proc, small_cov.c.record_id == proc.c.record_id)
    fields = small_cov.columns.keys() + proc.columns.keys()
    clustered_dupes = d.matchBlocks(
        clusterGen(windowed_query(rows, small_cov.c.block_id, 50000), fields), 
        threshold=0.75
    )
    makeCanonTable(session_id)
    review_count = writeEntityMap(clustered_dupes, session_id)
