if __name__ == "__main__":
    import sys
    import json
    import os
    from api.dedupe_utils import DedupeFileIO, WebDeduper, create_session
    from api.models import DedupeSession, User
    from api.database import session as db_session
    from api.trainer import readData
    from uuid import uuid4
    import dedupe
    from cStringIO import StringIO
    from cPickle import dumps, loads
    from sqlalchemy import MetaData, Table, create_engine
    from sqlalchemy.pool import NullPool
    
    DB_CONN = os.environ['DEDUPE_CONN']
    api_key = '44dd0042-d76e-4330-a243-c3c6e533316b'

    try:
        sess_key = sys.argv[1]
        use_settings = True
    except IndexError:
        use_settings = False

    if not use_settings:
        sess_key = unicode(uuid4())
        fname = 'Candidates.csv'
        training = 'training.json'
        data = readData(open(fname).read())
        fields = {
            'FullName': {'type': 'String'}, 
            'FullAddress': {'type':'String'}, 
            'OfficeName': {'type':'String'}, 
        }
        user = db_session.query(User).get(api_key)
        dd_session = DedupeSession(
            user=user,
            name='csv_messy_test.csv',
            id=sess_key,
            training_data=open(training, 'rb').read(),
            field_defs=json.dumps(fields))
        db_session.add(dd_session)
        db_session.commit()
        d = dedupe.Dedupe(fields)
        d.sample(data)
        fileio = DedupeFileIO(
            conn_string=DB_CONN,
            session_key=sess_key,
            filename=fname,
            file_obj=open(fname, 'rb'))
        deduper = WebDeduper(d, 
            api_key=api_key, 
            session_key=sess_key,
            file_io=fileio)
        deduper.dedupe()
    else:
        dd_session = db_session.query(DedupeSession).get(sess_key)
    engine = create_engine(
        DB_CONN,
        convert_unicode=True,
        poolclass=NullPool)
    metadata = MetaData()
    data_table = Table('%s_data' % sess_key, 
        metadata, autoload=True, autoload_with=engine)
    data = db_session.query(data_table).all()
    data_d = {}
    for c in data:
        data_d[c.id] = loads(c.blob)
    print 'SESSION ID %s' % dd_session.id
    match_blob = {'blob': {
        'FullName': 'Pat Quinn',
        'OfficeName': 'Governor',
        'FullAddress': '',
    }}
    sf = StringIO(dd_session.settings_file)
    d = dedupe.StaticGazetteer(sf)
    linked = d.match(match_blob, data_d, threshold=0, n_matches=20)
    ids = []
    if linked:
        for l in linked[0]:
            id_set, confidence = l
            ids.extend([i for i in id_set if i not in ids])
    them = db_session.query(data_table).filter(data_table.c.id.in_(ids)).all()
    print [loads(t.blob)['Address'] for t in them]
    print len(linked)
