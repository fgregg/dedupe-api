if __name__ == "__main__":
    import sys
    import json
    import os
    from api.dedupe_utils import DedupeFileIO, WebDeduper, create_session
    from api.database import DedupeSession, ApiUser
    from api.trainer import readData
    from uuid import uuid4
    import dedupe
    from cStringIO import StringIO
    from cPickle import dumps, loads
    from sqlalchemy import MetaData, Table, create_engine
    from sqlalchemy.pool import NullPool
    
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'api'))
    api_key = '29ca95b8-38f9-472f-9307-f82c90182812'
    db_session = create_session()
    
    try:
        sess_key = sys.argv[1]
        use_settings = True
    except IndexError:
        use_settings = False

    if not use_settings:
        sess_key = unicode(uuid4())
        fname = '1405614456.36_csv_example_messy_input.csv'
        training = 'api/upload_data/%s.json' % fname
        fpath = 'api/upload_data/%s' % fname
        data = readData(open(fpath).read())
        fields = {
            'Phone': {'type': 'String'}, 
            'Site name': {'type':'String'}, 
            'Address': {'type':'String'}, 
            'Zip':{'type':'String'}
        }
        user = db_session.query(ApiUser).get(api_key)
        dd_session = DedupeSession(
            user=user,
            name='csv_messy_test.csv',
            uuid=sess_key,
            training_data=open(training, 'rb').read(),
            field_defs=json.dumps(fields))
        db_session.add(dd_session)
        db_session.commit()
        d = dedupe.Dedupe(fields)
        d.sample(data)
        fileio = DedupeFileIO(
            conn_string='sqlite:///%s/dedupe.db' % db_path,
            session_key=sess_key,
            filename=fname,
            file_obj=open(fpath, 'rb'))
        deduper = WebDeduper(d, 
            api_key=api_key, 
            session_key=sess_key,
            file_io=fileio)
        deduper.dedupe()
    else:
        dd_session = db_session.query(DedupeSession).get(sess_key)
    path = 'sqlite:///%s/dedupe.db' % db_path
    engine = create_engine(
        path,
        convert_unicode=True,
        poolclass=NullPool)
    metadata = MetaData()
    data_table = Table('%s_data' % sess_key, 
        metadata, autoload=True, autoload_with=engine)
    data = db_session.query(data_table).all()
    data_d = {}
    for c in data:
        data_d[c.id] = loads(c.blob)
    match_blob = {'blob': {
        'Address': '10001 s woodlawn',
        'Site name': 'board trustees-city colleges of chicago',
        'Phone': '',
        'Zip': '',
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
