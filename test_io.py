if __name__ == "__main__":
    import json
    from api.dedupe_utils import DedupeFileIO, WebDeduper, create_session
    from api.database import DedupeSession, ApiUser
    from api.trainer import readData
    from uuid import uuid4
    import dedupe
    api_key = '99cf602a-21cd-44ae-aca0-7da0d7d3408e'
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
    db_session = create_session()
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
    fileio = DedupeFileIO(fpath,fname)
    deduper = WebDeduper(d, 
        api_key=api_key, 
        session_key=sess_key,
        file_io=fileio)
    deduper.dedupe()
