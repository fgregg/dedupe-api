if __name__ == "__main__":
    from api.dedupe_utils import DedupeFileIO, WebDeduper
    from api.trainer import readData
    from uuid import uuid4
    import dedupe
    api_key = '03b1511b-0739-44b4-b65c-a559b28f6899'
    sess_key = unicode(uuid4())
    fname = '1405091863.42_csv_example_messy_input.csv'
    training = 'api/upload_data/%s-training.json' % fname
    fpath = 'api/upload_data/%s' % fname
    data = readData(open(fpath).read())
    fields = {
        'Phone': {'type': 'String'}, 
        'Site name': {'type':'String'}, 
        'Address': {'type':'String'}, 
        'Zip':{'type':'String'}
    }
    d = dedupe.Dedupe(fields)
    d.sample(data)
    fileio = DedupeFileIO(fpath,fname)
    deduper = WebDeduper(d, 
        api_key=api_key, 
        session_key=sess_key,
        training_data=training,
        file_io=fileio)
    deduper.dedupe()
