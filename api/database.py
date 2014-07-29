import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

conn_string = os.environ['DEDUPE_CONN']

engine = create_engine(conn_string, convert_unicode=True)

session = scoped_session(sessionmaker(bind=engine, 
                                      autocommit=False, 
                                      autoflush=False))

Base = declarative_base()
Base.query = session.query_property()

def init_db():
    import api.models
    Base.metadata.create_all(bind=engine)
