import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from api.app_config import DB_CONN

DEFAULT_ROLES = [
    {
        'name': 'admin', 
        'description': 'Administrator',
    },
    {
        'name': 'reviewer',
        'description': 'Reviewer'
    }
]

engine = create_engine(DB_CONN, convert_unicode=True)

session = scoped_session(sessionmaker(bind=engine, 
                                      autocommit=False, 
                                      autoflush=False))

Base = declarative_base()
Base.query = session.query_property()

def init_db():
    import api.models
    Base.metadata.create_all(bind=engine)
    for role in DEFAULT_ROLES:
        session.add(api.models.Role(**role))
    session.commit()
