import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from api.app_config import DB_CONN, DEFAULT_USER

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
    admin = session.query(api.models.Role)\
        .filter(api.models.Role.name == 'admin').first()
    if DEFAULT_USER:
        name = DEFAULT_USER['user']['name']
        email = DEFAULT_USER['user']['email']
        password = DEFAULT_USER['user']['password']
        user = api.models.User(name, password, email)
        g_name = DEFAULT_USER['group']['name']
        description = DEFAULT_USER['group']['description']
        group = api.models.Group(name=g_name, description=description)
        session.add(group)
        session.commit()
        user.groups = [group]
        user.roles = [admin]
        session.add(user)
        session.commit()
