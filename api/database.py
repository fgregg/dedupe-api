import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
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

app_engine = create_engine(DB_CONN, convert_unicode=True)
worker_engine = create_engine(DB_CONN, convert_unicode=True)

app_session = scoped_session(sessionmaker(bind=app_engine, 
                                      autocommit=False, 
                                      autoflush=False))

worker_session = scoped_session(sessionmaker(bind=worker_engine,
                                          autocommit=False,
                                          autoflush=False))

Base = declarative_base()

def init_db():
    import api.models
    Base.metadata.create_all(bind=app_engine)
    for role in DEFAULT_ROLES:
        app_session.add(api.models.Role(**role))
    
    try:
        app_session.commit()
    except IntegrityError, e:
        app_session.rollback()
        print e.message

    admin = app_session.query(api.models.Role)\
        .filter(api.models.Role.name == 'admin').first()
    if DEFAULT_USER:
        name = DEFAULT_USER['user']['name']
        email = DEFAULT_USER['user']['email']
        password = DEFAULT_USER['user']['password']
        user = api.models.User(name, password, email)
        g_name = DEFAULT_USER['group']['name']
        description = DEFAULT_USER['group']['description']
        group = api.models.Group(name=g_name, description=description)
        app_session.add(group)
        app_session.commit()
        user.groups = [group]
        user.roles = [admin]
        app_session.add(user)
        app_session.commit()
