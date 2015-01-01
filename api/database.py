import os
from sqlalchemy import create_engine
from sqlalchemy.orm import create_session, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
import uuid

from api.app_config import DEFAULT_USER

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

engine = None

app_session = scoped_session(lambda: create_session(bind=engine, 
                                      autocommit=False, 
                                      autoflush=False))

worker_session = scoped_session(lambda: create_session(bind=engine,
                                          autocommit=False,
                                          autoflush=False))

Base = declarative_base()

def init_engine(uri):
    global engine
    engine = create_engine(uri, 
                           convert_unicode=True, 
                           server_side_cursors=True)
    return engine

def init_db(sess=None, eng=None):
    import api.models
    if not eng:
        eng = engine
    if not sess:
        sess = app_session
    Base.metadata.create_all(bind=eng)
    for role in DEFAULT_ROLES:
        sess.add(api.models.Role(**role))
    
    try:
        sess.commit()
    except IntegrityError, e:
        sess.rollback()
        print e.message

    admin = sess.query(api.models.Role)\
        .filter(api.models.Role.name == 'admin').first()
    if DEFAULT_USER:
        name = DEFAULT_USER['user']['name']
        email = DEFAULT_USER['user']['email']
        password = DEFAULT_USER['user']['password']
        user = api.models.User(name, password, email)
        g_name = DEFAULT_USER['group']['name']
        description = DEFAULT_USER['group']['description']
        group = api.models.Group(name=g_name, description=description)
        sess.add(group)
        sess.commit()
        user.groups = [group]
        user.roles = [admin]
        sess.add(user)
        sess.commit()
