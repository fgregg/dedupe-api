import os
from sqlalchemy import create_engine
from sqlalchemy.orm import create_session, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects import registry
import uuid
import re

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

def castStringArrayAsTuple(value, cur):
    # "value" is the raw string we get from the DB
    # including the brackets ("{}"). This is kind of a 
    # dumb way of doing this but it should work for all
    # of our purposes in dedupe.
    if value == '{}':
        return ()
    last_bracket = value.rfind('}', 1)

    # Handle two dimensional array
    vals = []
    for part in value[1:last_bracket].split('}","{'):
        cleaned = part.replace('}', '')\
                      .replace('{', '')\
                      .replace('"', '')\
                      .replace('\\', '')
        vals.append(tuple(cleaned.split(',')))
    if len(vals) == 1:
        return tuple(vals[0])
    return tuple(vals)

def castIntArrayAsTuple(value, cur):
    if value == '{}':
        return ()
    last_bracket = value.rfind('}', 1)
    cast_vals = []
    for val in value[1:last_bracket].split(','):
        try:
            cast_vals.append(int(val))
        except ValueError:
            cast_vals.append(None)
    return tuple(cast_vals)

def castFloatArrayAsTuple(value, cur):
    if value == '{}':
        return ()
    last_bracket = value.rfind('}', 1)
    cast_vals = []
    for val in value[1:last_bracket].split(','):
        try:
            cast_vals.append(float(val))
        except ValueError:
            cast_vals.append(None)
    return tuple(cast_vals)

class DedupeDialect(PGDialect_psycopg2):

    @classmethod
    def dbapi(cls):
        import psycopg2
        return psycopg2

    @classmethod
    def _psycopg2_extensions(cls):
        from psycopg2 import extensions
        return extensions

    @classmethod
    def _psycopg2_extras(cls):
        from psycopg2 import extras
        return extras

    def on_connect(self):
        extras = self._psycopg2_extras()
        extensions = self._psycopg2_extensions()

        fns = []
        if self.client_encoding is not None:
            def on_connect(conn):
                conn.set_client_encoding(self.client_encoding)
            fns.append(on_connect)

        if self.isolation_level is not None:
            def on_connect(conn):
                self.set_isolation_level(conn, self.isolation_level)
            fns.append(on_connect)

        if self.dbapi and self._json_deserializer:
            def on_connect(conn):
                if self._has_native_json:
                    extras.register_default_json(
                        conn, loads=self._json_deserializer)
                if self._has_native_jsonb:
                    extras.register_default_jsonb(
                        conn, loads=self._json_deserializer)
            fns.append(on_connect)
    
        def on_connect(conn):
            # Register new "type" which effectively overrides all of the ARRAY types
            # we care about. The first arg here is the PostgreSQL datatype object IDs
            # which are registered in it's internal tables.
            # More info here: 
            # http://initd.org/psycopg/docs/advanced.html#type-casting-of-sql-types-into-python-objects
            
            string_array_type = extensions.new_type((1015,1009,), "STRINGARRAY", castStringArrayAsTuple)
            float_array_type = extensions.new_type((1022,), "FLOATARRAY", castFloatArrayAsTuple)
            int_array_type = extensions.new_type((1007,), "INTARRAY", castIntArrayAsTuple)
            extensions.register_type(string_array_type, conn)
            extensions.register_type(float_array_type, conn)
            extensions.register_type(int_array_type, conn)
        fns.append(on_connect)

        if fns:
            def on_connect(conn):
                for fn in fns:
                    fn(conn)
            return on_connect
        else:
            return None

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

    # Register the dialect we created above
    registry.register("postgresql.dedupe", "api.database", "DedupeDialect")

    engine = create_engine(uri, 
                           convert_unicode=True, 
                           server_side_cursors=True)
    return engine

def init_db(sess=None, eng=None):
    import api.models
    if not eng: # pragma: no cover
        eng = engine
    if not sess: # pragma: no cover
        sess = app_session
    Base.metadata.create_all(bind=eng)
    for role in DEFAULT_ROLES:
        sess.add(api.models.Role(**role))
    
    try:
        sess.commit()
    except IntegrityError as e: # pragma: no cover
        sess.rollback()
        print(str(e))

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
