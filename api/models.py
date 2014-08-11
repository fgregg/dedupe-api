from sqlalchemy import String, Integer, LargeBinary, ForeignKey, Boolean, \
    Column, Table, Float
from sqlalchemy.orm import relationship, backref, synonym
from api.database import Base, engine, session
from flask_bcrypt import Bcrypt
from uuid import uuid4

bcrypt = Bcrypt()

def data_table(name, metadata):
    table = Table(name, metadata, 
        Column('id', Integer, primary_key=True),
        Column('group_id', Integer), 
        Column('confidence', Float(precision=50)),
        extend_existing=True
    )
    return table

class DBConnection(Base):
    __tablename__ = 'db_connection'
    session_id = Column(Integer, primary_key=True)
    sql_flavor = Column(String(15), nullable=False)
    host = Column(String(12), nullable=False)
    port = Column(Integer, nullable=False)
    user = Column(String, nullable=False)
    password  = Column(String, nullable=False)
    db_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)

    @property
    def conn_string(self):
        parts = (self.sql_flavor, self.user, self.password, 
                 self.host, self.port, self.db_name)
        return '%r://%r:%r@%r:%r/%r' % parts

    def __repr__(self):
        return '<DBConnection %r>' % self.conn_string

class DedupeSession(Base):
    __tablename__ = 'dedupe_session'
    id = Column(String, default=unicode(uuid4()), primary_key=True)
    name = Column(String, nullable=False)
    user_id = Column(String(36), ForeignKey('user.id'))
    user = relationship('User', backref=backref('sessions'))
    training_data = Column(LargeBinary)
    settings_file = Column(LargeBinary)
    field_defs = Column(LargeBinary)

    def __repr__(self):
        return '<DedupeSession %r (%r)>' % (self.user.name, self.name)

roles_users = Table('role_users', Base.metadata,
    Column('user_id', String(36), ForeignKey('user.id')),
    Column('role_id', Integer, ForeignKey('role.id'))
)

class Role(Base):
    __tablename__ = 'role'
    id = Column(Integer, primary_key=True)
    name = Column(String(80), unique=True)
    description = Column(String(255))
    
    def __repr__(self):
        return '<Role %r>' % self.name

class User(Base):
    __tablename__ = 'user'
    id = Column(String(36), default=unicode(uuid4()), primary_key=True)
    name = Column(String, nullable=False, unique=True)
    email = Column(String, nullable=False, unique=True)
    active = Column(Boolean())
    _password = Column('password', String, nullable=False)
    roles = relationship('Role', secondary=roles_users,
        backref=backref('users', lazy='dynamic'))
    
    def __repr__(self):
        return '<User %r>' % self.name

    def _get_password(self):
        return self._password
    
    def _set_password(self, value):
        self._password = bcrypt.generate_password_hash(value)

    password = property(_get_password, _set_password)
    password = synonym('_password', descriptor=password)

    def __init__(self, name, password, email):
        self.name = name
        self.password = password
        self.email = email

    @classmethod
    def get_by_username(cls, name):
        return session.query(cls).filter(cls.name == name).first()

    @classmethod
    def check_password(cls, name, value):
        user = cls.get_by_username(name)
        if not user:
            return False
        return bcrypt.check_password_hash(user.password, value)

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id
