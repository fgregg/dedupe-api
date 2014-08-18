from sqlalchemy import String, Integer, LargeBinary, ForeignKey, Boolean, \
    Column, Table, Float
from sqlalchemy.orm import relationship, backref, synonym
from api.database import Base, engine, session
from flask_bcrypt import Bcrypt
from uuid import uuid4

bcrypt = Bcrypt()

def entity_map(name, metadata, pk_type=Integer):
    table = Table(name, metadata, 
        Column('record_id', pk_type, primary_key=True),
        Column('group_id', Integer), 
        Column('confidence', Float(precision=50)),
        Column('clustered', Boolean),
        extend_existing=True
    )
    return table

def block_map_table(name, metadata, pk_type=Integer):
    table = Table(name, metadata,
        Column('block_key', String(200)),
        Column('record_id', pk_type),
        extend_existing=True
    )
    return table

def get_uuid():
    return unicode(uuid4())

class DedupeSession(Base):
    __tablename__ = 'dedupe_session'
    id = Column(String, default=get_uuid, primary_key=True)
    name = Column(String, nullable=False)
    training_data = Column(LargeBinary)
    settings_file = Column(LargeBinary)
    field_defs = Column(LargeBinary)
    conn_string = Column(String)
    table_name = Column(String)

    def __repr__(self):
        return '<DedupeSession %r >' % (self.name)

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
    id = Column(String(36), default=get_uuid, primary_key=True)
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

    def __init__(self, name, password, email, roles):
        self.name = name
        self.password = password
        self.email = email
        self.roles = roles

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
