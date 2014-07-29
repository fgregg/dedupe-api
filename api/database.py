from flask_sqlalchemy import SQLAlchemy
from flask.ext.security import SQLAlchemyUserDatastore
from flask.ext.security import UserMixin
from uuid import uuid4

db = SQLAlchemy()

def data_table(name, metadata):
    table = db.Table(name, metadata, 
        db.Column('id', db.Integer, primary_key=True),
        db.Column('group_id', db.Integer), 
        db.Column('blob', db.LargeBinary),
        db.Column('confidence', db.Float(precision=64)),
        extend_existing=True
    )
    return table

class DedupeSession(db.Model):
    id = db.Column(db.String, primary_key=True)
    name = db.Column(db.String, nullable=False)
    user_id = db.Column(db.String(36), db.ForeignKey('user.id'))
    user = db.relationship('User', backref=db.backref('sessions'))
    training_data = db.Column(db.LargeBinary)
    settings_file = db.Column(db.LargeBinary)
    field_defs = db.Column(db.LargeBinary)

    def __repr__(self):
        return '<DedupeSession %r (%r)>' % (self.user.name, self.name)

roles_users = db.Table('role_users',
    db.Column('user_id', db.String(36), db.ForeignKey('user.id')),
    db.Column('role_id', db.Integer(), db.ForeignKey('role.id')))

class Role(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True)
    description = db.Column(db.String(255))
    
    def __repr__(self):
        return '<Role %r>' % self.name

class User(db.Model, UserMixin):
    id = db.Column(db.String(36), default=unicode(uuid4()), primary_key=True)
    name = db.Column(db.String, nullable=False)
    email = db.Column(db.String, nullable=False)
    active = db.Column(db.Boolean())
    password = db.Column(db.String)
    roles = db.relationship('Role', secondary=roles_users,
        backref=db.backref('users', lazy='dynamic'))
    
    def __repr__(self):
        return '<User %r>' % self.name

user_datastore = SQLAlchemyUserDatastore(db, User, Role)
