from flask_sqlalchemy import SQLAlchemy
from flask.ext.security import SQLAlchemyUserDatastore
from flask.ext.security import UserMixin

db = SQLAlchemy()

def canon_table(name, metadata):
    table = db.Table(name, metadata, 
        db.Column('row_id', db.Integer, primary_key=True), 
        db.Column('row_blob', db.LargeBinary),
        extend_existing=True
    )
    return table



class DedupeSession(db.Model):
    uuid = db.Column(db.String, primary_key=True)
    name = db.Column(db.String, nullable=False)
    user_id = db.Column(db.String, db.ForeignKey('api_user.api_key'))
    user = db.relationship('ApiUser', backref=db.backref('sessions'))
    training_data = db.Column(db.LargeBinary)
    settings_file = db.Column(db.LargeBinary)
    field_defs = db.Column(db.LargeBinary)

    def __repr__(self):
        return '<DedupeSession %r (%r)>' % (self.user.name, self.name)

class ApiUser(db.Model):
    api_key = db.Column(db.String, primary_key=True)
    name = db.Column(db.String, nullable=False)
    email = db.Column(db.String, nullable=False)

    def __repr__(self):
        return '<ApiUser %r>' % self.name

roles_users = db.Table('role_users',
    db.Column('user_id', db.Integer(), db.ForeignKey('user.id')),
    db.Column('role_id', db.Integer(), db.ForeignKey('role.id')))

class Role(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True)
    description = db.Column(db.String(255))
    
    def __repr__(self):
        return '<Role %r>' % self.name

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    email = db.Column(db.String, nullable=False)
    active = db.Column(db.Boolean())
    password = db.Column(db.String)
    roles = db.relationship('Role', secondary=roles_users,
        backref=db.backref('users', lazy='dynamic'))
    
    def __repr__(self):
        return '<User %r>' % self.name

user_datastore = SQLAlchemyUserDatastore(db, User, Role)
