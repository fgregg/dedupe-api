import os
import json
from flask import Flask
from flask.ext.security import Security
from api.endpoints import endpoints
from api.auth import auth, security
from api.database import db, user_datastore

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.environ['FLASK_KEY']
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dedupe.db'
    app.config['SECURITY_LOGIN_URL'] = '/login/'
    app.config['SECURITY_LOGOUT_URL'] = '/logout/'
    db.init_app(app)
    security.init_app(app, user_datastore)
    app.register_blueprint(endpoints)
    app.register_blueprint(auth)
    if os.environ.get('DUMMY_USER'):
        @app.before_first_request
        def create_user():
            db.create_all()
            dummy_user = json.loads(os.environ['DUMMY_USER'])
            user_datastore.create_user(**dummy_user)
            db.session.commit()
    return app

