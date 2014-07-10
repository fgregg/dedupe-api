import os
import json
from flask import Flask
from flask.ext.security import Security
from api.endpoints import endpoints
from api.auth import auth, security, csrf
from api.database import db, user_datastore
from api.trainer import trainer
from api.redis_session import RedisSessionInterface

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')

def create_app():
    app = Flask(__name__)
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024
    app.config['SECRET_KEY'] = os.environ['FLASK_KEY']
    app.config['REDIS_QUEUE_KEY'] = 'deduper'
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dedupe.db'
    app.config['SECURITY_LOGIN_URL'] = '/login/'
    app.config['SECURITY_LOGOUT_URL'] = '/logout/'
    app.session_interface = RedisSessionInterface()
    try:
        from raven.contrib.flask import Sentry
        app.config['SENTRY_DSN'] = os.environ['DEDUPE_WEB_SENTRY_URL']
        sentry = Sentry(app)
    except ImportError:
        pass
    except KeyError:
        pass
    csrf.init_app(app)
    db.init_app(app)
    security.init_app(app, user_datastore)
    app.register_blueprint(endpoints)
    app.register_blueprint(auth)
    app.register_blueprint(trainer)
    if os.environ.get('DUMMY_USER'):
        @app.before_first_request
        def create_user():
            db.create_all()
            dummy_user = json.loads(os.environ['DUMMY_USER'])
            user_datastore.create_user(**dummy_user)
            db.session.commit()
    return app

