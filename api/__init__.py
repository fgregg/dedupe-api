import os
import json
from flask import Flask
from redis import Redis
from api.endpoints import endpoints
from api.auth import auth, login_manager, csrf
from api.models import bcrypt
from api.trainer import trainer
from api.manager import manager
from api.redis_session import RedisSessionInterface

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
DB_CONN = os.environ['DEDUPE_CONN']

def create_app():
    app = Flask(__name__)
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024
    app.config['SECRET_KEY'] = os.environ['FLASK_KEY']
    app.config['REDIS_QUEUE_KEY'] = 'deduper'
    app.config['DB_CONN'] = DB_CONN
    redis = Redis()
    app.session_interface = RedisSessionInterface(redis=redis)
    try:
        from raven.contrib.flask import Sentry
        app.config['SENTRY_DSN'] = os.environ['DEDUPE_WEB_SENTRY_URL']
        sentry = Sentry(app)
    except ImportError:
        pass
    except KeyError:
        pass
    csrf.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth.login"
    app.register_blueprint(endpoints)
    app.register_blueprint(auth)
    app.register_blueprint(trainer)
    app.register_blueprint(manager)
    return app

