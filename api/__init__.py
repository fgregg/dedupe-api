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

def create_app():
    app = Flask(__name__)
    app.config.from_object('api.app_config')
    redis = Redis()
    app.session_interface = RedisSessionInterface(redis=redis)
    if app.config.get('SENTRY_DSN'):
        try:
            from raven.contrib.flask import Sentry
            sentry = Sentry(app)
        except ImportError:
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

