import os
import json
from flask import Flask
from redis import Redis
from api.auth import auth, login_manager, csrf
from api.models import bcrypt
from api.trainer import trainer
from api.matching import matching
from api.admin import admin
from api.review import review
from api.redis_session import RedisSessionInterface

try:
    from raven.contrib.flask import Sentry
    from api.app_config import SENTRY_DSN
    sentry = Sentry(dsn=SENTRY_DSN)
except ImportError:
    sentry = None
except KeyError:
    sentry = None

def create_app():
    app = Flask(__name__)
    app.config.from_object('api.app_config')
    redis = Redis()
    app.session_interface = RedisSessionInterface(redis=redis)
    if sentry:
        sentry.init_app(app)
    csrf.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth.login"
    app.register_blueprint(admin)
    app.register_blueprint(review)
    app.register_blueprint(auth)
    app.register_blueprint(trainer)
    app.register_blueprint(matching)
    return app

