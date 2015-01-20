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
from api.database import init_engine
from api.track_usage import tracker, UserSQLStorage

try: # pragma: no cover
    from raven.contrib.flask import Sentry
    from api.app_config import SENTRY_DSN
    sentry = Sentry(dsn=SENTRY_DSN) 
except ImportError:
    sentry = None
except KeyError: #pragma: no cover
    sentry = None

def create_app(config='api.app_config'):
    app = Flask(__name__)
    app.config.from_object(config)
    engine = init_engine(app.config['DB_CONN'])
    redis = Redis()
    app.session_interface = RedisSessionInterface(redis=redis, 
                                prefix=app.config['REDIS_SESSION_KEY'])
    storage = UserSQLStorage(engine=engine)
    tracker.init_app(app, storage)
    if sentry: # pragma: no cover
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

    @app.template_filter('format_number')
    def reverse_filter(s):
        if s:
            return '{:,}'.format(s)
        return s
    
    @app.template_filter('format_date_sort')
    def reverse_filter(s):
        if s:
            return s.strftime('%Y%m%d%H%M')
        else:
            return '0'


    return app

