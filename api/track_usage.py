from flask.ext.track_usage import TrackUsage
from flask.ext.track_usage.storage import Storage
from flask import _request_ctx_stack, g, session
from datetime import datetime
import time
import json
import sqlalchemy as sql


class TrackUserUsage(TrackUsage):
    ''' 
    Subclassing base class to add user specific attributes
    to the tracking info. Assuming all routes are excluded from
    tracking unless explicitly included. Also assuming at least
    Python 2.7
    '''
    
    def init_app(self, app, storage):
        self.app = app
        self._storage = storage
        self._type = 'exclude'
        app.before_request(self.before_request)
        app.after_request(self.after_request)

    def include(self, view):
        self._include_views.add(view.func_name)

    def after_request(self, response):
        ctx = _request_ctx_stack.top
        view_func = self.app.view_functions.get(ctx.request.endpoint).func_name
        if view_func in self._include_views and \
            ctx.request.endpoint != 'static':
            
            now = datetime.utcnow()
            speed = (now - g.start_time).total_seconds()
            remote_addr = ctx.request.remote_addr
            if ctx.request.headers.get('X-Forwarded-For', None):
                remote_addr = ctx.request.headers['X-Forwarded-For']
            
            data = {
                'url': ctx.request.url,
                'user_agent': ctx.request.user_agent,
                'blueprint': ctx.request.blueprint,
                'view_args': ctx.request.view_args,
                'status': response.status_code,
                'remote_addr': remote_addr,
                'authorization': bool(ctx.request.authorization),
                'ip_info': None,
                'path': ctx.request.path,
                'speed': float(speed),
                'date': int(time.mktime(now.timetuple())),
                'api_key': session.get('api_key', None),
            }
         
            self._storage(data)
        return response

class UserSQLStorage(Storage):
    
    def set_up(self, engine=None, table_name="dedupe_usage"):

        self._eng = engine
        meta = sql.MetaData()
        try:
            self.track_table = sql.Table(table_name, meta, 
                autoload=True, autoload_with=self._eng, keep_existing=True)
        except sql.exc.NoSuchTableError:
            self.track_table = sql.Table(
                table_name, meta,
                sql.Column('id', sql.Integer, primary_key=True),
                sql.Column('url', sql.String(128)),
                sql.Column('ua_browser', sql.String(16)),
                sql.Column('ua_language', sql.String(16)),
                sql.Column('ua_platform', sql.String(16)),
                sql.Column('ua_version', sql.String(16)),
                sql.Column('blueprint', sql.String(16)),
                sql.Column('view_args', sql.String(64)),
                sql.Column('status', sql.Integer),
                sql.Column('remote_addr', sql.String(24)),
                sql.Column('authorization', sql.Boolean),
                sql.Column('ip_info', sql.String(128)),
                sql.Column('path', sql.String(32)),
                sql.Column('speed', sql.Float),
                sql.Column('datetime', sql.DateTime),
                sql.Column('api_key', sql.String(36), index=True)
            )
            meta.create_all(self._eng)

    def store(self, data):
        user_agent = data["user_agent"]
        utcdatetime = datetime.fromtimestamp(data['date'])
        stmt = self.track_table.insert().values(
            url=data['url'],
            ua_browser=user_agent.browser,
            ua_language=user_agent.language,
            ua_platform=user_agent.platform,
            ua_version=user_agent.version,
            blueprint=data["blueprint"],
            view_args=json.dumps(data["view_args"], ensure_ascii=False),
            status=data["status"],
            remote_addr=data["remote_addr"],
            authorization=data["authorization"],
            ip_info=data["ip_info"],
            path=data["path"],
            speed=data["speed"],
            datetime=utcdatetime,
            api_key=data['api_key'],
        )
        with self._eng.begin() as con:
            con.execute(stmt)

tracker = TrackUserUsage()
