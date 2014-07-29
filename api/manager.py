from flask import Blueprint, request, session as flask_session, \
    render_template
from api.database import session as db_session
from api.models import User
from api.auth import login_required

manager = Blueprint('manager', __name__)

@manager.route('/')
def index():
    return render_template('index.html')

@manager.route('/manager/')
@login_required
def manage():
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('manager.html', user=user)
