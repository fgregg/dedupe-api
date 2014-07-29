from flask import Blueprint

manager = Blueprint('manager', __name__)

@manager.route('/manager/')
@login_required
def manage():
    return None
