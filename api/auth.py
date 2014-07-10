from flask import session, redirect, url_for, request, Blueprint, \
    render_template, abort, flash
from flask_security import Security, login_required
from flask_wtf import Form
from flask_wtf.csrf import CsrfProtect
from wtforms import TextField, validators
from api.database import db, ApiUser
import os
import json
from uuid import uuid4

auth = Blueprint('auth', __name__)

security = Security()

csrf = CsrfProtect()

class AddApiUser(Form):
    name = TextField('name', [validators.DataRequired()])
    email = TextField('email', 
        [validators.DataRequired(), validators.Email()])

@auth.route('/add-api-user/', methods=['GET', 'POST'])
@login_required
def add_api_user():
    form = AddApiUser()
    if form.validate_on_submit():
        api_key = unicode(uuid4())
        user_info = {
            'name': form.name.data,
            'email': form.email.data,
            'api_key': api_key,
        }
        user = ApiUser(**user_info)
        db.session.add(user)
        db.session.commit()
        flash('User %s added' % user.name)
        return redirect(url_for('auth.api_user_list'))
    return render_template('add_api_user.html', form=form)

@auth.route('/')
@login_required
def api_user_list():
    users = db.session.query(ApiUser).all()
    return render_template('api_user_list.html', users=users)
