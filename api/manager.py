from flask import Blueprint, request, session as flask_session, \
    render_template
from api.database import session as db_session, DEFAULT_ROLES
from api.models import User, Role
from api.auth import login_required, check_roles
from flask_wtf import Form
from wtforms import TextField, PasswordField, SelectMultipleField
from wtforms.validators import DataRequired, Email

manager = Blueprint('manager', __name__)

ROLE_CHOICES = [(r['name'], r['description'],) for r in DEFAULT_ROLES]

class AddUserForm(Form):
    name = TextField('name', validators=[DataRequired()])
    email = TextField('email', validators=[DataRequired(), Email()])
    roles = SelectMultipleField('roles', choices=ROLE_CHOICES, 
                                validators=[DataRequired()])
    password = PasswordField('password', validators=[DataRequired()])

    def __init__(self, *args, **kwargs):
        Form.__init__(self, *args, **kwargs)
        self.user = None

    def validate(self):
        rv = Form.validate(self)
        if not rv:
            return False

        existing_name = db_session.query(User)\
            .filter(User.name == self.name.data).first()
        if existing_name:
            self.name.errors.append('Name is already registered')
            return False

        existing_email = db_session.query(User)\
            .filter(User.email == self.email.data).first()
        if existing_email:
            self.email.errors.append('Email address is already registered')
            return False
        
        return True

@manager.route('/add-user/', methods=['GET', 'POST'])
@login_required
@check_roles(roles=['admin'])
def add_user():
    form = AddUserForm()
    if form.validate_on_submit():
        roles = []
        for role in roles:
            r = db_session.query(Role).filter(Role.name == role).first()
            roles.append(r)
        user_info = {
            'name': form.name.data,
            'email': form.email.data,
            'roles': roles,
        }
        user = User(**user_info)
        db_session.add(user)
        db_session.commit()
        flash('User %s added' % user.name)
        return redirect(url_for('auth.user_list'))
    roles = db_session.query(Role).all()
    return render_template('add_user.html', form=form, roles=roles)

@manager.route('/user-list/')
@login_required
@check_roles(roles=['admin'])
def user_list():
    users = db_session.query(User).all()
    return render_template('user_list.html', users=users)

@manager.route('/')
@login_required
def index():
    user = db_session.query(User).get(flask_session['user_id'])
    return render_template('index.html', user=user)
