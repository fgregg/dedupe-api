from flask import request
from test_config import DEFAULT_USER
from api.database import app_session, worker_session
from tests import DedupeAPITestCase

class AuthTest(DedupeAPITestCase):
    ''' 
    Test the admin module
    '''

    def test_login_redirect(self):
        self.logout()
        with self.app.test_request_context():
            with self.client as c:
                rv = c.get('/', follow_redirects=False)
                rd_path = rv.location.split('http://localhost')[1]
                rd_path = rd_path.split('?')[0]
                assert rd_path == '/login/'
                rv = c.get(rd_path)
                assert 'Please log in to access this page' in rv.data

    def test_login(self):
        self.logout()
        user = DEFAULT_USER['user']
        with self.app.test_request_context():
            rv = self.login(email=user['email'], pw=user['password'])
            assert request.path == '/'
    
    def test_bad_login(self):
        self.logout()
        user = DEFAULT_USER['user']
        with self.app.test_request_context():
            self.logout()
            rv = self.login(email=user['email'], pw='boo')
            assert 'Password is not valid' in rv.data
            self.logout()
            rv = self.login(email='boo', pw='boo')
            assert 'Invalid email address' in rv.data
            self.logout()
            rv = self.login(email='boo@boo.com', pw='boo')
            assert 'Email address is not registered' in rv.data

    def test_roles(self):
        self.logout()
        with self.app.test_request_context():
            self.login(email='bob@bob.com', pw='bobspw')
            rv = self.client.get('/user-list/', follow_redirects=True)
            assert "Sorry, you don&#39;t have access to that page" in rv.data
