import unittest
from uuid import uuid4
from flask import request
from api import create_app
from test_config import DEFAULT_USER
from api.database import app_session, worker_session

class AuthTest(unittest.TestCase):
    ''' 
    Test the admin module
    '''
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(config='tests.test_config')
        cls.client = cls.app.test_client()

    @classmethod
    def tearDownClass(cls):
        app_session.close()
        worker_session.close()
        worker_session.bind.dispose()

    def tearDown(self):
        self.logout()

    def login(self, email, password):
        return self.client.post('/login/', data=dict(
                    email=email,
                    password=password,
                ), follow_redirects=True)

    def logout(self):
        return self.client.get('/logout/')

    def test_login_redirect(self):
        with self.app.test_request_context():
            rv = self.client.get('/', follow_redirects=False)
            rd_path = rv.location.split('http://localhost')[1]
            rd_path = rd_path.split('?')[0]
            assert rd_path == '/login/'
            rv = self.client.get(rd_path)
            assert 'Please log in to access this page' in rv.data

    def test_login(self):
        user = DEFAULT_USER['user']
        with self.app.test_request_context():
            rv = self.login(user['email'], user['password'])
            assert user['name'].title() in rv.data
            assert request.path == '/'
    
    def test_bad_login(self):
        user = DEFAULT_USER['user']
        with self.app.test_request_context():
            self.logout()
            rv = self.login(user['email'], 'boo')
            assert 'Password is not valid' in rv.data
            self.logout()
            rv = self.login('boo', 'boo')
            assert 'Invalid email address' in rv.data
            self.logout()
            rv = self.login('boo@boo.com', 'boo')
            assert 'Email address is not registered' in rv.data

    def test_roles(self):
        with self.app.test_request_context():
            self.login('bob@bob.com', 'bobspw')
            rv = self.client.get('/user-list/', follow_redirects=True)
            assert "Sorry, you don&#39;t have access to that page" in rv.data
