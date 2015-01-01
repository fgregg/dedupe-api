import unittest
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app

fixtures_path = join(dirname(abspath(__file__)), 'fixtures')

class MatchingTest(unittest.TestCase):
    ''' 
    Test the matching module
    '''
    pass

