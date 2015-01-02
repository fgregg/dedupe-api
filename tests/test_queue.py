import unittest
from api.queue import queuefunc, DelayedResult, processMessage
from os.path import join, abspath, dirname
from uuid import uuid4
from flask import request, session
from api import create_app
import time

@queuefunc
def add(a, b):
    return a + b

@queuefunc
def error():
    raise Exception('Test Exception')

class QueueTest(unittest.TestCase):
    ''' 
    Test the queue module
    '''

    def test_queuefunc(self):
        key = add.delay(1,3).key
        rv = DelayedResult(key)
        while not rv.return_value:
            processMessage()
            time.sleep(1)
        assert rv.return_value == 4

    def test_exception(self):
        key = error.delay().key
        rv = DelayedResult(key)
        while not rv.return_value:
            processMessage()
            time.sleep(1)
        print rv.return_value
        assert rv.return_value == 'Exc: Test Exception'

if __name__ == "__main__":
    unittest.main()
