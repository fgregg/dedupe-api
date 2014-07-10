import os
import json
from flask import Flask, make_response, request, session, Blueprint

endpoints = Blueprint('endpoints', __name__)

@endpoints.route('/match/', methods=['POST'])
def match():
    
    resp = make_response(json.dumps({}))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@endpoints.route('/train/', methods=['POST'])
def train():
    print request.data
    resp = make_response(json.dumps({}))
    resp.headers['Content-Type'] = 'application/json'
    return resp
