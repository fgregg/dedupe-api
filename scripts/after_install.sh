#!/bin/bash

sudo chown -R datamade.www-data /home/datamade
/home/datamade/.virtualenvs/bin/pip install -r /home/datamade/dedupe-api/requirements.txt --upgrade
aws s3 cp s3://datamade-codedeploy/configs/dedupeapi_app_config.py /home/datamade/dedupe-api/api/app_config.py --region us-east-1
/home/datamade/.virtualenvs/bin/python /home/datamade/dedupe-api/init_db.py
