#!/bin/bash

/home/datamade/.virtualenvs/pip install -r /home/datamade/dedupe-api/requirements.txt --upgrade
aws s3 cp s3://datamade-codedeploy/configs/dedupeapi_app_config.py . --region us-east-1
mv dedupeapi_app_config.py /home/datamade/dedupe-api/api/app_config.py
/home/datamade/.virtualenvs/python /home/datamade/dedupe-api/init_db.py
