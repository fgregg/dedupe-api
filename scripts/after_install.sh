#!/bin/bash

chown -R datamade.www-data /home/datamade
/home/datamade/.virtualenvs/dedupe-api/bin/pip install numpy
/home/datamade/.virtualenvs/dedupe-api/bin/pip install -r /home/datamade/dedupe-api/requirements.txt --upgrade
aws s3 cp s3://datamade-codedeploy/configs/dedupeapi_app_config.py /home/datamade/dedupe-api/api/app_config.py --region us-east-1
chown datamade.www-data /home/datamade/dedupe-api/api/app_config.py
/home/datamade/.virtualenvs/dedupe-api/bin/python /home/datamade/dedupe-api/init_db.py
