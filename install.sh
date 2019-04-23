#!/bin/bash

apt-get install libmysqlclient-dev python-pip virtualenv*

virtualenv env --no-site-packages
source env/bin/activate && pip install setuptools==3.4.4
source env/bin/activate && pip install -r requirements.txt
