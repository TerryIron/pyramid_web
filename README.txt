slidestrawberry README
==================

Getting Started
---------------

- cd <directory containing this file>

- $VENV/bin/pip install -e .

- $VENV/bin/python setup.py develop

- $VENV/bin/initialize_db development.ini

- $VENV/bin/pserve development.ini

- $VENV/bin/celery worker -A pyramid_celery.celery_app --ini development.ini

- $VENV/bin/celery beat -A pyramid_celery.celery_app --ini development.ini
