#!/bin/bash

[ "$1" != "" ] && {
FILE="$1"
} || {
FILE="development.ini"
}

celery beat -A pyramid_celery.celery_app --ini $FILE
