#!/bin/bash

#[ "$(ls work.log)" != ""] && {
#    mv work.log "work$(date -Iseconds).log"
#}
#celery worker --purge -A pyramid_celery.celery_app --ini development.ini 2>&1 | tee work.log
[ "$1" != "" ] && {
FILE="$1"
} || {
FILE="development.ini"
}
rm -rf /tmp/hive
rm -rf metastore_db
celery worker --purge -A pyramid_celery.celery_app --ini $FILE
