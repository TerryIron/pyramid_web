#!/bin/bash

PWD=$(pwd)
CONFIG=$1
PORT=$2

[ "$PORT" == "" ] && {
    PORT=6543
}

[ "$CONFIG"  == "" ] && {
        CONFIG="$PWD/production.ini"
} || { 
    [ "$CONFIG" == "-d" ] && {
        CONFIG="$PWD/development.ini"
    }
}

for i in $(find | grep setup.py$); do 
    j=$(dirname $i); 
    [ "$j" != "." ] && {
        cd $j && python setup.py bdist_egg && cd -
    }
done

export PYTHONPATH=$PWD
cd $PWD
PORT_TEXT="0.0.0.0:$PORT [::1]:$PORT"
sed -i 's/^listen\ \{0,\}=\ \{0,\}\(.*\)/listen = '"$PORT_TEXT"'/' $CONFIG
python $(which pserve) $CONFIG
