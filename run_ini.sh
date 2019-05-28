#!/bin/bash

PWD=$(pwd)
CONFIG=$1
PORT=$2
TCP=$3

[ "$PORT" == "" ] && {
    PORT=6543
}

PORT_TEXT="0.0.0.0:$PORT"
[ "$TCP" == "-4" ] && {
    PORT_TEXT="0.0.0.0:$PORT"
} 
[ "$TCP" == "-6" ] && {
    PORT_TEXT="0.0.0.0:$PORT [::1]:$PORT"
}

[ "$CONFIG"  == "" ] && {
        CONFIG="$PWD/production.ini"
} || { 
    [ "$CONFIG" == "-d" ] && {
        CONFIG="$PWD/development.ini"
    }
}

for i in $(find | grep -v "^./env*\|.*env/" |grep setup.py$); do 
    j=$(dirname $i); 
    [ "$j" != "." ] && {
        cd $j && python setup.py bdist_egg && cd -
    }
done

export PYTHONPATH=$PWD
cd $PWD
sed -i 's/^listen\ \{0,\}=\ \{0,\}\(.*\)/listen = '"$PORT_TEXT"'/' $CONFIG
export PYTHONOPTIMIZE=1
python $(which pserve) $CONFIG
