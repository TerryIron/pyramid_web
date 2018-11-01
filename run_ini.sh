#!/bin/bash

PWD=$(pwd)
CONFIG=$1

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
python $(which pserve) $CONFIG
