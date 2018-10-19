#!/bin/bash

CONFIG=$1

[ "$CONFIG"  == "" ] && {
    CONFIG="./production.ini"
} || { 
    [ "$CONFIG" == "-d" ] && {
        CONFIG="./development.ini"
    }
}

for i in $(find | grep setup.py); do 
    j=$(dirname $i); 
    [ "$j" != "." ] && {
        cd $j && python setup.py bdist_egg && cd -
    }
done

PWD=$(pwd)
export PYTHONPATH=$PWD
python $(which pserve) $CONFIG
