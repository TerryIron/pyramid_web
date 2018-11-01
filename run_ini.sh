#!/bin/bash

CONFIG=$1

[ "$CONFIG"  == "" ] && {
        CONFIG="$(pwd)/production.ini"
} || { 
    [ "$CONFIG" == "-d" ] && {
        CONFIG="$(pwd)/development.ini"
    }
}

for i in $(find | grep setup.py$); do 
    j=$(dirname $i); 
    [ "$j" != "." ] && {
        cd $j && python setup.py bdist_egg && cd -
    }
done

PWD=$(pwd)
export PYTHONPATH=$PWD
echo python $(which pserve) $CONFIG
python $(which pserve) $CONFIG
