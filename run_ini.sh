#!/bin/bash

CONFIG=$1

[ "$CONFIG"  == "" ] && {
    CONFIG="./production.ini"
} || { 
    [ "$CONFIG" == "-d" ] && {
        CONFIG="./development.ini"
    }
}

PWD=$(pwd)
export PYTHONPATH=$PWD
python $(which pserve) $CONFIG
