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

for i in $(find | grep setup.py$); do 
    j=$(dirname $i); 
    [ "$j" != "." ] && {
        cd $j && python setup.py bdist_egg && cd -
    }
done

export PYTHONPATH=$PWD
cd $PWD
sed -i 's/^listen\ \{0,\}=\ \{0,\}\(.*\)/listen = '"$PORT_TEXT"'/' $CONFIG

log_path="/tmp/run_ini"
log_file="$log_path/`echo $RANDOM|md5sum|cut -c 1-10`.log"
mkdir -p $log_path
> $log_file
python $(which pserve) $CONFIG 2>&1 | tee -a $log_file &
log_pid=$!
server_pid=$(netstat -tanp | grep "0.0.0.0:$PORT" | awk '{print $7}' | sed -n 's/\(.*\)\/.*/\1/p')
[ "$server_pid" == "" ] && {
    server_pid=$(($log_pid - 1))
}
father_pid=$(ps -ef  | grep -v grep | grep $log_pid | awk '{print $3}')
sleep 3
tail -100f $log_file | awk 'BEGIN{a=0;kcmd="kill -9 '$log_pid' '$server_pid' '$father_pid'";dd="rm -f '$log_file'"}{if(match($0, ".*Server Starting.*") > 0)a+=1;else if(a == 1)system(kcmd)system(dd)}' 2>/dev/null
