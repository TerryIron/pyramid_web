#!/bin/bash

PROJECT_PATH="$(pwd)"

# 基础数据库
HBASE_BIN="/home/terry/app/spark_server/db/hbase"
# 业务数据库
HBASE_DASHBOARD_BIN="/home/terry/app/spark_server/dashboard/hue"
# 数据分析
SPARK_BIN="/home/terry/app/spark_server/spark"

cd $HBASE_BIN
./start.sh
cd $HBASE_DASHBOARD_BIN
./start.sh
cd $SPARK_BIN
./start_master.sh
cd $PROJECT_PATH
