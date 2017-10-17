#!/usr/bin/env python
# coding=utf-8

#
# Copyright (c) 2015-2018  Terry Xi
# All Rights Reserved.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import os
import json
import urlparse
import logging
import subprocess
import base64
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

__author__ = 'terry'

logger = logging.getLogger(__name__)

# run with Python API


def _init_app(app_name, master, package):
    packages = package if isinstance(package, list) else [package]
    conf = SparkConf().setAppName(app_name).setMaster(master)
    for p in packages:
        conf.set('spark.jars.packages', p)
    return SparkContext(conf=conf)


class Handle(object):
    def __init__(self, handle, url):
        self.handle = handle
        self.url = url


# Not Used
def _get_handle_table_from_hbase(handle, table_name):
    if isinstance(handle, Handle):
        conf = {
            'hbase.zookeeper.quorum': handle.url,
            'hbase.mapreduce.inputtable': table_name,
            # 'hbase.mapreduce.scan.row.start': 'row2'
        }
        rdd = handle.handle.newAPIHadoopRDD(
            'org.apache.hadoop.hbase.mapreduce.TableInputFormat',
            'org.apache.hadoop.hbase.io.ImmutableBytesWritable',
            'org.apache.hadoop.hbase.client.Result',
            keyConverter='org.valux.converters.ImmutableBytesWritableToStringConverter',
            valueConverter='org.valux.converters.HBaseResultToStringConverter',
            conf=conf)
        return rdd


def _get_handle(uri, raw=False):
    # SparkContext._ensure_initialized()
    try:
        # SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
        _spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()
    except:
        _spark = SparkSession \
            .builder \
            .getOrCreate()

    if raw:
        return _spark
    else:
        _p = urlparse.urlparse(uri)
        # _host = urlparse.urlparse(uri).netloc.split(':')[0]
        return Handle(_spark, _p.hostname)


def mongo_handle(master_url, uri, package_name, raw=False):
    app = _init_app('MongoInfoApp', master_url, package_name)
    return _get_handle(uri, raw=raw)


def hbase_handle(master_url, uri, raw=False):
    _spark = SparkContext(master=master_url,
                          appName='HbaseInfoApp')
    if raw:
        return _spark
    else:
        return Handle(_spark, uri)


# run with Python Scripts
def start_spark_app(spark_bin, url, script_name, tables=None, packages=None, drivers=None, files=None,
                    cache_dir=None, ext_args=None, master_url='spark://127.0.0.1:7077', hadoop_home=None):
    if not os.path.exists(script_name):
        raise Exception('File {0} not exist!'.format(script_name))
    _cmd = ' '.join([spark_bin, '--master', master_url])

    def parse_db_type(db_url, cmd_line):
        _d = urlparse.urlparse(db_url)
        if _d.scheme == 'mongodb':
            _cmd_line = ' '.join([cmd_line,
                                  '--conf spark.mongodb.input.uri=' + db_url,
                                  '--conf spark.mongodb.output.uri=' + db_url])
            return _cmd_line
        else:
            return cmd_line

    _url_keys = []
    if not isinstance(url, dict):
        _cmd = parse_db_type(url, _cmd)
    else:
        _url_keys = url.keys()
        _url_keys.sort()
        _cmd = parse_db_type(url[_url_keys[0]], _cmd)

    if not packages:
        _packages = []
    else:
        _packages = [_p for _p in packages if _p] if isinstance(packages, str) else packages
    _packages.append('com.databricks:spark-csv_2.10:1.5.0')
    if _packages:
        _cmd += ' --packages ' + ','.join(_packages)
    if drivers:
        # 类似--jars
        _cmd += ' --driver-class-path ' + ','.join([_driver for _driver in drivers if os.path.exists(_driver)])
    if files:
        _cmd += ' --files ' + ','.join([_file for _file in files if os.path.exists(_file)])

    _cmd += ' ' + script_name + ' run'
    if _url_keys:
        _url_list = []
        for _key in _url_keys:
            _url = url[_key]
            _url_list.append(_url)
        _url_list_key = '^'.join(_url_list)
        _cmd += ' '.join([' --base-db', base64.b64encode(_url_list_key)])
    else:
        _cmd += ' '.join([' --base-db', base64.b64encode(url)])
    if tables:
        if isinstance(tables, list):
            _cmd += ' '.join([' --base-table', ','.join(tables)])
        elif isinstance(tables, dict):
            _cmd += ' '.join([' --base-table', '^'.join([','.join(tables[_key]) for _key in _url_keys
                                                         if _key in tables])])
    if cache_dir:
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        _cmd += ' '.join([' --cache-dir', cache_dir])

    if isinstance(url, dict):
        _cmd += ' --db-map ' + base64.b64encode(json.dumps(url))
    if ext_args and isinstance(ext_args, dict):
        for _k, _v in ext_args.items():
            _k = '-'.join(_k.split('_'))
            if _k and _v:
                if isinstance(_v, dict):
                    _cmd += ' --' + _k + ' ' + base64.b64encode(json.dumps(_v))
                else:
                    _cmd += ' --' + _k + ' ' + _v
    logger.debug('Command:{0}'.format(_cmd))
    if hadoop_home:
        os.putenv('HADOOP_HOME', hadoop_home)
    subprocess.call(_cmd, shell=True)


def spark_data_frame(spark_session, db, table, cmd=None):

    _d = urlparse.urlparse(db)
    _sqlcontext = SQLContext(spark_session.sparkContext)
    if _d.scheme == 'mongodb':
        _frame = spark_session.read.format("com.mongodb.spark.sql.DefaultSource").\
            option('uri', '.'.join([db, table]))
        # return spark_session.read.format("com.mongodb.spark.sql.DefaultSource").\
        #     option('uri', '.'.join([db, table])). \
        #     option('pipeline', pipeline)
    elif _d.scheme == 'sqlserver':
        _driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        if not cmd:
            _frame = _sqlcontext.read.format('jdbc').options(
                url='jdbc:' + db,
                driver=_driver,
                dbtable=table)
        else:
            _frame = _sqlcontext.read.format('jdbc').options(
                url='jdbc:' + db,
                driver=_driver,
                dbtable='({0}) as {1}'.format(cmd, table),
                lowerBound='10001',
                upperBound='499999',
                numPartitions='10')
    elif _d.scheme == 'mysql':
        _driver = 'com.mysql.jdbc.Driver'
        if not cmd:
            _frame = _sqlcontext.read.format('jdbc').options(
                url='jdbc:' + db,
                driver=_driver,
                dbtable=table)
        else:
            _frame = _sqlcontext.read.format('jdbc').options(
                url='jdbc:' + db,
                driver=_driver,
                dbtable='({0}) as {1}'.format(cmd, table),
                lowerBound='10001',
                upperBound='499999',
                numPartitions='10')

    return _frame, _d.scheme


def read_spark_data_frame(spark_session, db, table, cmd=None):
    __data_frame, __data_frame_type = spark_data_frame(spark_session, db, table, cmd)
    if cmd:
        if cmd == '__buildin__':
            # Rebuild for system
            if __data_frame_type == 'mongodb':
                pipeline = "{'$match': {'Version': '" + v + "'}}"
                return __data_frame.option('pipeline', pipeline).load()
        else:
            return __data_frame.load()
    else:
        if __data_frame_type == 'mongodb':
            return __data_frame.option('pipeline', '').load()
        else:
            return __data_frame.load()
