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
                    cache_dir=None, ext_args=None, master_url='spark://127.0.0.1:7077', hadoop_home=None,
                    enable_packages=False, cpu=None, mem=None):
    if not os.path.exists(script_name):
        raise Exception('File {0} not exist!'.format(script_name))
    _cmd = ' '.join([spark_bin, '--master ', master_url])

    if cpu:
        _cmd += ' --total-executor-cores {0} '.format(cpu)
    if mem:
        _cmd += ' --executor-memory {0}m '.format(mem)

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

    if enable_packages:
        if not packages:
            _packages = []
        else:
            _packages = [_p for _p in packages if _p] if isinstance(packages, str) else packages
        if _packages:
            _cmd += ' --packages ' + ','.join(_packages)
    if drivers:
        # 类似--jars
        _drivers = [_driver for _driver in drivers if os.path.exists(_driver)]
        if _drivers:
            _cmd += ' --driver-class-path ' + ','.join(_drivers)
            _cmd += ' --jars ' + ','.join(_drivers)
    if files:
        _files = [_file for _file in files if os.path.exists(_file)]
        if _files:
            _cmd += ' --files ' + ','.join(_files)

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
                if isinstance(_v, dict) or isinstance(_v, list):
                    _cmd += ' --' + _k + ' ' + base64.b64encode(json.dumps(_v))
                else:
                    _cmd += ' --' + _k + ' ' + _v
    logger.debug('Command:{0}'.format(_cmd))
    if hadoop_home:
        os.putenv('HADOOP_HOME', hadoop_home)
    subprocess.call(_cmd, shell=True)


def _is_buildin_command(_cmd):
    if _cmd == '__buildin__':
        return True
    else:
        return False


def _is_available_command(_cmd):
    if _cmd and _cmd != '__buildin__':
        return True
    else:
        return False


def spark_data_frame(spark_session, db, table, cmd=None):
    def _options(o):
        return o.options(lowerBound=1,
                         upperBound=10000000,
                         numPartitions=10)
    _d = urlparse.urlparse(db)
    _sqlcontext = SQLContext(spark_session.sparkContext)
    if _d.scheme == 'mongodb':
        _driver = 'com.mongodb.spark.sql.DefaultSource'
        _frame = spark_session.read.format(_driver).\
            option('uri', '.'.join([db, table]))
        # return spark_session.read.format("com.mongodb.spark.sql.DefaultSource").\
        #     option('uri', '.'.join([db, table])). \
        #     option('pipeline', pipeline)
    elif _d.scheme == 'sqlserver':
        _driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        _new_url, _username, _password = [], None, None
        for i in _d.netloc.split(';'):
            if i.startswith('user='):
                _username = i.split('user=')[1]
                continue
            if i.startswith('password='):
                _password = i.split('password=')[1]
                continue
            _new_url.append(i)
        _new_url = '://'.join([_d.scheme, ';'.join(_new_url)])

        if not _is_available_command(cmd):
            _frame = _sqlcontext.read.format('jdbc').options(
                url='jdbc:' + _new_url,
                driver=_driver,
                user=_username,
                password=_password,
                dbtable=table)
        else:
            _frame = _sqlcontext.read.format('jdbc').options(
                url='jdbc:' + _new_url,
                driver=_driver,
                user=_username,
                password=_password,
                dbtable='({0}) as {1}'.format(cmd, table))
        _frame = _options(_frame)
    else:
        _username = _d.username
        _password = _d.password
        _hostname = _d.hostname
        _port = _d.port
        _parts = (
            _d.scheme,
            ':'.join([str(_hostname), str(_port)]) if _hostname and _port else _hostname,
            _d.path,
            _d.params,
            _d.query,
            _d.fragment
        )
        _new_url = urlparse.urlunparse(_parts)
        if _d.scheme == 'mysql':
            _driver = 'com.mysql.jdbc.Driver'
            if not _is_available_command(cmd):
                _frame = _sqlcontext.read.format('jdbc').options(
                    url='jdbc:' + _new_url,
                    driver=_driver,
                    user=_username,
                    password=_password,
                    dbtable=table)
            else:
                _frame = _sqlcontext.read.format('jdbc').options(
                    url='jdbc:' + _new_url,
                    driver=_driver,
                    user=_username,
                    password=_password,
                    dbtable='({0}) as {1}'.format(cmd, table))
        _frame = _options(_frame)

    return _frame, _d.scheme


def read_spark_data_frame(spark_session, db, table, cmd=None, version='v1'):
    __data_frame, __data_frame_type = spark_data_frame(spark_session, db, table, cmd)
    if _is_buildin_command(cmd):
        if __data_frame_type == 'mongodb':
            pipeline = "{'$match': {'Version': '" + version + "'}}"
            return __data_frame.option('pipeline', pipeline).load()
        else:
            return __data_frame.load()
    else:
        return __data_frame.load()
