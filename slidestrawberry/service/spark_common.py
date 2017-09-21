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
import urlparse
import logging
import subprocess
import bson
import base64
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

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
        _host = urlparse.urlparse(uri).netloc.split(':')[0]
        return Handle(_spark, _host)


def mongo_handle(spark_master, uri, package_name, raw=False):
    app = _init_app('MongoInfoApp', spark_master, package_name)
    return _get_handle(uri, raw=raw)


def hbase_handle(spark_master, uri, raw=False):
    _spark = SparkContext(master=spark_master,
                          appName='HbaseInfoApp')
    if raw:
        return _spark
    else:
        return Handle(_spark, uri)


# run with Python Scripts
def start_spark_app(spark_bin, url, script_name, packages=None, drivers=None, tables=None, cache_dir=None,
                    ext_args=None, spark_master='local'):
    if not os.path.exists(script_name):
        raise Exception('File {0} not exist!'.format(script_name))
    _cmd = ' '.join([spark_bin, '--master', spark_master])

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
        _cmd += ' --driver-class-path ' + ','.join([_driver for _driver in drivers if os.path.exists(_driver)])

    _cmd += ' ' + script_name + ' run'
    if _url_keys:
        _url_list = []
        for _key in _url_keys:
            _url = url[_key]
            if len(_url.split('.')) > 4:
                _url_list.append('.'.join(_url.split('.')[:-1]))
            else:
                _url_list.append(_url)
        _url_list_key = '^'.join(_url_list)
        _cmd += ' '.join([' --base-db', base64.b64encode(_url_list_key)])
    else:
        if len(url.split('.')) > 4:
            _cmd += ' '.join([' --base-db', base64.b64encode('.'.join(url.split('.')[:-1]))])
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
        _cmd += ' --db-map ' + base64.b64encode(bson.dumps(url))
    if ext_args and isinstance(ext_args, dict):
        for _k, _v in ext_args.items():
            _k = '-'.join(_k.split('_'))
            if _k and _v:
                if isinstance(_v, dict):
                    _cmd += ' --' + _k + ' ' + base64.b64encode(bson.dumps(_v))
                else:
                    _cmd += ' --' + _k + ' ' + _v
    logger.debug('Command:{0}'.format(_cmd))
    subprocess.call(_cmd, shell=True)
