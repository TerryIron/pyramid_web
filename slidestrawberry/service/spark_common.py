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
# import commands
import subprocess
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


def start_spark_app(spark_bin, spark_master, url, script_name, packages=None, 
                    cache_dir=None, ext_args=None):
    if not os.path.exists(script_name):
        raise Exception('File {0} not exist!'.format(script_name))
    _cmd = ' '.join([spark_bin, '--master', spark_master])
    _url = urlparse.urlparse(url)
    if _url.scheme == 'mongodb':
        _cmd = ' '.join([_cmd, '--conf spark.mongodb.input.uri=' + url, '--conf spark.mongodb.output.uri=' + url])
    elif _url.scheme == 'hbase':
        return
    else:
        return
    if not packages:
        _packages = []
    else:
        _packages = [_p for _p in packages if _p] if isinstance(packages, str) else packages
    for _p in _packages:
        if _p:
            _cmd += ' --packages ' + _p
    _cmd += ' ' + script_name + ' run'
    if cache_dir:
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        _cmd += ' '.join([' --cache-dir', cache_dir])
    if ext_args and isinstance(ext_args, dict):
        for _k, _v in ext_args.items():
            _k = '-'.join(_k.split('_'))
            if _k and _v:
                _cmd += ' --' + _k + ' ' + _v
    logger.debug('Command:{0}'.format(_cmd))
    # logger.info(commands.getoutput(_cmd))
    subprocess.call(_cmd, shell=True)


if __name__ == "__main__":
    DEV_TASK = {
        'read_dev_data': os.path.join(os.path.dirname(__file__), 'read_dev_data.py'),
        'write_dev_data': os.path.join(os.path.dirname(__file__), 'write_dev_data.py'),
    }

    DEV_TASK_CACHE = os.path.join(os.path.dirname(__file__), 'data_dir')

    start_spark_app('/media/terry/Transcend/source/workspace/wizcloud/test/spark_server/spark/bin/spark-submit', 
                    'local', 'mongodb://127.0.0.1:27017/accesskey.wuchj_test', DEV_TASK['read_dev_data'],
                    packages=['org.mongodb.spark:mongo-spark-connector_2.11:2.1.0'],
                    cache_dir=DEV_TASK_CACHE)

    # spark = mongo_handle('local',
    #                      'mongodb://127.0.0.1:27017/accesskey.wuchj_test',
    #                      'org.mongodb.spark:mongo-spark-connector_2.11:2.1.0',
    #                      raw=True)

    # pipeline = "{'$match': {'Version': '1.0'}}"
    # df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("pipeline", pipeline).load()
