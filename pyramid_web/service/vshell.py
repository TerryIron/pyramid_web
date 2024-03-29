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

import argparse

__author__ = 'terry'


class VShell(object):
    """
    命令行基础类
    """

    def __init__(self):
        self.cmd_parser = argparse.ArgumentParser()
        self.sub_cmd = self.cmd_parser.add_subparsers()
        self.prepare()
        self.args = self.cmd_parser.parse_args()

    def has_command(self, command_name):
        return True if self.args.id == str(command_name) else False

    def get_argument(self, store_name):
        if hasattr(self.args, store_name):
            return getattr(self.args, store_name)

    def command(self, command_name, help_text=None):
        # sub-command: install
        sub_command = self.sub_cmd.add_parser(command_name, help=help_text)
        sub_command.set_defaults(id=command_name)

        class SubCommand(object):
            def __init__(self, command):
                self.command = command

            def install_argument(self,
                                 args,
                                 store_name,
                                 default=None,
                                 is_bool=False,
                                 help_text=None):
                args = [args] if not isinstance(args, list) else args
                if is_bool:
                    if default:
                        action = 'store_false'
                    else:
                        action = 'store_true'
                else:
                    action = 'store'
                self.command.add_argument(
                    *args,
                    default=default,
                    dest=store_name,
                    action=action,
                    help=help_text)

        return SubCommand(sub_command)

    def prepare(self):
        pass

    def run(self):
        pass


class SparkShell(VShell):
    """
    Spark命令行基础类
    """

    def prepare(self):
        super(SparkShell, self).prepare()
        command = self.command(
            'run',
            help_text=u'启动Spark脚本, 全局变量有如下'
            u'spark->SparkSession类'
            u'sc->SparkContext类'
            u'sql->spark.sql函数')
        command.install_argument(
            ['--cache-dir'],
            'cache_dir',
            default='temp_dir',
            help_text=u'脚本临时数据目录')
        command.install_argument(
            ['--config-db'], 'config_db', default='', help_text=u'脚本配置表')
        command.install_argument(
            ['--base-db'],
            'base_db',
            default='',
            help_text=u'脚本数据库源, base64加码')
        command.install_argument(
            ['--base-table'], 'base_table', default='', help_text=u'脚本数据表源')
        command.install_argument(
            ['--db-map'], 'db_map', default={}, help_text=u'脚本数据库源对照表')
        return command
