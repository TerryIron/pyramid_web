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

from pyramid.config import Configurator
from pyramid_celery import celery_app as app

__version__ = (0, 1, 0)

GLOBAL_CONFIG = {}


def main(global_config, **settings):
    """
    程序主入口
    :param global_config: 全局配置表
    :param settings: 配置表
    :return:
    """

    GLOBAL_CONFIG.update(global_config)
    """ This functionreturns a Pyramid WSGI application.
    """
    file_name = global_config.get('__file__')
    settings['__file__'] = file_name
    config = Configurator(settings=settings)
    config.include('pyramid_tm')
    config.include('pyramid_jinja2')
    config.include('.models')
    config.include('.plugins')
    config.include('.routes')
    config.configure_celery(file_name)
    app.ONE = {}
    config.scan()
    return config.make_wsgi_app()
