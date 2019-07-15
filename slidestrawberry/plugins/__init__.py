#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from slidestrawberry.plugins.loader import PluginLoaderV1


def includeme(config, load_plugin=False, load_eventloop=True):
    """
    Initialize plugins for an application.

    Activate this setup using ``config.include('PROJECT.plugins')``.

    """

    try:
        settings = config.settings
    except:
        settings = config.get_settings()

    from ConfigParser import ConfigParser

    class _PluginLoaderV1(PluginLoaderV1):
        @classmethod
        def start(cls):
            cls.start_plugins(load_eventloop=load_eventloop)

        @classmethod
        def load_plugins(cls, plugin_path):
            c = ConfigParser()
            c.read(settings['__file__'])
            if load_plugin:
                setattr(c, '__file__', settings['__file__'])
            cls.load_plugins_from_config(c)

    _PluginLoaderV1.start()

    _plugin_path = os.path.dirname(__file__)
    for i in os.listdir(_plugin_path):
        api_file = os.path.join(os.path.join(_plugin_path, i), 'routes.py')
        if os.path.exists(api_file):
            _mod = __import__('{}.routes'.format(i), globals(), locals(), ['routes'])
            if hasattr(_mod, 'get_routes') and callable(getattr(_mod, 'get_routes')):
                for k, v in getattr(_mod, 'get_routes')().items():
                    config.add_handler(k.replace('/', '_'), k, v)
