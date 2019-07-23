#!/usr/bin/env python                                                                                                                                                             
# -*- coding: utf-8 -*-

import logging


class Application(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def set_logger(self, logger):
        self.logger = logger


APP = None
CONFIG = {}


def init():
    global APP
    APP = Application()
    CONFIG['logger.path'] = '/tmp/sample.log'


def start(loader, **kwargs):
    return {'result': {}, 'data': {}}


def stop(loader, **kwargs):
    return {'result': {}, 'data': {}}
