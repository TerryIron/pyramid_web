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

from pyramid.response import Response
import functools
import json
import logging

logger = logging.getLogger(__name__)


def with_version(version_id, name):
    if '/' in name:
        return '/' + str(version_id) + str(name)
    else:
        return str(name) + '_' + str(version_id)


def filter_response(func):
    @functools.wraps(func)
    def _filter_response(*args, **kwargs):
        try:
            root_factory, request = args
            request.response.headers['Access-Control-Allow-Origin'] = '*'
            return func(request)
        except Exception as e:
            import traceback
            logger.error(traceback.format_exc())
            return Response(json.dumps([]), 500,
                            headers={'Content-Type': 'application/json',
                                     'Access-Control-Allow-Origin': '*'})
    return _filter_response


def includeme(config):
    config.add_static_view(name='static', path='static', cache_max_age=3600)
    config.add_route('home', '/')
    # -- API版本 --
    _version = '1.0'

    config.add_route(with_version(_version, 'test'), with_version(_version, '/test'))

