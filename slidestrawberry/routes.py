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
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(e)
            return Response(status=500, body=[])
    return _filter_response


def includeme(config):
    config.add_static_view(name='static', path='static', cache_max_age=3600)
    config.add_route('home', '/')
    # -- 数据API --
    _version = '1.0'
    # 数据统计
    config.add_route(with_version(_version, 'energy_count'), with_version(_version, '/energy_count'))
    config.add_route(with_version(_version, 'security_count'), with_version(_version, '/security_count'))
    config.add_route(with_version(_version, 'pms_count'), with_version(_version, '/pms_count'))
    config.add_route(with_version(_version, 'firealarm_count'), with_version(_version, '/firealarm_count'))
