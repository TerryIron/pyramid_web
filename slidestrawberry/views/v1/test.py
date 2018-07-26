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

from pyramid.view import view_config
from pyramid_handlers import action

from slidestrawberry.core import with_version, filter_response, check_request_params
from slidestrawberry.service.log import get_logger

__author__ = 'terry'

logger = get_logger(__name__)

VERSION = '1.0'


@view_config(
    route_name=with_version(VERSION, 'test'),
    request_method=('GET', 'POST'),
    renderer='json')
@check_request_params('test', expect_values=['test'], need_exist=True)
@filter_response(True)
def test(request):
    pass



class Test(object):
    __autoexpose__ = None

    def __init__(self, request):
        self.request = request

    @action(renderer='json')
    def get(self):
        pass

    @action(renderer='json')
    def post(self):
        pass

    @action(renderer='json')
    def put(self):
        pass

    @action(renderer='json')
    def delete(self):
        pass
