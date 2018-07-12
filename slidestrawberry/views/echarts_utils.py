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

__author__ = 'terry'


_ECHARTS_KEYS = {
    'line': ['legend', 'series', 'category'],
    'bar': ['legend', 'series', 'category'],
    'pie': ['legend', 'series'],
    'radius': ['legend', 'series', 'category', 'indicate'],
}


def build_echarts_struct(chart_type=None):
    """
    生成echarts图表结构
    :param chart_type: 图标类型
    :return:

    """
    _e = {}

    def _build_echarts(_key):
        if _key in ['legend']:
            _e[_key] = {'data': []}
        elif _key in ['series',
                      'category',
                      'indicate']:
            _e[_key] = []

    if not chart_type:
        return {
            'legend': {'data': []},
            'series': [],
            'category': [],  # x轴
        }

    if chart_type in _ECHARTS_KEYS:
        for _k in _ECHARTS_KEYS[chart_type]:
            _build_echarts(_k)

    return _e


def build_echarts_series(legend_name, chart_type, data, **kwargs):
    """
    生成echarts结构数据
    :param legend_name: 标识名称
    :param chart_type: 图表类型
    :param data: 数据
    :param kwargs: 可变字段定义
    :return:
    """

    if chart_type == 'radius':
        _data = {
            'name': legend_name,
            'type': chart_type,
            'value': data
        }
    else:
        _data = {
            'name': legend_name,
            'type': chart_type,
            'data': data
        }
    for _key, _value in kwargs.items():
        _data[_key] = _value
    return _data


def append_echarts_series(
        echart_struct,
        legend_name,
        chart_type,
        data,
        **kwargs):
    if 'series' not in echart_struct:
        return
    echart_struct['series'].append(
        build_echarts_series(
            legend_name,
            chart_type,
            data,
            **kwargs))


def append_echarts_legend(echart_struct, legend_list):
    if 'legend' not in echart_struct:
        return
    if 'data' not in echart_struct['legend']:
        return
    for l in legend_list:
        if l not in echart_struct['legend']['data']:
            echart_struct['legend']['data'].append(l)


def append_echarts_category(echart_struct, category_list):
    if 'category' not in echart_struct:
        return
    for c in category_list:
        if c not in echart_struct['category']:
            echart_struct['category'].append(c)


def append_echarts_indicate(echart_struct, category_list):
    if 'indicate' not in echart_struct:
        return
    for c in category_list:
        if c not in echart_struct['indicate']:
            echart_struct['indicate'].append(c)
