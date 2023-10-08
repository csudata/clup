#!/usr/bin/env python
# -*- coding:UTF-8

# Copyright (c) 2023 CSUDATA.COM and/or its affiliates.  All rights reserved.
# CLup is licensed under AGPLv3.
# See the GNU AFFERO GENERAL PUBLIC LICENSE v3 for more details.
# You can use this software according to the terms and conditions of the AGPLv3.
#
# THIS SOFTWARE IS PROVIDED BY CSUDATA.COM "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT, ARE
# DISCLAIMED.  IN NO EVENT SHALL CSUDATA.COM BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
@Author: tangcheng
@description: 通用工具
"""

import textwrap
from copy import deepcopy
from datetime import datetime

import config
import ip_lib
import logging

main_logger_name = config.get("logger_name", "main")
logger = logging.getLogger(f"{main_logger_name}.{__name__}")
md_line_break = "<br>"
td_fmt = "<td>{}</td>"
head_fmt = "<h2>{}</h2>"

__author__ = 'HeBee'


def get_cells_widths(columns, rows, max_width):
    widths = {column_name: (len(column_name) + 2)
              for column_name in columns}
    for row in rows:
        for column_name, value in row.items():
            if column_name not in columns:
                continue
            width = max(widths[column_name], len(str(value)) + 2)  # 2 为边框空白
            widths[column_name] = width if width < max_width else max_width
    return widths


# noinspection PyTypeChecker
def format_row(columns, row, widths):
    """
    :param columns: 为 list 元素 为字段名
        value为 该列表的类型(左对齐、居中、右对齐)
    :param row: 为列表的内容 字典形式 key 为字段名 value 为内容
    :param widths: 为字典形式 key 为字段名 value 为 该字段的最大长度
    :return:
    """
    _max_width = 40
    format_dict = {
        '-': str.ljust,
        '_': str.center,
        '': str.rjust
    }

    # 若 value 过长 则 换行
    for key, value in row.items():
        if key not in columns:
            continue
        lines = value.split('\n')
        new_lines = []
        for line in lines:
            line = textwrap.fill(line, widths[key])
            new_lines.append(line)
        value = '\n'.join(new_lines)
        row[key] = value

    # 获得该行的最高行高
    row_height = 0
    for key in columns:
        height = len(row.get(key, '').split('\n'))
        row_height = height if height > row_height else row_height

    bits = [['|'] for i in range(row_height)]  # 该行的内容

    for key in columns:
        value = row.get(key, "")
        lines = value.split('\n')
        # 如果 行高不足最大行高 则用 空字符 补齐缺少的行
        if len(lines) < row_height:
            lines += ([''.center(widths[key] - 2)] * (row_height - len(lines)))
        # 每一列根据 \n 将一行  一一对应的添加到对应的bits中去
        for index, line in enumerate(lines):
            # 如果 为表格 字段 本身则选择 居中方式
            if key == value:
                format_method = str.center
            else:
                # 根据 表格的 字段 选择对应的对齐方式
                format_method = format_dict.get(columns[key], str.center)
            bits[index].append(
                " " + format_method(line, widths[key] - 2) + " ")

            # 添加 分隔符
            bits[index].append('|')

    for index, lines in enumerate(bits):
        bits[index] = ''.join(lines)

    return '\n'.join(bits)


def format_data(data):
    """
    将 从sql 中查询的数据 转化为 unicode
    :param data: 为list 元素为 查询获得的字典
    :return:
    """
    new_data = deepcopy(data)
    for line in new_data:
        for key in line:
            line[key] = str(line[key])
    return new_data


def print_table(data, prt_type_fields, max_width=40, title=None):
    lines = []
    formatted_data = format_data(data)
    widths = get_cells_widths(prt_type_fields, formatted_data, max_width)
    table_len = sum(widths.values()) + len(prt_type_fields)  # 表格的最大长度

    if title is not None:
        lines.append(title.center(table_len))

    # 格式化 表头
    formatted_fields = format_row(
        prt_type_fields,
        dict(zip(prt_type_fields, prt_type_fields)),
        widths
    )
    lines.append(formatted_fields)

    for cluster_info in formatted_data:
        formatted_row = format_row(prt_type_fields, cluster_info, widths)
        lines.append(formatted_row)
    hbar = table_len * '-'
    mid_bar = f'\n{hbar}\n'
    print(mid_bar.join(lines))
    print("-" * table_len)


def format_rows(rows):
    try:
        max_len_dict = {}
        for row in rows:
            for k, v in row.items():
                if max_len_dict.get(k, 1) < len(v):
                    max_len_dict[k] = len(v)
        for row in rows:
            for k, v in row.items():
                if max_len_dict.get(k, 1) > 1:
                    row[k] = v.center(max_len_dict.get(k), ' ')
    except Exception as e:
        print(repr(e))
        return rows
    return rows


def get_current_time_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_my_ip():
    # 获取自己的ip地址
    nic_dict = ip_lib.get_nic_ip_dict()
    my_ip, my_mac = ip_lib.get_ip_in_network(nic_dict, config.get('network'))
    if not my_ip:
        my_ip = '127.0.0.1'
    return my_ip, my_mac


# ---------------20221124适配cinspect---------------

def reformat_log_data(data, data_format='table', separator=':'):
    """make shell ouput data into dict"""
    if data_format == 'table':
        results = list()
        keys = None
        first_value = None
        for line in data:
            if not line:
                continue

            if str(line).startswith(("select", "SELECT")):
                continue
            if keys is None:
                # when separator is ' ' need to remove the ' '
                keys = [data.strip(' ').lower().replace('%', '')
                        for data in line.strip(' ').split(separator)
                        if data]
            else:
                tmp_list = [data.strip(' ')
                            for data in line.strip(' ').split(separator)
                            if data]

                if len(tmp_list) == 1:
                    first_value = tmp_list[0]
                    continue
                if first_value:
                    tmp_list.insert(0, first_value)
                    first_value = None
                tmp_list += [None for i in range(len(keys) - len(tmp_list))]
                results.append(dict(zip(keys, tmp_list)))

    elif data_format == 'dict':
        results = dict()
        for line in data:
            if separator not in str(line):
                continue
            if " " in separator:
                tmp_data = [data for data in line.split(separator) if data]
                if 'Free' in tmp_data or len(tmp_data) > 2:
                    key = separator.join(tmp_data[:2])
                    value = tmp_data[2]
                else:
                    try:
                        key, value = tmp_data
                    except ValueError:
                        key = tmp_data[0]
                        value = ""
            else:
                key, value = line.split(separator, 1)
            results[key.strip(' \t\n').lower()] = value.strip(' \t\n')
    # 将表格第一个参数作为字典
    elif data_format == 'name_table':
        results = dict()
        keys = None
        for line in data:
            if separator not in line:
                continue
            if keys is None:
                keys = [data.strip(' ')
                        for data in line.strip(' ').split(separator)
                        if data]
            else:
                if ' ' in separator:
                    tmp_list = [data.strip(' :').lower()
                                for data in line.strip(' ').split(separator)
                                if data]
                else:
                    tmp_list = [data.strip(' :').lower()
                                for data in line.strip(' ').split(separator)
                                ]
                if len(keys) == len(tmp_list):
                    tmp_dict = dict(zip(keys[1:], tmp_list[1:]))
                else:
                    tmp_dict = dict(zip(keys, tmp_list[1:]))
                results.update({tmp_list[0]: tmp_dict})
    elif data_format == 'replicate_name_table':
        results = list()
        tmp_list = []
        sep_idx_list = None
        for line in data:
            # 跳过第一行分类数据
            if line.strip().startswith('----'):
                continue
            if separator not in line:
                continue
            if sep_idx_list is None:
                sep_idx = -1
                sep_idx_list = [0]
                for i in range(line.count(separator)):
                    sep_idx = line.index(separator, sep_idx + 1)
                    sep_idx_list.append(sep_idx)
                # print(list(zip(sep_idx_list, sep_idx_list[1:] + [None])))

            # values = [value.strip() for value in line.strip('| ').split(separator)]
            values = [line[i:j].strip(' |')
                      for i, j in zip(sep_idx_list, sep_idx_list[1:] + [None])]
            # a | b | c
            #   | d | e
            if values[0].strip():
                # print(values)
                # print("==="* 20)
                tmp_list = values
                results.append(tmp_list)
            else:
                for idx, value in enumerate(values):
                    if not value.strip():
                        continue
                    try:
                        tmp_list[idx] += ('\n' + value.rstrip('+').strip())
                    except IndexError:
                        print(line)
                        raise
    else:
        print(data)
        raise TypeError
    return results
