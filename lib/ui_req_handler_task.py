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
@description: WEB界面的任务处理后端服务模块
"""

import json

import csu_http  # pylint: disable=import-error

import general_task_mgr


def get_general_task_log(req):
    params = {
        'task_id': csu_http.MANDATORY | csu_http.INT,
        'get_task_state': csu_http.INT,
        'seq': csu_http.INT,
    }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    task_id = pdict['task_id']
    if 'get_task_state' in pdict:
        get_task_state = pdict['get_task_state']
    else:
        get_task_state = 0
    if 'seq' in pdict:
        seq = pdict['seq']
    else:
        seq = 0
    err_code, state, data = general_task_mgr.get_task_log(task_id, get_task_state, seq)
    if err_code > 0:
        return 400, data
    elif err_code < 0:
        return 500, data
    else:
        ret_data = {"state": state, "data": data}
        raw_data = json.dumps(ret_data)
        return 200, raw_data


def get_general_task_list(req):
    params = {'page_num': csu_http.MANDATORY | csu_http.INT,
              'page_size': csu_http.MANDATORY | csu_http.INT,
              'task_class': csu_http.MANDATORY | csu_http.INT,
              'state': 0,
              'cluster_id': 0,
              'task_type': 0,
              'task_name': 0,
              'search_key': 0,
              'begin_create_time': 0,
              'end_create_time': 0,
              }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    page_num = pdict['page_num']
    page_size = pdict['page_size']
    task_class = pdict['task_class']

    conds = {}
    allows_conds = ['task_id', 'search_key', 'cluster_id', 'state', 'task_type', 'task_name', 'begin_create_time', 'end_create_time']
    for k in allows_conds:
        if k in pdict:
            conds[k] = pdict[k]

    total_count, task_list_data = general_task_mgr.get_task_list(conds, page_num, page_size, task_class)
    ret_data = {"total": total_count, "page_size": pdict['page_size'], "rows": task_list_data}
    raw_data = json.dumps(ret_data)
    return 200, raw_data


def get_task_type_list_by_class(req):
    params = {
        'task_class': csu_http.MANDATORY | csu_http.INT,
    }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    task_type_list = general_task_mgr.get_task_type_list_by_class(pdict['task_class'])
    return 200, json.dumps(task_type_list)


def get_general_task_state(req):
    """
    获取目标任务id的状态
    """
    params = {
        'task_id': csu_http.MANDATORY | csu_http.INT,
    }
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    try:
        task_id = pdict['task_id']
        err_code, state = general_task_mgr.get_general_task_state(task_id)
        if err_code != 0:
            return 400, json.dumps({'state': state})
        return 200, json.dumps({'state': state})
    except Exception:
        return 400, json.dumps({'state': -1})
