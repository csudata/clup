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
@description: WEB界面的CLup自身管理接口后端服务处理模块
"""

import json
import logging
# agent_log
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import agent_logger
import config
import csu_http
import dbapi
import general_task_mgr
import general_utils
import ip_lib
import logger
import rpc_utils
import task_type_def


def get_agent_package_list(req):
    """
    获取agent包
    :param req:
    :return:
    """

    params = {}

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    err_code, package_list = general_utils.get_agent_package_list()
    if err_code != 0:
        return 400, package_list
    return 200, json.dumps(package_list)


def upgrade_agent(req):
    """
    升级agent
    hid_list: hid列表
    restart: True/False, 是否需要重启agent.
    file_list: [{
        file_name: 文件绝对路径,
        tar_path: tar包解压路径, 默认: /opt
        }]
    """
    params = {
        'hid_list': csu_http.MANDATORY,
        'restart': csu_http.MANDATORY,
        'file_list': csu_http.MANDATORY
    }
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    host_dict_list = []
    host_list = []
    for hid in pdict['hid_list']:
        sql = "SELECT ip FROM clup_host WHERE hid=%s"
        rows = dbapi.query(sql, (hid, ))
        if not rows:
            logging.error(f'No relevant information found(hid={hid}).')
            return
        host_list.append(rows[0]['ip'])
        host_dict_list.append({
            'host': rows[0]['ip'],
            'file_list': pdict['file_list']
        })

    # 使用线程执行
    task_id = general_task_mgr.create_task(task_type_def.UPDATE_AGENT, 'update-agent', {})
    general_task_mgr.run_task(task_id, general_utils.update_agent, (host_dict_list, pdict['restart']))
    ret_dict = {"task_id": task_id}
    return 200, json.dumps(ret_dict)


def update_agent_conf(req):
    """
    更新agent配置文件
    host_list: 主机列表
    server_address: 修改的clup服务地址
    """
    params = {
        'host_list': csu_http.MANDATORY,
        'server_address': csu_http.MANDATORY
    }
    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    host_list = pdict['host_list']
    if len(host_list) == 0:
        return 400, 'The host list is not allowed to be empty.'
    user_name = req.session_data['user_name']
    # 使用线程执行
    task_id = general_task_mgr.create_task(task_type_def.UPDATE_AGENT_CONF, 'update-agent-conf', {})
    general_task_mgr.run_task(task_id, general_utils.update_agent_conf, (host_list, pdict['server_address'], user_name))
    ret_dict = {"task_id": task_id}
    return 200, json.dumps(ret_dict)


def get_clup_status(req):
    """
    获取集群版clup状态
    """
    params = {}
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    if not config.get('clup_host_list'):
        return 200, '-1'
    return 200, 'online'


def get_clup_host_list(req):
    """
    获取clup主机列表
    """
    params = {}
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    # 获取自己的ip地址
    nic_dict = ip_lib.get_nic_ip_dict()
    my_ip, _my_mac = ip_lib.get_ip_in_network(nic_dict, config.get('network'))
    if not my_ip:
        my_ip = '127.0.0.1'
        logging.error(f"In clup.conf network is {config.get('network')}, but this machine not in this network")

    ret = []
    clup = True
    host = my_ip
    rpc_port = config.get('server_rpc_port', 4342)

    # 连接csumdb检查数据库是否正常
    csumdb_is_running = True
    try:
        conn = dbapi.connect_db(host)
        conn.close()
    except Exception as e:
        logging.error(f"({host}) csumdb connect failed: {repr(e)}")
        csumdb_is_running = False

    url = None
    ret.append({
        'host': host,
        'port': rpc_port,
        'csumdb': csumdb_is_running,
        'url': url,
        'clup': clup,
        'primary': True
    })
    return 200, json.dumps(ret)


def get_log_level_list(req):
    params = {
        'page_num': csu_http.MANDATORY | csu_http.INT,
        'page_size': csu_http.MANDATORY | csu_http.INT,
        'filter': 0,
    }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    log_level_name_dict = logger.get_log_level_name_dict()

    log_type_list = logger.get_log_type_list()
    rows = []
    for log_type in log_type_list:
        row = {}
        try:
            tmp_logger = logging.getLogger(log_type)
            row['log_type'] = log_type if log_type else 'main'
            row['level'] = tmp_logger.level
            row['level_name'] = log_level_name_dict.get(tmp_logger.level, str(tmp_logger.level))
        except Exception:
            row['level'] = -1
            row['level_name'] = 'unknown'
            row['log_type'] = log_type if log_type else 'main'
        rows.append(row)

    ret_data = {"total": len(rows), "page_size": pdict['page_size'],
                "rows": rows}

    raw_data = json.dumps(ret_data)
    return 200, raw_data


def set_log_level(req):
    params = {
        'log_type': csu_http.MANDATORY,
        'level_name': csu_http.MANDATORY
    }
    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    log_type = pdict['log_type']
    if log_type == 'main':
        log_type = ''
    log_type_list = logger.get_log_type_list()
    if log_type not in log_type_list:
        return 400, f"log_type({log_type} not in {log_type_list}"

    log_level_name = pdict['level_name']
    log_level_dict = logger.get_log_level_dict()
    if log_level_name not in log_level_dict:
        return 400, f"level_name({log_level_name} not in {log_level_dict.keys()}"
    log_level = log_level_dict[log_level_name]
    tmp_logger = logging.getLogger(log_type)
    tmp_logger.setLevel(log_level)
    return 200, 'ok'


def get_agent_log_level_list(req):
    params = {
        'page_num': csu_http.MANDATORY | csu_http.INT,
        'page_size': csu_http.MANDATORY | csu_http.INT,
        'filter': 0,
    }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    where_cond = ''
    if 'filter' in pdict:
        if pdict["filter"]:
            where_cond = (' WHERE ip like %(filter)s')
    sql = 'SELECT count(*) AS cnt FROM clup_host'
    sql += where_cond
    rows = dbapi.query(sql, pdict)
    row_cnt = rows[0]['cnt']

    log_type_list = agent_logger.get_log_type_list()

    log_level_name_dict = agent_logger.get_log_level_name_dict()
    log_type_cnt = len(log_type_list)
    total_cnt = row_cnt * log_type_cnt

    sql_arg = {}

    sql_arg['offset'] = (pdict['page_num'] - 1) * pdict['page_size'] // log_type_cnt
    # 结束的行号
    end_pos = math.ceil(pdict['page_num'] * pdict['page_size'] / log_type_cnt)
    sql_arg['page_size'] = end_pos - sql_arg['offset'] + 1
    if where_cond:
        sql_arg['filter'] = pdict['filter']
        sql = 'SELECT * FROM clup_host WHERE ip like %(filter)s' \
            ' order by clup_host.hid' \
            ' offset %(offset)s limit %(page_size)s'
    else:
        sql = 'SELECT * FROM clup_host ' \
            ' order by clup_host.hid' \
            ' offset %(offset)s limit %(page_size)s'
    rows = dbapi.query(sql, sql_arg)

    # 应该开始的行位置
    start_pos = (pdict['page_num'] - 1) * pdict['page_size']
    # 循环开始的行位置
    loop_pos = sql_arg['offset'] * log_type_cnt
    # 跳过的行
    skip_cnt = start_pos - loop_pos

    pool = ThreadPoolExecutor(10)
    task_dict = {}
    for row in rows:
        task = pool.submit(agent_logger.query_agent_log_level, row['ip'], log_type_list)
        task_dict[task] = row

    for task in as_completed(task_dict.keys()):
        agent_log_level_dict = task.result()
        row = task_dict[task]
        row['log_level_dict'] = agent_log_level_dict
    pool.shutdown()


    ret_rows = []
    i = 0
    exit_loop = False
    for row in rows:
        for log_type in log_type_list:
            if i >= skip_cnt:
                ret_row = {}
                ret_row['ip'] = row['ip']
                ret_row['log_type'] = log_type if log_type else 'main'
                ret_row['level'] = row['log_level_dict'][log_type]
                ret_row['level_name'] = log_level_name_dict.get(ret_row['level'], str(ret_row['level']))
                ret_rows.append(ret_row)
                if len(ret_rows) == pdict['page_size']:
                    exit_loop = True
                    break
            i += 1
        if exit_loop:
            break

    ret_data = {
        "total": total_cnt,
        "page_size": pdict['page_size'],
        "rows": ret_rows
    }

    raw_data = json.dumps(ret_data)
    return 200, raw_data


def set_agent_log_level(req):
    params = {
        'ip': csu_http.MANDATORY,
        'log_type': csu_http.MANDATORY,
        'level_name': csu_http.MANDATORY
    }
    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    log_type = pdict['log_type']
    if log_type == 'main':
        log_type = ''
    log_type_list = logger.get_log_type_list()
    if log_type not in log_type_list:
        return 400, f"log_type({log_type} not in {log_type_list}"
    log_level_name = pdict['level_name']
    log_level_dict = agent_logger.get_log_level_dict()
    if log_level_name not in log_level_dict:
        return 400, f"level_name({log_level_name} not in {log_level_dict.keys()}"
    log_level = log_level_dict[log_level_name]

    err_code, err_msg = rpc_utils.get_rpc_connect(pdict['ip'], 1)
    if err_code != 0:
        return 400, err_msg
    rpc = err_msg
    try:
        err_code, err_msg = rpc.set_log_level(log_type, log_level)
        if err_code != 0:
            return 400, err_msg
        else:
            return 200, 'ok'
    except Exception as e:
        return 400, str(e)
    finally:
        rpc.close()


# 获取clup_settings中的值
def get_clup_settings(req):
    params = {
        'page_num': csu_http.MANDATORY | csu_http.INT,
        'page_size': csu_http.MANDATORY | csu_http.INT,
        'filter': 0,
    }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    try:
        where_cond = ' WHERE category >=10'
        if 'filter' in pdict:
            if pdict["filter"]:
                where_cond += ' AND key like %(filter)s'
        sql = f'SELECT count(*) AS cnt FROM clup_settings {where_cond}'
        rows = dbapi.query(sql, pdict)
        row_cnt = rows[0]['cnt']

        pdict['offset'] = (pdict['page_num'] - 1) * pdict['page_size']
        sql = f'SELECT * FROM clup_settings {where_cond}' \
            ' order by key' \
            ' offset %(offset)s limit %(page_size)s'
        rows = dbapi.query(sql, pdict)
    except Exception as e:
        return 400, str(e)


    ret_data = {"total": row_cnt, "page_size": pdict['page_size'], "rows": rows}
    row_data = json.dumps(ret_data)
    return 200, row_data


# 更新clup_settings设置
def update_clup_settings(req):
    params = {
        'key': csu_http.MANDATORY,
        'content': csu_http.MANDATORY,
    }
    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    try:
        sql = "UPDATE clup_settings SET content=%(content)s WHERE key=%(key)s"
        dbapi.execute(sql, pdict)
    except Exception as e:
        return 400, str(e)
    config.set_key(pdict['key'], pdict['content'])

    return 200, 'Update success'


if __name__ == '__main__':
    pass
