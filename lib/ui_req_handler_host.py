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
@description: WEB界面的主机管理后端服务模块
"""

import json
import logging

import csu_http
import dbapi
import rpc_utils


def get_host_list(req):
    params = {'page_num': csu_http.MANDATORY | csu_http.INT,
              'page_size': csu_http.MANDATORY | csu_http.INT,
              'state': 0,
              'filter': 0,
              }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    page_num = pdict['page_num']
    page_size = pdict['page_size']

    if 'filter' in pdict:
        filter_cond = pdict['filter']
    else:
        filter_cond = ''

    offset = (page_num - 1) * page_size

    where_cond = ""
    if pdict.get('filter', None):
        filter_cond = filter_cond.replace("'", "")
        filter_cond = filter_cond.replace('"', "")
        where_cond = "where (ip like %(filter_cond)s or data->>'hostname' like %(filter_cond)s )"
    args = {"filter_cond": filter_cond}
    with dbapi.DBProcess() as dbp:
        sql = "SELECT count(*) as cnt FROM clup_host " + where_cond
        rows = dbp.query(sql, args)
        row_cnt = rows[0]['cnt']
        ret_rows = []
        if row_cnt > 0:
            args['limit'] = page_size
            args['offset'] = offset
            sql = "SELECT * FROM clup_host {} ORDER BY ip LIMIT %(limit)s OFFSET %(offset)s".format(where_cond)
            ret_rows = dbp.query(sql, args)
            for row in ret_rows:
                data = row['data']
                attr_dict = data
                attr_dict.pop('ip', None)
                row.update(attr_dict)

    # leifliu 添加机器状态查询，只查询Down掉的机器
    down_rows = []
    for row in ret_rows:
        err_code, err_msg = rpc_utils.get_rpc_connect(row['ip'], conn_timeout=2)
        if err_code == 0:
            rpc = err_msg
            try:
                row['version'] = rpc.get_agent_version()
            except Exception as e:
                logging.info(f'Failed to retrieve version: {repr(e)}')
                row['version'] = ''
            finally:
                rpc.close()
            row['state'] = 1
        else:
            row['state'] = -1
            if "state" in pdict:
                down_rows.append(row)
    if "state" in pdict:
        ret_rows = down_rows
    ret_data = {"total": row_cnt, "page_size": pdict['page_size'], "rows": ret_rows}
    raw_data = json.dumps(ret_data)
    return 200, raw_data


def remove_host(req):
    params = {
        'ip': csu_http.MANDATORY,
    }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    sql = "SELECT hid FROM clup_host WHERE ip=%(ip)s"
    rows = dbapi.query(sql, pdict)
    if len(rows) < 1:
        ret_data = '0'
    else:
        ret_data = '1'

        # 检查是否还有相关的数据库
        check_db_sql = "SELECT db_id FROM clup_db WHERE host = %(ip)s"
        db_rows = dbapi.query(check_db_sql, pdict)
        if len(db_rows):
            db_id_list = [row['db_id'] for row in db_rows]
            return 400, f"There are databases(db_id in {db_id_list}) present on the host(ip=({pdict['ip']})), please delete the databases before taking the host offline."

        # 移除机器
        dbapi.execute("DELETE FROM clup_host WHERE ip=%(ip)s", pdict)

    return 200, ret_data


def check_os_user_exists(req):
    """
    检查操作系统上指定的用户是否存在
    """
    param = {
        "host": csu_http.MANDATORY,
        "user": csu_http.MANDATORY,
    }
    code_msg, pdict = csu_http.parse_parms(param, req)
    if code_msg != 0:
        return 400, pdict

    host = pdict['host']
    user = pdict['user']
    err_code, err_msg = rpc_utils.get_rpc_connect(host)
    if err_code != 0:
        err_msg = f"Unable to verify if the user(user={user}) exists because the host(host={host}) cannot be connected."
        logging.info(err_msg + f" *** {err_msg}")
        return 200, json.dumps({"err_code": -1, "err_msg": err_msg})
    rpc = err_msg
    try:
        uid = rpc.os_user_exists(user)
    finally:
        rpc.close()
    return 200, json.dumps({"err_code": 0, "exists": uid})


def check_os_uid_exists(req):
    """
    检查操作系统上指定的用户是否存在
    """
    param = {
        "host": csu_http.MANDATORY,
        "os_uid": csu_http.MANDATORY | csu_http.INT,
    }
    code_msg, pdict = csu_http.parse_parms(param, req)
    if code_msg != 0:
        return 400, pdict

    host = pdict['host']
    os_uid = pdict['os_uid']
    err_code, err_msg = rpc_utils.get_rpc_connect(host)
    if err_code != 0:
        err_msg = f"Unable to verify the user's UID({os_uid}) because the host({host}) cannot be connected."
        logging.info(err_msg + f" *** {err_msg}")
        return 200, json.dumps({"err_code": -1, "err_msg": err_msg})
    rpc = err_msg
    try:
        exists = rpc.os_uid_exists(os_uid)
    finally:
        rpc.close()
    return 200, json.dumps({"err_code": 0, "exists": exists})


def check_path_is_dir(req):
    param = {
        "host": csu_http.MANDATORY,
        "path": csu_http.MANDATORY,
    }
    code_msg, pdict = csu_http.parse_parms(param, req)
    if code_msg != 0:
        return 400, pdict

    host = pdict['host']
    path = pdict['path']
    err_code, err_msg = rpc_utils.get_rpc_connect(host)
    if err_code != 0:
        err_msg = f"Unable to verify if the directory({path}) exists because the host({host}) cannot be connected."
        logging.info(err_msg + f" *** {err_msg}")
        return 200, json.dumps({"err_code": -1, "err_msg": err_msg})
    rpc = err_msg
    try:
        if not rpc.path_is_dir(path):
            return 200, json.dumps({"err_code": -1, "err_msg": "This path does not exist or is not a directory!"})
    finally:
        rpc.close()
    return 200, json.dumps({"err_code": 0, "err_msg": ""})


def check_port_is_used(req):
    """
    检查主机上端口是否已备占用
    """
    param = {
        "host": csu_http.MANDATORY,
        "port": csu_http.MANDATORY | csu_http.INT,
    }
    code_msg, pdict = csu_http.parse_parms(param, req)
    if code_msg != 0:
        return 400, pdict

    host = pdict['host']
    port = pdict['port']
    err_code, err_msg = rpc_utils.get_rpc_connect(host)
    if err_code != 0:
        err_msg = f"Unable to verify if the port({port}) is in use because the host({host}) cannot be connected."
        return 200, json.dumps({"err_code": -1, "err_msg": err_msg})
    rpc = err_msg
    try:
        err_code, is_port_used = rpc.check_port_used(port)
        if err_code != 0:
            err_msg = f"check host({pdict['host']} whether the port({port}) is used faile: {is_port_used}"
            return 200, json.dumps({"err_code": -1, "err_msg": err_msg})
    finally:
        rpc.close()
    if is_port_used:
        return 200, json.dumps({"err_code": 0, "is_used": 1})
    else:
        return 200, json.dumps({"err_code": 0, "is_used": 0})


# 检查polardb共享存储目录是否存在
def check_polar_shared_dirs(req):
    param = {
        "host": csu_http.MANDATORY,
        "pfs_disk_name": csu_http.MANDATORY,
        "polar_datadir": csu_http.MANDATORY
    }
    code_msg, pdict = csu_http.parse_parms(param, req)
    if code_msg != 0:
        return 400, pdict

    host = pdict['host']
    pfs_disk_name = pdict['pfs_disk_name']
    polar_datadir = pdict['polar_datadir']

    if "/" in pfs_disk_name or "/" in polar_datadir:
        return 400, "The input is incorrect. Please remove the '/' and only input the name of pfs_disk_name or polar_datadir."
    try:
        rpc = None
        check_disk_cmd = f"pfs -C disk ls /{pfs_disk_name}/"
        check_datadir_cmd = f"pfs -C disk ls /{pfs_disk_name}/{polar_datadir}"
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            return 200, json.dumps({"err_code": -1, "err_msg": f"Unable to connect to the host({host}), so it's not possible to check the shared disks and database directories."})
        rpc = err_msg
        # 检查共享磁盘是否存在
        err_code, err_msg, _out_msg = rpc.run_cmd_result(check_disk_cmd)
        if err_code != 0 and err_code != 255:
            if err_msg == '':
                err_msg = f'the disk ({pfs_disk_name}) maybe not mkfs.'
            return 200, json.dumps({"err_code": -1, "err_msg": f"Error occurred while checking shared disks, err_msg={err_msg};"})
        elif err_code == 255:
            return 200, json.dumps({"err_code": -1, "err_msg": f"The shared disk directory({pfs_disk_name}) does not exist, please check and try again."})

        # 检查数据目录是否存在，目的是要不存在，即返回码为255
        err_code, err_msg, _out_msg = rpc.run_cmd_result(check_datadir_cmd)
        if err_code == 0:
            return 200, json.dumps({"err_code": -1, "err_msg": f"The data directory already exists, please check and manually delete it. Here's a reference command:'pfs -C disk rm -r /{pfs_disk_name}/{polar_datadir}'"})
        elif err_code != 255:
            return 200, json.dumps({"err_code": -1, "err_msg": f"Check for data directory failed, err_msg={err_msg};"})
        return 200, json.dumps({"err_code": 0, "is_used": 0})
    except Exception as e:
        return 400, str(e)
    finally:
        if rpc:
            rpc.close()
