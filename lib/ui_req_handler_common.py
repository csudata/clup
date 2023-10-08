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
@description: WEB界面的通用后端服务处理模块
"""

import hashlib
import json

import cluster_state
import config
import csu_http
import dbapi
import node_state
import rpc_utils


def gen_mac(ip):
    hv = hashlib.md5(ip).hexdigest()
    return f"52:54:00:{hv[0:2]}:{hv[2:4]}:{hv[4:6]}"


def get_dict_list(req):
    """
    获得各种字典
    :param req:
    :return:
    """

    params = {
        'dict_type_list': csu_http.MANDATORY,
    }

    # 检查参数的合法性，如果成功，把参数放到一个字典中
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict

    dict_type_list = pdict['dict_type_list'].split(',')
    dict_type_list = [k.strip() for k in dict_type_list]
    ret_dict_data = {}
    for dict_type in dict_type_list:
        if dict_type == 'cluster_state':
            ret_data = cluster_state.get_dict()
            ret_dict_data[dict_type] = ret_data
        elif dict_type == 'db_state':
            ret_data = node_state.get_dict()
            ret_dict_data[dict_type] = ret_data
        elif dict_type == 'host_state':
            ret_data = {"1": "Normal", "2": "仅VIP存在"}
            ret_dict_data[dict_type] = ret_data
        elif dict_type == 'cluster_type':
            ret_data = {"1": "流复制", "2": "共享磁盘"}
            ret_dict_data[dict_type] = ret_data
        else:
            return 400, f"Invalid dict_type: {pdict['dict_type']} !"
    raw_data = json.dumps(ret_dict_data)
    return 200, raw_data


def get_dashboard(req):
    cluster_stats = {}
    clu_state_mapping = cluster_state.get_dict()
    for state in clu_state_mapping:
        str_state = cluster_state.to_str(state)
        cluster_stats[str_state] = 0

    with dbapi.DBProcess() as dbp:
        sql = "select state, count(*) cnt from clup_cluster group by state"
        c_rows = dbp.query(sql)

        sql = "select ip from clup_host"
        h_rows = dbp.query(sql)

        sql = "select db_state, count(*) as cnt from clup_db group by db_state;"
        d_rows = dbp.query(sql)

    db_stats = {"total": 0, "startup": 0, "stop": 0, "failed": 0, "abnormal": 0}
    for row in d_rows:
        if row['db_state'] == -1:
            db_stats["abnormal"] += row['cnt']
        elif row['db_state'] == 0:
            db_stats["startup"] += row['cnt']
        elif row['db_state'] == 1:
            db_stats["stop"] += row['cnt']
        elif row['db_state'] == 4:
            db_stats["failed"] += row['cnt']
        db_stats["total"] += row['cnt']

    for row in c_rows:
        state = row['state']
        str_state = cluster_state.to_str(state)
        cluster_stats[str_state] = row['cnt']

    host_stats = {'normal': 0, 'abnormal': 0}
    for row in h_rows:
        err_code, err_msg = rpc_utils.get_rpc_connect(row['ip'], conn_timeout=2)
        if err_code == 0:
            rpc = err_msg
            rpc.close()
            host_stats['normal'] += 1
        else:
            host_stats['abnormal'] += 1

    ret_dict = {
        "cluster_stats": cluster_stats,
        "db_stats": db_stats,
        "host_stats": host_stats
    }
    return 200, json.dumps(ret_dict)


def basic_test_api(req):
    """
    用于探测clup是否正常
    @param req:
    @return:
    """
    params = {
    }
    err_code, pdict = csu_http.parse_parms(params, req)
    if err_code != 0:
        return 400, pdict
    ret = config.get('test', 'none')
    return 200, ret


if __name__ == "__main__":
    config.load()
    import sys
    argvs = sys.argv
    print(argvs)
