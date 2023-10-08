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
@Author: liulei
@description: PolarDB的数据库操作模块
"""

import copy
import json
import logging
import traceback

import dao
import database_state
import db_encrypt
import dbapi
import general_task_mgr
import long_term_task
import pg_db_lib
import pg_helpers
import polar_lib
import rpc_utils
import task_type_def


def build_polar_reader(pdict):
    """搭建polardb reader
    Args:
        pdict (dict): 搭建数据库的参数

    Returns:
        int: err_code
        str: err_msg
        int: task_id
        int: db_id
    """
    err_code = 0
    err_msg = ''

    try:
        sql = f""" SELECT count(*) FROM clup_db WHERE host = '{pdict['host']}' AND port= {pdict['port']} """
        rows = dbapi.query(sql)
        if rows[0]['count'] > 0:
            return -1, f"Create failed. Port {pdict['port']} is already in use by a database on the host({pdict['host']}).", 0, 0

        # 查询上级库的信息
        sql = "SELECT db_id,up_db_id, cluster_id, host, repl_app_name, repl_ip, port,pgdata,db_type," \
            "clup_cluster.state as state," \
            "db_detail->>'db_user' as db_user, db_detail->>'db_pass' as db_pass," \
            " db_detail->>'instance_type' as instance_type,"\
            " db_detail->'repl_ip' as repl_ip_in_detail, db_detail " \
            " FROM clup_db left join clup_cluster using (cluster_id) WHERE db_id = %s "
        rows = dbapi.query(sql, (pdict['up_db_id'], ))
        if len(rows) == 0:
            return -1, 'The superior database instance does not exist. Please try again.', 0, 0
        if rows[0]['cluster_id'] and rows[0]['state'] == 1:
            return -1, f"The superior database is in the cluster[cluster_id: {rows[0]['cluster_id']}], please take it offline before operating.", 0, 0

        # leifliu Test 更新上级库信息，添加repl_user、repl_pass
        update_dict = json.dumps({"repl_user": pdict['repl_user'], "repl_pass": pdict['repl_pass']})
        sql = "UPDATE clup_db SET db_detail = db_detail || (%s::jsonb) WHERE db_id = %s"
        dbapi.execute(sql, (update_dict, pdict['up_db_id']))

        up_db_dict = rows[0]
        err_code, err_msg = pg_helpers.get_pg_setting_list(pdict['up_db_id'])
        if err_code != 0:
            return err_code, f"Unable to obtain the configuration of the source database:{err_msg}", 0, 0
        up_db_dict['setting_list'] = err_msg

        # db_dict存储往表clup_db中插入的数据
        db_dict = copy.copy(pdict)
        # db_detail放clup_db.db_detail字段的数据
        db_detail = {
            'os_user': pdict['os_user'],
            'os_uid': pdict['os_uid'],
            'db_user': up_db_dict['db_user'],
            'db_pass': up_db_dict['db_pass'],
            'instance_type': pdict['instance_type'],
            'repl_user': pdict['repl_user'],
            'repl_pass': pdict['repl_pass'],
            'version': pdict['version'],
            'polar_type': 'reader',
            'pg_bin_path': pdict['pg_bin_path'],
            'pfs_disk_name': up_db_dict['db_detail']["pfs_disk_name"],
            'pfsdaemon_params': up_db_dict['db_detail']['pfsdaemon_params'],
            'polar_datadir': up_db_dict['db_detail']['polar_datadir']
        }
        if pdict.get('delay'):
            db_detail['delay'] = pdict['delay']
        if pdict.get('tblspc_dir'):
            db_detail['tblspc_dir'] = pdict['tblspc_dir']

        # 获取集群的polar_hostid
        err_code, err_msg = polar_lib.get_cluster_polar_hostid(up_db_dict['cluster_id'])
        if err_code != 0:
            return -1, err_msg
        if not err_msg.get('polar_hostid'):
            return -1, f"Failed to get polar_hostid, please modify the polar_hostid in the cluster(cluster_id={up_db_dict['cluster_id']}) information first."
        polar_hostid = int(err_msg['polar_hostid'])
        db_detail['polar_hostid'] = polar_hostid

        db_dict['db_detail'] = json.dumps(db_detail)
        db_dict['repl_app_name'] = pdict['repl_ip']
        db_dict['up_db_id'] = pdict['up_db_id']
        db_dict['up_db_host'] = up_db_dict['host']
        db_dict['up_db_repl_ip'] = up_db_dict['repl_ip_in_detail'] if up_db_dict['repl_ip_in_detail'] else up_db_dict['repl_ip']
        db_dict['up_db_port'] = up_db_dict['port']
        db_dict['cluster_id'] = up_db_dict['cluster_id']
        db_dict["db_type"] = up_db_dict["db_type"]
        db_dict['is_primary'] = 0
        db_dict['state'] = 1  # 注意这是HA的状态，不是数据库的状态
        if 'instance_name' not in pdict:
            db_dict['instance_name'] = ''
        db_dict['db_state'] = database_state.CREATING      # 数据库状态默认为创建中

        try:
            # 先插入数据库生成id
            sql = "INSERT INTO clup_db (cluster_id, state, pgdata, is_primary, repl_app_name, host, repl_ip, " \
                "instance_name, db_detail, port, db_state, up_db_id, db_type) values " \
                "(%(cluster_id)s, %(state)s, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, %(repl_ip)s, "\
                "%(instance_name)s, %(db_detail)s, %(port)s, %(db_state)s, %(up_db_id)s, %(db_type)s) RETURNING db_id"
            rows = dbapi.query(sql, db_dict)
        except Exception as e:
            return -1, f'Failed to insert data into clup_db: {str(e)}', 0, 0
        if len(rows) == 0:
            return -1, 'Failed to insert data into clup_db:', 0, 0
        # 更新集群的polar_hostid
        polar_lib.update_cluster_polar_hostid(db_dict['cluster_id'], polar_hostid + 1)

        db_id = rows[0]['db_id']
        db_dict['db_id'] = db_id

        # rpc_dict 放下创建备库的调用的参数

        db_dict['polar_hostid'] = polar_hostid
        db_dict['primary_slot_name'] = f"replica{polar_hostid}"
        db_dict['pfs_disk_name'] = db_detail['pfs_disk_name']
        db_dict['pfsdaemon_params'] = db_detail['pfsdaemon_params']

        rpc_dict = db_dict
        # 数据库用户，当db_user禹os_user不相同是，需要在pg_hba.conf中加用户映射，否则本地无法误密码的登录数据库
        rpc_dict['db_user'] = up_db_dict['db_user']
        rpc_dict['db_pass'] = up_db_dict['db_pass']
        rpc_dict['up_db_host'] = up_db_dict['host']
        rpc_dict['up_db_pgdata'] = up_db_dict['pgdata']
        rpc_dict['repl_pass'] = rpc_dict['repl_pass']
        rpc_dict["other_param"] = pdict["other_param"]

        # 开始搭建只读节点
        task_name = f"build polardb reader(db={db_dict['host']}:{db_dict['port']})"
        rpc_dict['task_name'] = task_name
        # rpc_dict['task_key'] = task_name + str(db_dict['db_id'])

        task_id = general_task_mgr.create_task(
            task_type=task_type_def.PG_BUILD_STANDBY_TASK,
            task_name=task_name,
            task_data=rpc_dict
        )
        try:
            rpc_dict['task_id'] = task_id
            rpc_dict['pre_msg'] = f"Build reader(db_id={db_dict['db_id']})"
            host = db_dict['host']
            general_task_mgr.run_task(task_id, long_term_task.task_build_polar_reader, (host, db_id, rpc_dict))
            return 0, '', task_id, db_id
        except Exception as e:
            err_msg = str(e)
            general_task_mgr.complete_task(task_id, -1, err_msg)
            dao.update_db_state(db_dict['db_id'], database_state.FAULT)
        return -1, err_msg, task_id, db_id
    except Exception as e:
        err_msg = traceback.format_exc()
        logging.error(f"Build reader unexpect error: {err_msg}")
        return -1, err_msg, 0, 0


def build_polar_standby(pdict):
    """搭建polardb standby节点
    Args:
        pdict (dict): 搭建数据库的参数

    Returns:
        int: err_code
        str: err_msg
        int: task_id
        int: db_id
    """
    err_code = 0
    err_msg = ''

    try:
        sql = f""" SELECT count(*) FROM clup_db WHERE host = '{pdict['host']}' AND port= {pdict['port']} """
        rows = dbapi.query(sql)
        if rows[0]['count'] > 0:
            return -1, f"Create failed. Port {pdict['port']} is already in use by a database on the host({pdict['host']}).", 0, 0

        # 查询上级库的信息
        sql = "SELECT db_id, up_db_id, cluster_id, host, repl_app_name, repl_ip, port,pgdata,db_type," \
            " clup_cluster.state as state," \
            " db_detail->>'db_user' as db_user, db_detail->>'db_pass' as db_pass," \
            " db_detail->>'instance_type' as instance_type,"\
            " db_detail->'repl_ip' as repl_ip_in_detail, db_detail " \
            " FROM clup_db left join clup_cluster using (cluster_id) WHERE db_id = %s "
        rows = dbapi.query(sql, (pdict['up_db_id'], ))
        if len(rows) == 0:
            return -1, 'The superior database instance does not exist. Please try again', 0, 0
        if rows[0]['cluster_id'] and rows[0]['state'] == 1:
            return -1, f"The superior database is in the cluster([cluster_id: {rows[0]['cluster_id']}]), please take it offline before operation.", 0, 0

        polar_type = polar_lib.get_polar_type(pdict["up_db_id"])
        if polar_type != "master":
            return -1, f"The source database({pdict['up_db_id']}) is not an RW node and cannot be used to build a standby database.", 0, 0

        # leifliu Test 更新上级库信息，添加repl_user、repl_pass
        update_dict = json.dumps({"repl_user": pdict['repl_user'], "repl_pass": pdict['repl_pass']})
        sql = "UPDATE clup_db SET db_detail = db_detail || (%s::jsonb) WHERE db_id = %s"
        dbapi.execute(sql, (update_dict, pdict['up_db_id']))

        up_db_dict = rows[0]
        err_code, err_msg = pg_helpers.get_pg_setting_list(pdict['up_db_id'])
        if err_code != 0:
            return err_code, f"Unable to obtain the configuration of the source database:{err_msg}", 0, 0
        up_db_dict['setting_list'] = err_msg

        # db_dict存储往表clup_db中插入的数据
        db_dict = copy.copy(pdict)
        # db_detail放clup_db.db_detail字段的数据
        db_detail = {
            'os_user': pdict['os_user'],
            'os_uid': pdict['os_uid'],
            'db_user': up_db_dict['db_user'],
            'db_pass': up_db_dict['db_pass'],
            'instance_type': pdict['instance_type'],
            'repl_user': pdict['repl_user'],
            'repl_pass': pdict['repl_pass'],
            'version': pdict['version'],
            'polar_type': 'standby',
            'pg_bin_path': pdict['pg_bin_path'],
            'polar_datadir': pdict.get('polar_datadir', 'polar_shared_data')
        }
        if pdict.get('delay'):
            db_detail['delay'] = pdict['delay']
        if pdict.get('tblspc_dir'):
            db_detail['tblspc_dir'] = pdict['tblspc_dir']

        # 获取集群的polar_hostid
        err_code, err_msg = polar_lib.get_cluster_polar_hostid(up_db_dict['cluster_id'])
        if err_code != 0:
            return -1, err_msg
        if not err_msg.get('polar_hostid'):
            return -1, f"Failed to get polar_hostid, please modify the polar_hostid in the cluster(cluster_id={up_db_dict['cluster_id']}) information first."
        polar_hostid = int(err_msg['polar_hostid'])
        db_detail['polar_hostid'] = polar_hostid

        db_dict['db_detail'] = json.dumps(db_detail)
        db_dict['repl_app_name'] = pdict['repl_ip']
        db_dict['up_db_host'] = up_db_dict['host']
        db_dict['up_db_repl_ip'] = up_db_dict['repl_ip_in_detail'] if up_db_dict['repl_ip_in_detail'] else up_db_dict['repl_ip']
        db_dict['up_db_port'] = up_db_dict['port']
        db_dict['cluster_id'] = up_db_dict['cluster_id']
        db_dict["db_type"] = up_db_dict["db_type"]
        db_dict['is_primary'] = 0
        db_dict['state'] = 1  # 注意这是HA的状态，不是数据库的状态
        if 'instance_name' not in pdict:
            db_dict['instance_name'] = ''
        db_dict['db_state'] = database_state.CREATING      # 数据库状态默认为创建中

        try:
            # 先插入数据库生成id
            sql = "INSERT INTO clup_db (cluster_id, state, pgdata, is_primary, repl_app_name, host, repl_ip, " \
                "instance_name, db_detail, port, db_state, up_db_id, db_type, scores) values " \
                "(%(cluster_id)s, %(state)s, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, %(repl_ip)s, "\
                "%(instance_name)s, %(db_detail)s, %(port)s, %(db_state)s, %(up_db_id)s, %(db_type)s, 0) RETURNING db_id"
            rows = dbapi.query(sql, db_dict)
        except Exception as e:
            return -1, f'Failed to insert data into clup_db: {str(e)}', 0, 0
        if len(rows) == 0:
            return -1, 'Failed to insert data into clup_db:', 0, 0
        # 更新集群的polar_hostid
        polar_lib.update_cluster_polar_hostid(db_dict['cluster_id'], polar_hostid + 1)

        db_id = rows[0]['db_id']
        db_dict['db_id'] = db_id

        # rpc_dict 放下创建备库的调用的参数
        # rpc_name_list定义需要从db_dict中复制过来的数据
        rpc_name_list = [
            'up_db_id',       # 上级库id
            'up_db_host',     # 上级库host
            'up_db_port',     # 上级库端口
            'up_db_repl_ip',  # 上级库流复制ip
            'db_id',          # 本备库id
            'os_user',        # 操作系统用户
            'os_uid',         # 操作系统用户uid
            'db_user',        # 数据库超级用户名
            'port',           # 备库的端口
            'pg_bin_path',    # 数据库软件目录
            'repl_user',      # 流复制用于
            'repl_pass',      # 流复制密码
            'pgdata',         # 数据库的数据目录
            'repl_app_name',  # 流复制的application_name
            'delay',          # 延迟时间
            'instance_type',  # 实例类型：独立实例
            'cpu',            # CPU
            'version',        # 版本信息
            'tblspc_dir',     # 表空间的目录信息:  [{'old_dir': '', 'new_dir': ''}, {'old_dir': '', 'new_dir': ''}]
            'other_param',     # pg_basebackup的附加参数
            'polar_datadir',   # 共享存储文件夹的本地路径
            'polar_hostid'
        ]
        db_dict['polar_datadir'] = db_detail['polar_datadir']
        db_dict['polar_hostid'] = polar_hostid
        rpc_dict = {}
        for k in rpc_name_list:
            rpc_dict[k] = db_dict.get(k)

        # 数据库用户，当db_user与os_user不相同是，需要在pg_hba.conf中加用户映射，否则本地无法误密码的登录数据库
        rpc_dict['db_user'] = up_db_dict['db_user']
        rpc_dict['repl_pass'] = rpc_dict['repl_pass']

        # 开始搭建备库
        task_name = f"build_standby(db={db_dict['host']}:{db_dict['port']})"
        rpc_dict['task_name'] = task_name
        # rpc_dict['task_key'] = task_name + str(db_dict['db_id'])

        task_id = general_task_mgr.create_task(
            task_type=task_type_def.PG_BUILD_STANDBY_TASK,
            task_name=task_name,
            task_data=rpc_dict
        )
        try:
            rpc_dict['task_id'] = task_id
            rpc_dict['pre_msg'] = f"Build standby(db_id={db_dict['db_id']})"
            host = db_dict['host']
            general_task_mgr.run_task(task_id, long_term_task.task_build_polar_standby, (host, db_id, rpc_dict))
            return 0, '', task_id, db_id
        except Exception as e:
            err_msg = str(e)
            general_task_mgr.complete_task(task_id, -1, err_msg)
            dao.update_db_state(db_dict['db_id'], database_state.FAULT)
        return -1, err_msg, task_id, db_id
    except Exception as e:
        err_msg = traceback.format_exc()
        logging.error(f"Build standby unexpect error: {err_msg}")
        return -1, err_msg, 0, 0


def repair_build_polar_reader(task_id, host, db_id, pdict):
    """polardb reader重搭加回集群
    Args:
        pdict (dict): 搭建数据库的参数

    Returns:
        int: err_code
        str: err_msg
        int: task_id
        int: db_id
    """
    try:
        # 查询上级库的信息
        sql = "SELECT db_id,up_db_id, cluster_id, host, repl_app_name, repl_ip, port,pgdata,db_type," \
            "clup_cluster.state as state," \
            "db_detail->>'db_user' as db_user, db_detail->>'db_pass' as db_pass," \
            " db_detail->>'instance_type' as instance_type,"\
            " db_detail->'repl_ip' as repl_ip_in_detail, db_detail " \
            " FROM clup_db left join clup_cluster using (cluster_id) WHERE db_id = %s "
        rows = dbapi.query(sql, (pdict['up_db_id'], ))
        if len(rows) == 0:
            return -1, 'Superior database instance does not exist, please try again.', 0, 0

        up_db_dict = rows[0]
        err_code, err_msg = pg_helpers.get_pg_setting_list(pdict['up_db_id'])
        if err_code != 0:
            return err_code, f"Failed to get the configuration of the superior database(up_db_id={pdict['up_db_id']})：{err_msg}", 0, 0
        up_db_dict['setting_list'] = err_msg

        # db_dict存储往表clup_db中插入的数据
        db_dict = copy.copy(pdict)
        db_detail = up_db_dict['db_detail']
        db_detail['delay'] = pdict.get('delay', None)

        db_detail['tblspc_dir'] = pdict.get('tblspc_dir', [])

        db_dict["db_type"] = up_db_dict["db_type"]
        db_dict['instance_name'] = pdict.get('instance_name', '')

        # 获取集群的polar_hostid
        err_code, err_msg = polar_lib.get_db_polar_hostid(db_id)
        if err_code != 0:
            return -1, err_msg
        if not err_msg.get('polar_hostid'):
            return -1, f"Failed to get polar_hostid. Please modify the polar_hostid in the database(db_id={db_id}) information first."
        polar_hostid = int(err_msg['polar_hostid'])

        db_dict['db_id'] = db_id
        db_dict['polar_hostid'] = polar_hostid
        db_dict['pfs_disk_name'] = db_detail['pfs_disk_name']
        db_dict['pfsdaemon_params'] = db_detail['pfsdaemon_params']

        # 数据库用户，当db_user禹os_user不相同是，需要在pg_hba.conf中加用户映射，否则本地无法误密码的登录数据库
        db_dict['db_user'] = up_db_dict['db_user']
        db_dict['db_pass'] = up_db_dict['db_pass']
        db_dict['up_db_host'] = up_db_dict['host']
        db_dict['up_db_pgdata'] = up_db_dict['pgdata']
        db_dict['repl_pass'] = db_encrypt.to_db_text(db_dict['repl_pass'])
        db_dict["other_param"] = '-X stream --progress --write-recovery-conf -v'
        try:
            err_code, err_msg = long_term_task.build_polar_reader(task_id, host, db_id, db_dict)
            return err_code, err_msg
        except Exception as e:
            err_msg = str(e)
            general_task_mgr.complete_task(task_id, -1, err_msg)
            dao.update_db_state(db_dict['db_id'], database_state.FAULT)
            return -1, err_msg
    except Exception:
        err_msg = f"Build polar reader unexpected error, {traceback.format_exc()}."
        logging.error(err_msg)
        general_task_mgr.complete_task(task_id, -1, err_msg)
        return -1, err_msg


def repair_build_polar_standby(task_id, host, db_id, pdict):
    """polardb standby节点重搭加回集群
    Args:
        pdict (dict): 搭建数据库的参数

    Returns:
        int: err_code
        str: err_msg
        int: task_id
        int: db_id
    """
    try:
        # 查询上级库的信息
        sql = "SELECT db_id,up_db_id, cluster_id, host, repl_app_name, repl_ip, "\
            "port,pgdata,clup_cluster.state as state, db_detail," \
            "db_detail->>'db_user' as db_user, db_detail->>'db_pass' as db_pass," \
            "db_detail->>'instance_type' as instance_type, db_detail->'repl_ip' as repl_ip_in_detail"\
            " FROM clup_db left join clup_cluster using (cluster_id) WHERE db_id = %s "
        rows = dbapi.query(sql, (pdict['up_db_id'], ))
        if len(rows) == 0:
            return -1, 'The superior database instance does not exist. Please try again'

        up_db_dict = rows[0]
        err_code, err_msg = pg_helpers.get_pg_setting_list(pdict['up_db_id'])
        if err_code != 0:
            return -1, f"Unable to obtain configuration of the source database:{err_msg}"
        up_db_dict['setting_list'] = err_msg

        # db_dict存储往表clup_db中插入的数据
        db_dict = copy.copy(pdict)
        db_detail = up_db_dict['db_detail']

        db_detail['delay'] = pdict.get('delay', None)
        db_detail['tblspc_dir'] = pdict.get('tblspc_dir', [])

        db_dict['repl_app_name'] = pdict['repl_ip']
        db_dict['up_db_host'] = up_db_dict['host']
        db_dict['up_db_repl_ip'] = up_db_dict['repl_ip_in_detail'] if up_db_dict['repl_ip_in_detail'] else up_db_dict['repl_ip']
        db_dict['up_db_port'] = up_db_dict['port']
        db_dict['cluster_id'] = up_db_dict['cluster_id']
        # db_dict["db_type"] = up_db_dict["db_type"]
        db_dict['is_primary'] = 0

        db_dict['db_id'] = db_id

        # 获取集群的polar_hostid
        err_code, err_msg = polar_lib.get_db_polar_hostid(db_id)
        if err_code != 0:
            return -1, err_msg
        if not err_msg.get('polar_hostid'):
            return -1, f"Failed to get polar_hostid. Please modify the polar_hostid in the database(db_id={db_id}) information first."
        polar_hostid = int(err_msg['polar_hostid'])

        db_dict['polar_hostid'] = polar_hostid
        # 数据库用户，当db_user与os_user不相同是，需要在pg_hba.conf中加用户映射，否则本地无法误密码的登录数据库
        db_dict['db_user'] = db_dict['db_user']
        db_dict['repl_pass'] = db_encrypt.to_db_text(db_dict['repl_pass'])
        db_dict["other_param"] = '-X stream --progress --write-recovery-conf -v'
        try:
            err_code, err_msg = long_term_task.build_polar_standby(task_id, host, db_id, db_dict)
            return err_code, err_msg
        except Exception as e:
            err_msg = str(e)
            general_task_mgr.complete_task(task_id, -1, err_msg)
            dao.update_db_state(db_dict['db_id'], database_state.FAULT)
            return -1, err_msg
    except Exception:
        err_msg = f"Build standby unexpect error: {traceback.format_exc()}."
        logging.error(err_msg)
        general_task_mgr.complete_task(task_id, -1, err_msg)
        return -1, err_msg


def change_recovery_up_db(db_id, up_db_id, polar_type):
    """修改备库的流复制连接

    """
    rows = dao.get_db_info(up_db_id)
    if not len(rows):
        return -1, f"Failed to obtain database(db_id={up_db_id}) information."
    up_db_host = rows[0]['host']
    up_db_port = rows[0]['port']

    # 先检查是否存在recovery.conf文件，没有的话直接新建一个
    err_code, err_msg = polar_lib.check_or_create_recovery_conf(db_id, rows[0])
    if err_code != 0 and err_code != 1:
        return -1, err_msg

    # 返回值为0说明存在recovery.conf,为1时已新建不需要再修改
    if err_code == 0:
        err_code, err_msg = polar_lib.update_recovery(db_id, up_db_host, up_db_port)
        if err_code != 0:
            return -1, err_msg

    rows = dao.get_db_info(db_id)
    if not len(rows):
        return -1, f"Failed to obtain database(db_id={db_id}) information."
    host = rows[0]['host']
    pgdata = rows[0]['pgdata']

    if polar_type in ['reader', 'master']:
        # 启动pfs
        err_code, err_msg = polar_lib.start_pfs(host, db_id)
        if err_code != 0 and err_code != 1:
            return -1, err_msg

    err_code, err_msg = pg_db_lib.restart(host, pgdata)
    return err_code, err_msg


def recovery_standby(task_id, msg_prefix, recovery_host, pgdata):
    """recovery polardb standby in local storage

    Args:
        recovery_host (_type_): _description_
        pgdata (_type_): _description_
    """
    try:
        # move polar_shared_data to local
        err_code, err_msg = polar_lib.polar_share_to_local(task_id, msg_prefix, recovery_host, pgdata)
        if err_code != 0:
            general_task_mgr.log_error(task_id, err_msg)
            return -1, err_msg

        # create rpc connect
        rpc = None
        err_code, err_msg = rpc_utils.get_rpc_connect(recovery_host)
        if err_code != 0:
            err_msg = f"Connect to host({recovery_host}) failed, {err_msg}."
            return -1, err_msg
        rpc = err_msg

        # disable some settings
        postgresql_conf = f"{pgdata}/postgresql.conf"
        remove_conf_list = [
            'polar_datadir',
            'polar_disk_name',
            'polar_vfs.localfs_mode',
            'polar_storage_cluster_name',
            'polar_enable_shared_storage_mode'
        ]
        # edit the postgres.conf file
        err_code, err_msg = polar_lib.disable_settings(rpc, postgresql_conf, remove_conf_list)
        if err_code != 0:
            err_msg = f"""Restore file is ok, but edit the {postgresql_conf} failed,{err_msg},
            need disable the {remove_conf_list} by yourself.
            """
            general_task_mgr.log_error(task_id, err_msg)
            return -1, err_msg
        # need add polar_vfs.localfs_mode to on
        rpc.modify_config_type1(postgresql_conf, {"polar_vfs.localfs_mode": "on"}, is_backup=False)
        # check postgres.auto.conf
        postgresql_auto_conf = f"{pgdata}/postgresql.auto.conf"
        err_code, err_msg = polar_lib.disable_settings(rpc, postgresql_auto_conf, remove_conf_list)
        if err_code != 0:
            err_msg = f"""Restore file is ok, but edit the {postgresql_auto_conf} failed,{err_msg},
            need disable the {remove_conf_list} by yourself.
            """
            general_task_mgr.log_error(task_id, err_msg)
            return -1, err_msg

        return 0, "recovery for polrdb success"
    except Exception:
        err_msg = f"recovery polardb to local storage with unexcept error, {traceback.format_exc()}"
        general_task_mgr.log_error(task_id, err_msg)
        return -1, err_msg
    finally:
        if rpc:
            rpc.close()


# 存在问题暂时弃用2023-07-19
def recovery_master(task_id, msg_prefix, pdict):
    """recovery polardb standby in local storage

    Args:
        recovery_host (_type_): _description_
        pdict (_type_): {
            "recovery_host": ,
            "pgdata": ,
            "shared_data": ,
            "pg_bin_path": ,
            "pfs_disk_name": ,
            "pfsdaemon_params": ,
        }
    """
    recovery_host = pdict["recovery_host"]
    try:
        # start pfs, pfs_dict={"pfs_disk_name": , "pfsdaemon_params": ,}
        pfs_dict = {
            "pfs_disk_name": pdict["pfs_disk_name"],
            "pfsdaemon_params": pdict.get("pfsdaemon_params")
        }
        err_code, err_msg = polar_lib.start_pfs(recovery_host, pfs_dict=pfs_dict)
        if err_code != 0:
            return -1, err_msg

        # move polar_shared_data to shared_data (pg_bin_path, pgdata, shared_data)
        err_code, err_msg = polar_lib.polar_local_to_share(task_id, msg_prefix, pdict)
        if err_code != 0:
            general_task_mgr.log_error(task_id, err_msg)
            polar_lib.stop_pfs(recovery_host, pfs_dict=pfs_dict)
            return -1, err_msg
        return 0, "recovery for polrdb success"
    except Exception:
        polar_lib.stop_pfs(recovery_host, pfs_dict=pfs_dict)
        err_msg = f"recovery polardb to local storage with unexcept error, {traceback.format_exc()}"
        general_task_mgr.log_error(task_id, err_msg)
        return -1, err_msg


if __name__ == '__main__':
    pass
