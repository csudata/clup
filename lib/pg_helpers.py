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
@description: PostgreSQL数据库操作模块
"""

import copy
import json
import logging
import os
import re
import time
import traceback

import dao
import database_state
import db_encrypt
import dbapi
import general_task_mgr
import long_term_task
import pg_db_lib
import pg_utils
import probe_db
import rpc_utils
import task_type_def


def log_info(task_id, msg):
    logging.info(msg)
    general_task_mgr.log_info(task_id, msg)


def log_warn(task_id, msg):
    logging.warning(msg)
    general_task_mgr.log_warn(task_id, msg)


def log_error(task_id, msg):
    logging.warning(msg)
    general_task_mgr.log_error(task_id, msg)


def source_check(host, port, pgdata):
    """创建数据库前检查资源是否可用

    Args:
        db_info (_type_): {
            host:
            port:
            pgdata:
        }
    """
    # step1: check ip + port
    check_sql = "SELECT db_id FROM clup_db WHERE host=%s and port=%s"
    rows = dbapi.query(check_sql, (host, port))
    if rows:
        return -1, f"The port {port} in host {host} is aready uesd by db_id={rows[0]['db_id']}."
    # step2: check pgdata
    try:
        rpc = None
        # check is exists or not, if exists must be empty
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            return -1, f"Connect the host failed, {err_msg}."
        rpc = err_msg
        is_exists = rpc.os_path_exists(pgdata)
        if is_exists:
            # check is empty or not
            is_empty = rpc.dir_is_empty(pgdata)
            if not is_empty:
                return -1, f"The pgdata {pgdata} is not empty."
    except Exception:
        return -1, f"Check the source in host {host} with unexcept error, {traceback.format_exc()}."
    finally:
        if rpc:
            rpc.close()
    return 0, "Check is OK"


def create_db(pdict):
    """创建数据库

    Args:
        pdict (dict): 创建数据库的参数,以自带的方式传过来

    Returns:
        int: err_code
        str: err_msg
        int: task_id
    """

    try:
        instance_name = pdict.get('instance_name')
        db_state = database_state.CREATING  # 正在创建中

        setting_dict = pg_setting_list_to_dict(pdict['setting_list'])
        setting_dict['port'] = pdict['port']
        if "pg_stat_statements" not in setting_dict.get("shared_preload_libraries"):
            return -1, 'Database needs pg_stat_statements plug-in, please fill in in shared_preload_libraries'

        host = pdict['host']
        rpc = None
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            dao.update_db_state(pdict['db_id'], database_state.FAULT)
            err_msg = f"Host connection failure({host}), please check service clup-agent is running: {err_msg}"
            return -1, err_msg
        rpc = err_msg
    except Exception:
        return -1, traceback.format_exc()

    pre_msg = "Check can create db"
    try:
        # 如果配置中配置了插件,但软件中没有此插件,则报错返回
        plug_str = setting_dict.get('shared_preload_libraries')
        plug_list = plug_str.replace("'", '').split(',')
        err_code = err_msg = ''
        pg_bin_path = pdict['pg_bin_path']
        for plug in plug_list:
            plug_ctl_file = f"{pg_bin_path}/../share/extension/{plug}.control"
            if not rpc.os_path_exists(plug_ctl_file):
                plug_ctl_file = f"{pg_bin_path}/../share/postgresql/extension/{plug}.control"
                if not rpc.os_path_exists(plug_ctl_file):
                    return -1, f"Plug-in({plug}) not installed!"

        pdict['db_detail'] = json.dumps(dict(os_user=pdict['os_user'], os_uid=pdict['os_uid'], db_user=pdict['db_user'], db_pass=pdict['db_pass'],
                        instance_type=pdict['instance_type'], version=pdict['version'], pg_bin_path=pdict['pg_bin_path'], room_id='0'))
        pdict['is_primary'] = 1
        pdict['instance_name'] = instance_name
        pdict['db_state'] = db_state
        # leifliu Test
        pdict['repl_app_name'] = pdict['host']
        pdict['repl_ip'] = pdict['host']
        # add repl_app_name, repl_ip
        sql = """INSERT INTO clup_db(state, pgdata, is_primary, repl_app_name, host, repl_ip,
        instance_name, db_detail, port, db_state, db_type, scores)
        VALUES(0, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, %(repl_ip)s,
        %(instance_name)s, %(db_detail)s, %(port)s, %(db_state)s, %(db_type)s, 0)
        RETURNING db_id
        """
        rows = dbapi.query(sql, pdict)
        db_id = rows[0]['db_id']

        pre_msg = f"create_db(db_id={db_id})"

        pdict['db_id'] = db_id
        # pdict['instance_name'] = f"pg{db_id:09d}"
        pdict['setting_dict'] = setting_dict
        pdict['db_pass'] = db_encrypt.from_db_text(pdict['db_pass'])

        task_name = f"create_db(db={pdict['db_id']})"

        rpc_dict = {}
        rpc_dict.update(pdict)
        rpc_dict['task_name'] = task_name

        task_id = general_task_mgr.create_task(
            task_type=task_type_def.PG_CREATE_INSTANCE_TASK,
            task_name=task_name,
            task_data=rpc_dict
        )
        rpc_dict['task_id'] = task_id

        general_task_mgr.run_task(task_id, long_term_task.task_create_pg_db, (host, db_id, rpc_dict))
        return 0, task_id

    except Exception as e:
        dao.update_db_state(pdict['db_id'], database_state.CREATE_FAILD)
        err_msg = f"{pre_msg}: Database creation failure: unknown error {str(e)}"
        log_error(task_id, err_msg)
        general_task_mgr.complete_task(task_id, -1, err_msg)
        return -1, err_msg
    finally:
        if rpc:
            rpc.close()


def build_standby(pdict):
    """搭建备库

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
            return -1, f"Failed to create the database, The database on the host({pdict['host']}) has used the port({pdict['port']})"

        # 查询上级库的信息
        sql = "SELECT db_id,up_db_id,db_type, cluster_id, host, repl_app_name, repl_ip, port,pgdata,clup_cluster.state as state," \
            "db_detail->>'db_user' as db_user, db_detail->>'db_pass' as db_pass," \
            " db_detail->>'instance_type' as instance_type,"\
            " db_detail->'repl_ip' as repl_ip_in_detail, db_detail " \
            " FROM clup_db left join clup_cluster using (cluster_id) WHERE db_id = %s "
        rows = dbapi.query(sql, (pdict['up_db_id'], ))
        if len(rows) == 0:
            return -1, 'the primary database instance does not exist. Please try again'
        if rows[0]['cluster_id'] and rows[0]['state'] == 1:
            return -1, f"The primary database is in the cluster[cluster_id: {rows[0]['cluster_id']}]. Please take it offline before performing operations."
        # leifliu Test 更新上级库信息,添加repl_user、repl_pass
        update_dict = json.dumps({"repl_user": pdict['repl_user'], "repl_pass": pdict['repl_pass']})
        sql = "UPDATE clup_db SET db_detail = db_detail || (%s::jsonb) WHERE db_id = %s"
        dbapi.execute(sql, (update_dict, pdict['up_db_id']))

        up_db_dict = rows[0]
        err_code, err_msg = get_pg_setting_list(pdict['up_db_id'])
        if err_code != 0:
            return err_code, f"The configuration of the primary database could not be obtained: {err_msg}"
        up_db_dict['setting_list'] = err_msg

        # leifliu Test
        # 检查上级主库中安装的插件,如果要搭建的备库的数据库目录中没有此插件,则报错返回
        setting_dict = pg_setting_list_to_dict(up_db_dict["setting_list"])
        plug_str = setting_dict.get('shared_preload_libraries', '')
        if plug_str:
            plug_str = plug_str.replace("'", '')
        if plug_str != '':
            plug_list = plug_str.split(',')
        else:
            plug_list = []
        pg_bin_path = pdict['pg_bin_path']
        err_code, err_msg = rpc_utils.get_rpc_connect(pdict["host"])
        if err_code != 0:
            err_msg = f"Cannot connect to the host({pdict['host']}), cannot verify whether the directory({pg_bin_path}) is PG BIN directory"
            logging.info(err_msg + f" *** {err_msg}")
            return -1, err_msg
        rpc = err_msg
        try:
            for plug in plug_list:
                plug_ctl_file = f"{pg_bin_path}/../share/extension/{plug}.control"
                if not rpc.os_path_exists(plug_ctl_file):
                    plug_ctl_file = f"{pg_bin_path}/../share/postgresql/extension/{plug}.control"
                    if not rpc.os_path_exists(plug_ctl_file):
                        return -1, f"Plug-in({plug}) not installed!"
        finally:
            rpc.close()

        unix_socket_directories = setting_dict.get('unix_socket_directories', '/tmp')
        cells = unix_socket_directories.split(',')
        unix_socket_dir = cells[0].strip()

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
            'pg_bin_path': pdict['pg_bin_path']
        }
        if pdict.get('delay'):
            db_detail['delay'] = pdict['delay']
        if pdict.get('tblspc_dir'):
            db_detail['tblspc_dir'] = pdict['tblspc_dir']

        db_dict['db_detail'] = json.dumps(db_detail)
        db_dict['repl_app_name'] = pdict['host']
        db_dict['up_db_host'] = up_db_dict['host']
        db_dict['up_db_repl_ip'] = up_db_dict['repl_ip_in_detail'] if up_db_dict['repl_ip_in_detail'] else up_db_dict['repl_ip']
        db_dict['up_db_port'] = up_db_dict['port']
        db_dict['cluster_id'] = up_db_dict['cluster_id']
        db_dict['db_type'] = up_db_dict["db_type"]
        db_dict['is_primary'] = 0
        db_dict['state'] = 1  # 注意这是HA的状态,不是数据库的状态
        if 'instance_name' not in pdict:
            db_dict['instance_name'] = ''
        db_dict['db_state'] = database_state.CREATING      # 数据库状态默认为创建中

        try:
            # 先插入数据库生成id
            sql = "INSERT INTO clup_db (cluster_id, state, pgdata, is_primary, repl_app_name, host, repl_ip, " \
                "instance_name, db_detail, port, db_state, up_db_id, scores, db_type) values " \
                "(%(cluster_id)s, %(state)s, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, %(repl_ip)s, "\
                "%(instance_name)s, %(db_detail)s, %(port)s, %(db_state)s, %(up_db_id)s, 0, %(db_type)s) RETURNING db_id"
            rows = dbapi.query(sql, db_dict)
        except Exception as e:
            return -1, f'Failed to insert data into the table(clup_db): {str(e)}'
        if len(rows) == 0:
            return -1, 'Failed to insert data into the table(clup_db)'
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
            'other_param',    # pg_basebackup的附加参数
            'is_exclusive',
            'cpu_num',
            'memory_size'
        ]

        rpc_dict = {}
        for k in rpc_name_list:
            rpc_dict[k] = db_dict.get(k)

        rpc_dict['unix_socket_dir'] = unix_socket_dir

        # # 数据库用户,当db_user禹os_user不相同是,需要在pg_hba.conf中加用户映射,否则本地无法误密码的登录数据库
        rpc_dict['db_user'] = up_db_dict['db_user']
        rpc_dict['repl_pass'] = db_encrypt.from_db_text(rpc_dict['repl_pass'])

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
            # 开始搭备库
            pdict['repl_pass'] = db_encrypt.from_db_text(pdict['repl_pass'])
            host = db_dict['host']
            general_task_mgr.run_task(task_id, long_term_task.task_build_pg_standby, (host, db_id, rpc_dict))
            return 0, (task_id, db_id)
        except Exception as e:
            err_code = -1
            err_msg = str(e)
            general_task_mgr.complete_task(task_id, -1, err_msg)
            dao.update_db_state(db_dict['db_id'], database_state.CREATE_FAILD)
        return err_code, (task_id, db_id)
    except Exception as e:
        err_code = -1,
        err_msg = traceback.format_exc()
        dao.update_db_state(db_dict['db_id'], database_state.CREATE_FAILD)
        logging.error(f"Build standby unexpect error: {err_msg}")
        return err_code, err_msg


def change_up_db_by_db_id(db_id, up_db_id):
    """
    将db_id的上级库切换成up_db_id
    :param db_id:
    :param up_db_id:
    :return:
    """
    if up_db_id is None:
        # 这种情况是要切换主库
        # 注释掉set_primary,此函数不需要激活主库
        # return set_primary(db_id)
        return -1, 'up_db_id can not none for call change_up_db_by_db_id'

    # 查到更换上级主库所需参数,并修改recovery文件
    sql = "SELECT host,pgdata,db_detail->>'instance_type' AS instance_type, " \
          " db_detail->>'db_user' as db_user,  db_detail->>'db_pass' as db_pass, " \
          " db_detail->>'repl_user' AS repl_user, db_detail->>'repl_pass' AS repl_pass, " \
          " db_detail->>'version' as version, repl_app_name" \
          " FROM clup_db  WHERE db_id = %s"
    # 当前数据库的信息
    db_rows = dbapi.query(sql, (db_id, ))
    if len(db_rows) == 0:
        return -1, f'Database does not exist(db_id:{db_id})'
    row = db_rows[0]
    host = row['host']
    pgdata = row['pgdata']
    repl_user = row['repl_user']
    repl_pass = db_encrypt.from_db_text(row['repl_pass'])
    repl_app_name = row['repl_app_name']

    sql = "SELECT repl_ip as pri_host, port FROM clup_db WHERE db_id = %s"
    # 上一级主库需要的信息
    up_db_rows = dbapi.query(sql, (up_db_id, ))
    if len(up_db_rows) == 0:
        return -1, f'Database does not exist(db_id:{up_db_id})'

    row = up_db_rows[0]
    up_db_host = row['pri_host']
    up_db_port = row['port']

    err_code, err_msg = rpc_utils.get_rpc_connect(host, 2)
    if err_code != 0:
        return -1, f"Failed to connect to the agent({host})."
    rpc = err_msg
    try:
        err_code, err_msg = pg_db_lib.pg_change_standby_updb(rpc, pgdata, repl_user, repl_pass, repl_app_name, up_db_host, up_db_port)
    finally:
        rpc.close()
    if err_code != 0:
        return -1, f'Failed to modify: {err_msg}'
    # 更新数据库
    dao.update_up_db_id(up_db_id, db_id, 0)
    return 0, ''


def set_primary(db_id):
    """
    将db_id设置为主库; 直接将pgdata下面的recovery.conf 改为recovery.done,更新一下is_primary
    pg12版本只需要删掉standby.signal文件
    :param db_id:
    :return:
    """
    sql = "SELECT pgdata,host,cluster_id,db_detail->>'instance_type' AS instance_type, db_detail->>'version' as version FROM clup_db WHERE db_id = %s "
    rows = dbapi.query(sql, (db_id, ))
    if len(rows) == 0:
        return -1, f'The database could not be found(db_id: {db_id})'
    db = rows[0]
    host = db['host']
    # 先将旧的主库关闭
    old_primary = dao.get_primary_db(db_id)[0]
    err_code, err_msg = pg_db_lib.stop(old_primary)
    if err_code != 0:
        return -1, f'Failed to stop the primary database: {err_msg}'

    # 下面代码已改用promote来提升备库为主库,无需再获取数据库版本
    # if not db['version']:
    #     err_code, err_msg = renew_pg_bin_info(db_id)
    #     if err_code != 0:
    #         return err_code, err_msg

    # old_file = pgdata + '/recovery.conf'
    # new_file = pgdata + '/recovery.done'

    pgdata = db['pgdata']
    err_code, err_msg = rpc_utils.get_rpc_connect(host, 2)
    if err_code != 0:
        return -1, f'Host connection failure(host: {host})'
    rpc = err_msg
    try:
        # if int(str(version).split('.')[0]) >= 12:
        err_code, err_msg = pg_db_lib.promote(rpc, pgdata)
        # else:
        #     err_code, err_msg = rpc.change_file_name(old_file, new_file)

        if err_code < 0:
            rpc.close()
            return -1, err_msg

        err_code, err_msg = pg_db_lib.restart(rpc, db['pgdata'])
        msg = ''
        if err_code != 0:
            rpc.close()
            msg = f'Database(db_id: {db_id}) restart failure: {err_msg} '
            logging.error(f'set primary: {msg}.')
    finally:
        rpc.close()
    # 修改完成启动旧的主库
    err_code, err_msg = pg_db_lib.start(old_primary['host'], old_primary['pgdata'])
    if err_code != 0:
        return -1, f'The old database failed to start: {err_msg}'
    sql = "UPDATE clup_db SET up_db_id = null, is_primary = 1 WHERE db_id = %s"
    dbapi.execute(sql, (db_id,))
    return 0, msg


def sr_test_can_switch(old_pri_db, new_pri_db):
    """检查备库是否落后主库太多,如果备库需要的WAL已经不再主库上了,则不能切换
    Args:
        old_pri_db ([type]): [description]
        new_pri_db ([type]): [description]

    Returns:
        [type]: [description]
    """

    try:
        repl_pass = db_encrypt.from_db_text(new_pri_db['repl_pass'])
        err_code, new_pri_last_wal_file = probe_db.get_last_wal_file(new_pri_db['repl_ip'], new_pri_db['port'],
                                                    new_pri_db['repl_user'], repl_pass)

        if err_code != 0:
            err_msg = f"get new pirmary({new_pri_db['host']}) last wal file failed: {new_pri_last_wal_file}"
            return err_code, err_msg

        # err_code, str_ver = rpc_utils.pg_version(old_pri_db['host'], old_pri_db['pgdata'])
        err_code, str_ver = pg_db_lib.pgdata_version(old_pri_db['host'], old_pri_db['pgdata'])
        if err_code != 0:
            err_msg = f"get pg version error: {str_ver}"
            return err_code, err_msg
        pg_major_int_version = int(str(str_ver).split('.')[0])
        if pg_major_int_version >= 10:
            wal_dir = 'pg_wal'
        else:
            wal_dir = 'pg_xlog'
        xlog_file_full_name = os.path.join(old_pri_db['pgdata'], wal_dir, new_pri_last_wal_file)
        err_code, is_exists = rpc_utils.os_path_exists(old_pri_db['host'], xlog_file_full_name)
        if err_code != 0:
            err_msg = f"call rpc os_path_exists({old_pri_db['host']}, {xlog_file_full_name}) failed when find last wal file in " \
                      f"original primary({old_pri_db['host']})," \
                      f"so it can not became primary: {is_exists}"
            return err_code, err_msg

        if not is_exists:
            err_msg = f"new pirmary({new_pri_db['host']}) last wal file({xlog_file_full_name}) not exists in original primary({old_pri_db['host']}), " \
                      "so it can not became primary!"
            return 1, err_msg
        return 0, ''
    except Exception:
        err_msg = f"unexpect error occurred: {traceback.format_exc()}"
        return -1, err_msg


def task_log_info(task_id, msg):
    if task_id is not None:
        general_task_mgr.log_info(task_id, msg)


def task_log_error(task_id, msg):
    if task_id is not None:
        general_task_mgr.log_info(task_id, msg)


def switch_over_db(old_pri_db, new_pri_db, task_id, pre_msg):
    """
    将两个数据库的主备关系互换
    """
    # 步骤1,先把原主库停掉
    try:
        task_log_info(task_id, f"{pre_msg}: begin stopping current primary database({old_pri_db['host']})...")
        err_code, err_msg = pg_db_lib.stop(old_pri_db['host'], old_pri_db['pgdata'], 30)  # 停止数据库等待的时间为
        if err_code != 0:
            err_msg = f"current primary database({old_pri_db['host']}) can not be stopped: {err_msg}"
            task_log_error(task_id, f"{pre_msg}: {err_msg}")
            return -1, err_msg
        task_log_info(task_id, f"{pre_msg}: current primary database({old_pri_db['host']}) stopped.")

        err_code, err_msg = sr_test_can_switch(old_pri_db, new_pri_db)
        if err_code != 0:
            task_log_error(task_id, f"{pre_msg}: {err_msg}")
            # 出现错误,开始回滚前面的操作
            task_log_info(task_id, f"{pre_msg}: switch failed, start the original primary database({old_pri_db['host']}).")
            pg_db_lib.start(old_pri_db['host'], old_pri_db['pgdata'])
            task_log_info(task_id, f"{pre_msg}: the original primary database({old_pri_db['host']}) is started.")
            return err_code, err_msg

        # 先把新主库关掉,然后把旧主库上比较新的xlog文件都拷贝过来：
        task_log_info(task_id, f'{pre_msg}: stop new pirmary database then sync wal from old primary ...')
        # 在rpc.pg_cp_delay_wal_from_pri中 会把新主库给先停掉
        err_code, err_msg = rpc_utils.pg_cp_delay_wal_from_pri(
            new_pri_db['host'],
            old_pri_db['host'],
            old_pri_db['pgdata'],
            new_pri_db['pgdata']
        )

        if err_code != 0:
            err_msg = f"stop new pirmary database then sync wal from old primary failed: {err_msg}"
            task_log_error(task_id, f"{pre_msg}: {err_msg}")
            return err_code, err_msg

        task_log_info(task_id, f'{pre_msg}: stop new pirmary database then sync wal from old primary finished')

        # 再重新启动新主库(目前还处于只读standby模式)
        task_log_info(task_id, f'{pre_msg}: restart new pirmary database and wait it is ready ...')
        err_code, err_msg = pg_db_lib.start(new_pri_db['host'], new_pri_db['pgdata'])
        if err_code != 0:
            err_msg = f"start new primary failed: {err_msg}"
            task_log_error(task_id, f"{pre_msg}: {err_msg}")
            return -1, err_msg

        # 等待新主库滚日志到最新
        task_log_info(task_id, f'{pre_msg}: wait new pirmary to be started and is ready to accept new connection ...')
        while True:
            err_code, err_msg = pg_db_lib.is_ready(new_pri_db['host'], new_pri_db['pgdata'], new_pri_db['port'])
            if err_code == 0:
                break
            elif err_code == 1:
                task_log_info(task_id, f"{pre_msg}: Check is ready, {err_msg}")
                time.sleep(1)
                continue
            else:
                task_log_error(task_id, f"{pre_msg}: Check is ready, {err_msg}")
                return -1, err_msg
        task_log_info(task_id, f'{pre_msg}: new pirmary database started and it is ready.')

        # 需要把其旧主库转换成备库模式,并让其指向新的主库
        log_info(task_id, f"{pre_msg}: change old primary database to standby and change upper level database to new primary...")
        change_up_db_by_db_id(old_pri_db['db_id'], new_pri_db['db_id'])
        # 更新数据库存的上级主库
        # dao.update_up_db_id('null', new_pri_db['db_id'], 1)

        # 修改数据库配配置：把新主库的上级库设置为NULL,is_primary设置为1
        dao.update_up_db_id('NULL', new_pri_db['db_id'], 1)
        # 修改数据库配配置：把旧主库的上级库设置新主库,is_primary设置为1
        dao.update_up_db_id(new_pri_db['db_id'], old_pri_db['db_id'], 0)

        # 把选中的这个新主库（目前处于备库状态）激活成主库
        task_log_info(task_id, f'{pre_msg}: promote new primary ...')
        err_code, err_msg = pg_db_lib.promote(new_pri_db['host'], new_pri_db['pgdata'])
        if err_code != 0:
            err_msg = f"{pre_msg}: promote failed: {str(err_msg)} "
            task_log_error(task_id, f"{pre_msg}: {err_msg}")
            return err_code, err_msg
        task_log_info(task_id, f'{pre_msg}: promote new primary completed.')
        return 0, ''
    except Exception:
        err_msg = f"unexpect error occurred: {traceback.format_exc()}"
        return -1, err_msg


def renew_pg_bin_info(db_id):
    """
    更新PG数据库中与PG软件和操作系统相关的信息,如版本、os_user、os_uid、pg_bin_path等信息
    """

    sql = ("SELECT db_id, up_db_id, cluster_id, scores, state, pgdata, is_primary, repl_app_name, host, repl_ip, port, "
        " db_detail->>'instance_type' AS instance_type, db_detail->>'db_user' as db_user,"
        " db_detail->>'os_user' as os_user, db_detail->>'os_uid' as os_uid, "
        " db_detail->>'pg_bin_path' as pg_bin_path, db_detail->>'version' as version, "
        " db_detail->>'db_pass' as db_pass, db_detail->'room_id' as room_id  "
        f" FROM clup_db WHERE db_id={db_id}")

    rows = dbapi.query(sql)
    if len(rows) == 0:
        return -10, f'Database(db_id: {db_id}) does not exist, please refresh and try again.'
    db = rows[0]

    err_code, err_msg = rpc_utils.get_rpc_connect(db['host'])
    if err_code != 0:
        return err_code, err_msg
    rpc = err_msg
    renew_dict = {}
    try:
        # 读取pgdata目录的属主
        pgdata = db['pgdata']
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            err_msg = f"get file({pgdata}) stat failed: {err_msg}"
            return err_code, err_msg
        st_dict = err_msg
        os_uid = st_dict['st_uid']
        # 根据os_uid获得用户名称
        err_code, err_msg = rpc.pwd_getpwuid(os_uid)
        if err_code != 0:
            err_msg = f"get username by uid({os_uid}) failed: {err_msg}"
            return err_code, err_msg
        pw_dict = err_msg
        os_user = pw_dict['pw_name']
        renew_dict['os_user'] = os_user
        renew_dict['os_uid'] = os_uid

        # if not db['pg_bin_path']:  # 旧版本没有此字段,其他情况也可能没有这个字段
        # 先看看数据库是否运行,如果数据库正在运行,使用正在运行的程序的pid获得程序路径,否则使用which postgres去获得
        pg_pid_file = f"{pgdata}/postmaster.pid"
        pg_bin_path = None
        err_code, err_msg = rpc.file_read(pg_pid_file)
        if err_code == 0:
            content = err_msg
            cells = content.split('\n')
            pg_master_pid = cells[0].strip()
            proc_pid_exe_file = f'/proc/{pg_master_pid}/exe'
            err_code, err_msg = rpc.os_readlink(proc_pid_exe_file)
            if err_code == 0:
                pg_bin_file = err_msg
                pg_bin_path = os.path.dirname(pg_bin_file)
        if not pg_bin_path:
            cmd = f"su - {os_user} -c 'which postgres'"
            err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
            if err_code != 0:
                return err_code, err_msg
            out_msg = out_msg.strip()
            pg_bin_path = os.path.dirname(out_msg)
        if pg_bin_path:
            renew_dict['pg_bin_path'] = pg_bin_path
        # else:
        #     pg_bin_path = db['pg_bin_path']
        err_code, err_msg = pg_db_lib.get_pg_bin_version(rpc, pg_bin_path)
        if err_code != 0:
            return err_code, err_msg
        version = err_msg
        renew_dict['version'] = version
    finally:
        rpc.close()
    sql = "UPDATE clup_db set db_detail = db_detail || (%s::jsonb) WHERE db_id=%s"
    dbapi.execute(sql, (json.dumps(renew_dict), db_id))
    db.update(renew_dict)
    return 0, db


def get_db_params(up_db_id, standby_id):
    primary = dao.get_db_info(up_db_id)
    standby = dao.get_db_info(standby_id)
    if not primary:
        return -1, f'Failed to obtain database({up_db_id}) information.'
    if not standby:
        return -1, f'Failed to obtain database({standby_id}) information.'
    standby = standby[0]
    primary = primary[0]

    pdict = {
        "host": standby['host'],
        "os_user": standby['os_user'] if standby['os_user'] else 'postgres',
        "os_uid": standby['os_uid'] if standby['os_uid'] else 701,
        "pg_bin_path": standby['pg_bin_path'],
        "db_user": standby['db_user'],
        "up_db_host": primary['host'],
        "up_db_port": primary['port'],
        "up_db_repl_ip": primary['repl_ip'],
        "repl_user": standby['repl_user'],
        "repl_pass": db_encrypt.from_db_text(standby['repl_pass']),
        "pgdata": standby['pgdata'],
        "repl_ip": standby['repl_ip'],
        "repl_app_name": standby['repl_app_name'],
        "instance_type": standby['instance_type'],
        "up_db_id": up_db_id,
        "standby_id": standby_id,
        "tblspc_dir": standby['tblspc_dir'],
        "cpu": standby.get('cpu'),
        "db_name": "template1"
    }
    if not primary['version']:
        err_code, err_msg = renew_pg_bin_info(up_db_id)
        if err_code != 0:
            return err_code, err_msg
        renew_dict = err_msg
        pdict['version'] = renew_dict['version']
    return 0, pdict


def pg_setting_list_to_dict(setting_list):
    setting_dict = dict()
    for conf in setting_list:
        sql = "SELECT setting_type FROM clup_init_db_conf WHERE setting_name=%s"
        rows = dbapi.query(sql, (conf['setting_name'], ))
        if rows:
            setting_type = rows[0]['setting_type']
        else:
            setting_type = 1

        if setting_type == 3 or setting_type == 4:
            # 带有单位的配置值
            if 'unit' not in conf:
                if setting_type == 4:
                    conf['unit'] = 'ms'
                else:
                    conf['unit'] = ''
            if setting_type == 3:
                if conf['unit'] == 'B':
                    conf['unit'] = ''
            setting_dict[conf['setting_name']] = str(conf['val']) + str(conf['unit'])
        elif setting_type == 6:
            # 需要加引号的配置
            setting_dict[conf['setting_name']] = f"\'{conf['val']}\'"
        else:
            # 常见类型(1)和布尔类型(2)和enum类型(5)
            setting_dict[conf['setting_name']] = conf['val']

    return setting_dict


def modify_setting_list(setting_list, pdict):
    # setting_list: [{"setting_name": "s", "val": 11}, {}]
    if not setting_list:
        setting_list = []
    for s in setting_list:
        if s['setting_name'] in pdict.keys():
            s['val'] = pdict[s['setting_name']]
            del pdict[s['setting_name']]
    setting_list.extend([{"setting_name": k, "val": v} for k, v in pdict.items()])

    return setting_list


def get_db_conn(db_id):
    """
    通过db_id 获取连接
    """
    sql = "SELECT host as host, port as port, db_detail->'db_user' as db_user," \
          "db_detail->'db_pass' as db_pass  FROM clup_db WHERE db_id=%s"
    rows = dbapi.query(sql, (db_id,))
    if len(rows) == 0:
        return 400, 'The instance does not exist.'
    db_dict = rows[0]
    db_dict['db_name'] = 'template1'
    try:
        conn = dao.get_db_conn(db_dict)
        if not conn:
            return 400, 'Failed to connect to the database!'
    except Exception as e:
        return 400, f'Failed to connect to the database: {str(e)}'
    return 0, conn


def check_sr_conn(db_id, up_db_id, repl_app_name=None):
    """
    检查流复制连接, 如果传了repl_app_name参数就不需要再去查询了
    @param db_id:
    @param up_db_id:
    @param repl_app_name:
    @return:
    """
    application_name = repl_app_name
    if not repl_app_name:
        db = dao.get_db_info(db_id)
        if not db and not repl_app_name:
            return -1, f'No database(db_id={db_id}) information found', {}
        application_name = db[0]['repl_app_name']
    try:
        err_code, conn = get_db_conn(up_db_id)
        if err_code != 0:
            return err_code, f'Failed to obtain the database connection: {conn}', ''
        sql = "SELECT count(*) AS cnt  FROM pg_stat_replication WHERE application_name=%s AND state='streaming'"
        rows = dao.sql_query(conn, sql, (application_name, ))

    except Exception as e:
        return -1, repr(e), {}
    return 0, '', rows[0]


def check_auto_conf(host, pgdata, item_list):
    """
    host, pgdata, item_dict
    """
    err_code, err_msg = rpc_utils.get_rpc_connect(host)
    if err_code != 0:
        return -1, err_msg

    rpc = err_msg
    try:
        auto_conf = f'{pgdata}/postgresql.auto.conf'
        err_code, item_dict = rpc.read_config_file_items(auto_conf, item_list)
        if err_code != 0:
            return err_code, item_dict
    finally:
        rpc.close()
    return 0, item_dict


def alter_system_conf(db_id, k, v):
    try:
        db_dict = dao.get_db_conn_info(db_id)
        conn = dao.get_db_conn(db_dict)
        sql = f"""ALTER SYSTEM SET {k}="{v}" """
        dao.execute(conn, sql)
    except Exception as e:
        return -1, str(e)
    return 0, ""


def psql_test(db_id, sql):
    """
    db_dict: {
    host
    port
    db_user
    db_pass
    sql    }
    """
    db_dict = dao.get_db_conn_info(db_id)
    if not db_dict:
        return -1, f'No information was found for database {db_id}'
    try:
        conn = dao.get_db_conn(db_dict)
        if conn == '':
            return -1, "CONN_ERROR: cant connect the database, maybe is not started."
    except Exception as e:
        return -1, "CONN_ERROR: " + str(e)
    try:
        rows = dao.sql_query(conn, sql)
    except Exception as e:
        return -1, "SQL_ERROR: " + str(e)
    return 0, rows


def get_pg_tblspc_dir(db_id):
    db_dict = dao.get_db_conn_info(db_id)
    table_space = []
    try:
        conn = dao.get_db_conn(db_dict)
        sql = "SELECT spcname AS name, pg_catalog.pg_tablespace_location(oid) AS location FROM pg_catalog.pg_tablespace where spcname not in ('pg_default', 'pg_global');"
        table_space = dao.sql_query(conn, sql)
        conn.close()
    except Exception as e:
        logging.error(f'get primary db info error: {repr(e)}')
    tblspc_dir = []
    for space in table_space:
        tblspc_dir.append({'old_dir': space['location'], 'new_dir': space['location']})
    return tblspc_dir


def get_pg_setting_name_type_dict():
    name_type_dict = dict()
    sql = "SELECT setting_name,setting_type FROM clup_init_db_conf"
    rows = dbapi.query(sql)
    for row in rows:
        name_type_dict[row['setting_name']] = row['setting_type']
    return name_type_dict


    # 通过sql查询当前正在使用的配置
    # db_dict = dao.get_db_conn_info(db_id)
    # conn = dao.get_db_conn(db_dict)
    # rows = dao.sql_query(conn, 'SHOW ALL')
    # setting_dict = {row['name']: row['setting'] for row in rows if row['name'] in data.keys()}
    # data.update(setting_dict)


def get_pg_setting_item_val(item):
    val = item['val']
    if isinstance(val, str):
        val = val.strip("'")
    if 'unit' in item:
        unit = item['unit']
    else:
        unit = ''

    if unit == '':
        return val
    elif unit == 'B':
        return int(val)
    elif unit == 'kB':
        return 1024 * int(val)
    elif unit == 'MB':
        return 1024 * 1024 * int(val)
    elif unit == 'GB':
        return 1024 * 1024 * 1024 * int(val)
    elif unit == 'TB':
        return 1024 * 1024 * 1024 * 1024 * int(val)
    elif unit == 's':
        return 1000 * int(val)
    elif unit == 'ms':
        return int(val)
    elif unit == 'min':
        return 60 * 1000 * int(val)
    elif unit == 'h':
        return 60 * 60 * 1000 * int(val)
    elif unit == 'd':
        return 3600 * 24 * 1000 * int(val)



def get_all_settings(db_id, condition_dict):
    """
    读取数据库所有的参数,优先级:postgresql.auto.conf > postgresql.conf > pg_settings
    """

    sql = "SELECT host, pgdata FROM clup_db WHERE db_id = %s"
    rows = dbapi.query(sql, (db_id, ))
    if not rows:
        return -1, f'No database(db_id: {db_id}) information found'
    err_code, err_msg = rpc_utils.get_rpc_connect(rows[0]['host'])
    if err_code != 0:
        return -1, f'agent connection failure: {err_msg}'
    rpc = err_msg
    try:
        pgdata = rows[0]['pgdata']
        conf_file = pgdata + '/postgresql.conf'
        pg_auto_conf_file = pgdata + '/postgresql.auto.conf'

        sql = "select category, name, setting, unit, vartype, context, enumvals, min_val, max_val, short_desc, extra_desc, pending_restart from pg_settings where true "
        args = []
        for key, value in condition_dict.items():
            if value:
                sql += f"and {key} like %s"
                args.append(value)
        # 连接目标库查询设置
        db_dict = dao.get_db_conn_info(db_id)
        conn = dao.get_db_conn(db_dict)
        setting_rows = dao.sql_query(conn, sql, tuple(args))
        # 取出所有配置项,这里的配置值是默认值
        all_conf_list = [dict(row) for row in setting_rows]
        conf_list = [cnf['name'] for cnf in all_conf_list]
        conf_file = pgdata + '/postgresql.conf'
        pg_auto_conf_file = pgdata + '/postgresql.auto.conf'
        # 读取配置文件中的配置
        err_code, data = rpc.read_config_file_items(conf_file, conf_list)
        if err_code != 0:
            return -1, data
        is_exists = rpc.os_path_exists(pg_auto_conf_file)
        auto_data = None
        if is_exists:
            err_code, err_msg = rpc.read_config_file_items(pg_auto_conf_file, conf_list)
            if err_code != 0:
                return -1, err_msg
            auto_data = err_msg
        # postgresql.auto.conf中的优先级高,用postgresql.auto.conf覆盖postgresql.conf中的相同配置项
        if auto_data:
            data.update(auto_data)
        # 配置格式处理
        for setting_name in data:
            if not isinstance(data[setting_name], str):  # 如果已经不是字符串,就不要处理了。
                continue
            # 去掉前后的单引号
            if len(data[setting_name]) >= 1:
                if data[setting_name][0] == "'":
                    data[setting_name] = data[setting_name][1:]
            if len(data[setting_name]) >= 1:
                if data[setting_name][-1] == "'":
                    data[setting_name] = data[setting_name][:-1]
        # 未做配置或配置为空的取默认值
        for conf in all_conf_list:
            # 判断配置类型setting_type
            if conf['unit']:
                if conf['unit'] in ['s', 'ms', 'min', 'h', 'd']:
                    conf['setting_type'] = 4
                    conf['setting'] = int(conf['setting']) if conf['setting'].isdigit() else conf['setting']
                elif conf['unit'] in ['B', 'KB', '8kB', 'kB', 'MB', 'GB', 'TB']:
                    conf['setting_type'] = 3
                    conf['setting'] = int(conf['setting']) if conf['setting'].isdigit() else conf['setting']
                    # if conf['unit'] == '8kB':
                    #     conf['setting'] = int(conf['setting']) * 8 if conf['setting'].isdigit() else conf['setting']
                    #     conf['unit'] = 'kB'
                    #     conf['min_val'] = str(int(conf['min_val']) * 8)
                    #     conf['max_val'] = str(int(conf['max_val']) * 8)
                    # else:
                    #     conf['setting'] = int(conf['setting']) if conf['setting'].isdigit() else conf['setting']
                else:
                    # 有单位但类型未知归为类型6
                    conf['setting_type'] = 6
            else:
                if conf['vartype'] in ['enum']:
                    conf['setting_type'] = 5
                elif conf['vartype'] in ['bool']:
                    conf['setting_type'] = 2
                elif conf['vartype'] in ['string']:
                    conf['setting_type'] = 6
                    # real和integer归为类型1
                elif conf['vartype'] == 'integer':
                    conf['setting'] = int(conf['setting'])
                    conf['setting_type'] = 1
                elif conf['vartype'] == 'real':
                    conf['setting'] = float(conf['setting'])
                    conf['setting_type'] = 1
                else:
                    conf['setting_type'] = 1

            # 获取到的conf文件配置值装入列表
            conf_name = conf['name']
            if conf_name in data:
                conf['conf_setting'] = data[conf_name]
                conf['conf_unit'] = ''
                # -------对配置参数是否生效进行检查,对比配置文件和数据库pg_settings表数值、单位进行判断-------
                # 因为关联性配置导致取值disabled的或者配置文件未配置视为已生效
                if conf['setting'] == '(disabled)' or not data[conf_name]:
                    conf['take_effect'] = 1
                elif conf['unit'] in ['s', 'ms', 'min', 'h', 'd']:
                    conf_value = pg_utils.get_time_with_unit(conf['setting'], conf['unit'])
                    unit = ''.join([i for i in data[conf_name] if i.isalpha()])
                    val = data[conf_name].replace(unit, '')
                    val = val if val else 0
                    unit = unit if unit else conf['unit']
                    data_value = pg_utils.get_time_with_unit(val, unit)
                    if data_value == conf_value:
                        conf['take_effect'] = 1
                    else:
                        conf['take_effect'] = 0
                    conf['conf_setting'] = val
                    conf['conf_unit'] = unit
                    conf['min_val'] = pg_utils.get_time_with_unit(conf['min_val'], conf['unit'])
                    conf['max_val'] = pg_utils.get_time_with_unit(conf['max_val'], conf['unit'])
                elif conf['unit'] in ['B', 'KB', '8kB', 'kB', 'MB', 'GB', 'TB']:
                    conf_value = pg_utils.get_val_with_unit(conf['setting'], conf['unit'])
                    unit = ''.join([i for i in data[conf_name] if i.isalpha()])
                    val = data[conf_name].replace(unit, '')
                    val = val if val else 0
                    unit = unit if unit else conf['unit']
                    data_value = pg_utils.get_val_with_unit(val, unit)
                    if conf['setting'] == pg_utils.fomart_by_unit(data_value, conf['unit']):
                        conf['take_effect'] = 1
                    else:
                        conf['take_effect'] = 0
                    conf['conf_setting'] = val
                    conf['conf_unit'] = unit
                    conf['min_val'] = pg_utils.get_val_with_unit(conf['min_val'], conf['unit'])
                    conf['max_val'] = pg_utils.get_val_with_unit(conf['max_val'], conf['unit'])
                elif conf['vartype'] == 'integer':
                    if int(conf['setting']) == int(data[conf_name]):
                        conf['take_effect'] = 1
                    else:
                        conf['take_effect'] = 0
                elif conf['vartype'] == 'real':
                    if float(conf['setting']) == float(data[conf_name]):
                        conf['take_effect'] = 1
                    else:
                        conf['take_effect'] = 0
                elif str(conf['setting']) == str(data[conf_name]):
                    conf['take_effect'] = 1
                else:
                    conf['take_effect'] = 0
            else:
                conf['conf_setting'] = ''
                conf['take_effect'] = 1
            # 需要重启的参数判断生效
            # if conf['pending_restart']:
            #     conf['take_effect'] = 0
        return 0, all_conf_list
    except Exception:
        err_msg = traceback.format_exc()
        return -1, err_msg
    finally:
        rpc.close()


def modify_db_conf(pdict):
    """
    修改一条pg_settings配置,并同步修改postgresql.auto.conf,postgresql.conf文件
    """
    # 修改的配置信息
    db_id = pdict['db_id']
    setting_name = pdict['setting_name']
    setting_value = pdict['setting_value']
    setting_unit = pdict['setting_unit'] if pdict['setting_unit'] else ''
    is_reload = pdict['is_reload']
    try:
        # pg_settings表部分数据不能直接修改,在配置文件修改后重启自动同步到表内
        sql = "SELECT host, port, pgdata, db_detail->'instance_type' as instance_type  FROM clup_db WHERE db_id = %s"
        rows = dbapi.query(sql, (db_id, ))
        if not rows:
            return -1, f'No database(db_id: {db_id}) information found'
        err_code, err_msg = rpc_utils.get_rpc_connect(rows[0]['host'])
        if err_code != 0:
            return -1, f'agent connection failure: {err_msg}'
        rpc = err_msg
        # 连接目标库查询设置
        db_dict = rows[0]

        # 获取原有配置
        err_code, err_msg = get_pg_setting_list(pdict['db_id'], setting_name=setting_name)
        if err_code != 0:
            return -1, f'The original configuration of the database({pdict["db_id"]}) cannot be obtained.error: {err_msg}'

        pre_setting = err_msg[0]
        if pre_setting.get("is_string"):
            setting_dict = {setting_name: f"'{setting_value}'"}
        else:
            setting_dict = {setting_name: f"{setting_value}{setting_unit}"}

        # 如果是shared_preload_libraries配置项必须包含这个插件
        if setting_name == 'shared_preload_libraries' and "pg_stat_statements" not in setting_value:
            return -1, f'Database({pdict["db_id"]}) needs pg_stat_statements plug, please fill it in the shared_preload_libraries.'

        # 修改配置文件配置数据
        auto_flag = False
        if pre_setting.get("is_in_auto_conf"):
            auto_flag = True
        err_code, err_msg = pg_db_lib.modify_pg_conf(rpc, db_dict['pgdata'], setting_dict, is_pg_auto_conf=auto_flag)
        if err_code != 0:
            return -1, err_msg

        # reload
        if is_reload:
            pg_db_lib.reload(db_dict['host'], db_dict['pgdata'])

        return 0, 'Configuration modification succeeded'
    except Exception as e:
        return -1, str(e)
    finally:
        rpc.close()


def get_pg_setting_list(db_id, fill_up_default=False, setting_name=None):
    """
    读取数据库配置参数,读取postgresql.conf和postgresql.auto.conf中的配置项目
    @param db_id:
    @param fill_up_default: 如果配置文件中无此项,是否填充clup的默认值
    @return:
    """
    try:
        sql = "SELECT host, pgdata FROM clup_db WHERE db_id = %s"
        rows = dbapi.query(sql, (db_id, ))
        if not rows:
            return -1, f'No database(db_id: {db_id}) information found'
        err_code, err_msg = rpc_utils.get_rpc_connect(rows[0]['host'])
        if err_code != 0:
            return -1, f'agent connection failure: {err_msg}'
        rpc = err_msg
        try:
            pgdata = rows[0]['pgdata']
            conf_file = pgdata + '/postgresql.conf'
            pg_auto_conf_file = pgdata + '/postgresql.auto.conf'

            where_cond = ""
            if setting_name:
                where_cond = f" WHERE setting_name='{setting_name}'"
            sql = f"SELECT setting_name, setting_type, val, unit, common_level FROM clup_init_db_conf {where_cond}"
            rows = dbapi.query(sql)
            if not rows and setting_name:
                # 有些参数在clup_init_db_conf中可能没有,那就在pg_settings中查
                sql = f"SELECT name, setting, unit, vartype FROM pg_settings WHERE name = '{setting_name}'"
                # 暂时先按照csumdb(PG12)的版本为准,理论上应该查询指定的数据库
                result = dbapi.query(sql)
                if result:
                    setting_type_dict = {
                        "bool": 2,
                        "enum": 5,
                        "integer": 3,
                        "real": 4,
                        "string": 6
                    }
                    result_dict = dict(result[0])
                    # 如果是integer且没有单位,则为无单位普通数字类型
                    if result_dict["vartype"] == 'integer' and not result_dict.get("unit"):
                        setting_type_dict["integer"] = 1

                    # 如果是字符串, 读取的值是不带单引号的,这里添加上,方便后面处理
                    setting_value = result_dict["setting"]
                    if result_dict["vartype"] == 'string':
                        result_dict["setting"] = f"'{setting_value}'"

                    setting_name_dict = {
                        "setting_name": result_dict["name"],
                        "common_level": 3,
                        "val": result_dict["setting"],
                        "unit": result_dict["unit"],
                        "setting_type": setting_type_dict[result_dict["vartype"]]
                    }
                    rows = [setting_name_dict]

            conf_list = [row['setting_name'] for row in rows]
            conf_unit_list = [row['setting_name'] for row in rows if row['setting_type'] == 3 or row['setting_type'] == 4]

            # 处理下参数类型
            conf_type_dict = dict()
            for setting in rows:
                conf_type_dict[setting["setting_name"]] = setting["setting_type"]

            name_type_dict = {}
            name_clup_default_val_dict = {}
            name_common_level_dict = {}
            for row in rows:
                name_common_level_dict[row['setting_name']] = row['common_level']
                name_type_dict[row['setting_name']] = row['setting_type']
                name_clup_default_val_dict[row['setting_name']] = row

            # # 通过sql查询默认值
            # db_dict = dao.get_db_conn_info(db_id)
            # conn = dao.get_db_conn(db_dict)
            # rows = dao.sql_query(conn, 'select name,reset_val from pg_settings')
            # setting_default_dict = {}
            # for row in rows:
            #     setting_default_dict[row['name']] = row['reset_val']

            err_code, data = rpc.read_config_file_items(conf_file, conf_list)
            if err_code != 0:
                return -1, data

            is_exists = rpc.os_path_exists(pg_auto_conf_file)
            auto_data = None
            if is_exists:
                err_code, err_msg = rpc.read_config_file_items(pg_auto_conf_file, conf_list)
                if err_code != 0:
                    return -1, err_msg
                auto_data = err_msg
        finally:
            rpc.close()
        # postgresql.auto.conf中的优先级高,用postgresql.auto.conf覆盖postgresql.conf中的相同配置项
        if auto_data:
            data.update(auto_data)

        setting_list = []
        # 存在参数是默认值,但是在配置文件已经被删除的情况
        if setting_name and not data:
            data = {
                setting_name_dict["setting_name"]: setting_name_dict["val"]
            }
        for setting_name, value in data.items():
            common_level = name_common_level_dict[setting_name]
            conf = {"setting_name": setting_name, "val": value, "common_level": common_level, "is_string": False}

            # 如果已经不是字符串,就不要处理了。
            if isinstance(data[setting_name], str):
                # 去掉前后的单引号
                if len(value) >= 1:
                    if "'" in value:
                        conf["val"] = data[setting_name].strip("'")
                if conf_type_dict[setting_name] == 6:
                    conf["is_string"] = True
                #     if data[setting_name][0] == "'":
                #         data[setting_name] = data[setting_name][1:]
                # if len(data[setting_name]) >= 1:
                #     if data[setting_name][-1] == "'":
                #         data[setting_name] = data[setting_name][:-1]

                # 如果值为空,则设置为默认值：
                # if data[setting_name] == '':
                #     if setting_name in setting_default_dict:
                #         data[setting_name] = setting_default_dict[setting_name]
                #     else:
                #         continue

        # for k, v in data.items():
            if fill_up_default:
                # v为空字符串,则表示配置文件中没有配置的项,把这些项的值设置为clup_init_db_conf中的默认值
                if value == '':
                    conf['common_level'] = 0
                    item = name_clup_default_val_dict[setting_name]
                    conf['val'] = item['val']
                    if item['unit']:
                        conf['unit'] = item['unit']

            if setting_name in conf_unit_list:  # setting_type是3和4的情况
                if value.isdigit():  # 这是如参数track_activity_query_size,可以带一个单位B,也可以不带
                    setting_type = name_type_dict[setting_name]
                    if setting_type == 3:
                        conf['unit'] = 'B'
                    elif setting_type == 4:
                        conf['unit'] = 'ms'
                else:
                    res = re.findall(r'(-{0,1}\d+)(\D+)', value)  # 128MB  -> [(128, 'MB')]
                    if not res:
                        continue
                    conf['unit'] = res[0][1]
                    conf['val'] = res[0][0]

                # 把是数字的值从字符串类型转换成数字类型
                conf['val'] = int(conf['val'])
            else:
                pass
                # if k in name_type_dict:
                #     setting_type = name_type_dict[k]
                #     if setting_type == 1:
                #         if '.' in conf['val']:
                #             conf['val'] = float(conf['val'])
                #         else:
                #             conf['val'] = int(conf['val'])

            if auto_data and setting_name in auto_data:
                conf['is_in_auto_conf'] = 1
            else:
                conf['is_in_auto_conf'] = 0
            setting_list.append(conf)

        return 0, setting_list
    except Exception:
        return -1, traceback.format_exc()


def get_db_room(db_id):
    """
    从集群获取对应的机房名字
    @param db_id:
    @return:
    """
    sql = "SELECT db_detail->'room_id' as room_id, cluster_id FROM clup_db WHERE db_id=%s"
    rows = dbapi.query(sql, (db_id, ))
    if not rows:
        return -1, f'No database(db_id: {db_id}) information found'
    room_id = rows[0]['room_id'] if rows[0]['room_id'] else '0'
    cluster_id = rows[0]['cluster_id']
    if not cluster_id:
        return 0, {"room_name": '默认机房', "room_id": '0'}
    sql = "SELECT cluster_data  FROM clup_cluster WHERE cluster_id = %s"
    rows = dbapi.query(sql, (cluster_id, ))
    if not rows:
        return -1, f'No cluster(cluster_id: {cluster_id}) information found'
    cluster_data = rows[0]['cluster_data']
    room_info = cluster_data.get('rooms', {})
    if not room_info or (room_id == '0' and not room_info.get(str(room_id))):
        room = {"room_name": "默认机房",
                "vip": cluster_data['vip'],
                "read_vip": cluster_data.get('read_vip', ''),
                "cstlb_list": cluster_data.get('cstlb_list', ''),
                "room_id": room_id
                }
    else:
        room = room_info.get(str(room_id))
        room['room_id'] = room_id
    return 0, room


def get_new_cluster_data(cluster_id, room_id):
    sql = "SELECT cluster_data FROM clup_cluster WHERE cluster_id=%s"
    rows = dbapi.query(sql, (cluster_id, ))
    if not rows:
        return {}
    cluster_data = rows[0]['cluster_data']
    room_info = cluster_data.get('room_info', {})
    cluster_data.update(room_info.get(str(room_id), {}))
    return cluster_data


def update_cluster_room_info(cluster_id, db_id=None):
    """
    更新集群的机房信息
    @param cluster_id:
    @return:
    """
    sql = "SELECT cluster_type, cluster_data FROM clup_cluster WHERE cluster_id=%s"
    rows = dbapi.query(sql, (cluster_id, ))
    if not rows:
        return -1, f'No cluster(cluster_id: {cluster_id}) information found'

    cluster_type = rows[0]['cluster_type']
    if cluster_type == 2:  # 跳过共享存储的集群对机房信息的更新
        return

    cluster_data = rows[0]['cluster_data']
    room_info = cluster_data.get('rooms', {})
    if not db_id:
        sql = "SELECT db_detail->'room_id' as room_id FROM clup_db WHERE is_primary = 1 AND cluster_id=%s"
        rows = dbapi.query(sql, (cluster_id, ))
    else:
        sql = "SELECT db_detail->'room_id' as room_id FROM clup_db WHERE db_id = %s"
        rows = dbapi.query(sql, (db_id,))
    if not rows:
        return -1, f'The primary database in the cluster(cluster_id: {cluster_id}) is not found.'

    primary_room_id = rows[0]['room_id']
    primary_room_id = primary_room_id if primary_room_id else '0'

    if not room_info:
        room_info = {'0': {
            'vip': cluster_data['vip'],
            'read_vip': cluster_data.get('read_vip', ''),
            'cstlb_list': cluster_data.get('cstlb_list', ''),
            'room_name': '默认机房',
        }
        }
    primary_room_info = room_info[str(primary_room_id)]
    cluster_data.update(primary_room_info)
    cluster_data['rooms'] = room_info
    try:
        sql = "UPDATE clup_cluster SET cluster_data= %s WHERE cluster_id=%s"
        dbapi.execute(sql, (json.dumps(cluster_data), cluster_id))
    except Exception as e:
        return -1, f'The cluster(cluster_id: {cluster_id}) room data fails to be updated. error: {repr(e)}'
    return 0, 'ok'


def get_max_lsn_db(db_list):
    """
    db_list :  [{'db_id': 277},{'db_id': 292}]
    @param db_list:
    @return:
    """
    curr_lsn = ''
    new_pri_pg = None
    for db in db_list:
        db_id = db['db_id']
        db = dao.get_db_info(db_id)
        if not db:
            logging.error(f"Failed to obtain database(db_id={db_id}) information.")
            continue
        err_code, lsn, _ = probe_db.get_last_lsn(db[0]['host'], db[0]['port'], db[0]['repl_user'], db_encrypt.from_db_text(db[0]['repl_pass']))
        if err_code != 0:
            logging.error(f"Failed to obtain database(db_id={db_id}) lsn information. error: {lsn}")
            continue
        if lsn > curr_lsn and not new_pri_pg:
            curr_lsn = lsn
            new_pri_pg = db_id
            continue
    return new_pri_pg


def get_current_cluster_room(cluster_id):
    """
    获取集群当前机房信息
    @param cluster_id:
    @return:
    """
    sql = "SELECT cluster_data->'rooms' as rooms FROM clup_cluster WHERE cluster_id = %s"
    room_info_rows = dbapi.query(sql, (cluster_id, ))
    if not room_info_rows:
        return f'No cluster(cluster_id: {cluster_id}) information found'
    sql = "SELECT db_detail->'room_id' as room_id FROM clup_db WHERE cluster_id=%s AND is_primary=1"
    room_id_rows = dbapi.query(sql, (cluster_id, ))
    if not room_id_rows:
        return f"Cluster(cluster_id={cluster_id}) primary database information is not found."
    rooms = room_info_rows[0]['rooms'] if room_info_rows[0]['rooms'] else {}
    cur_room_id = room_id_rows[0]['room_id']
    cur_room_id = cur_room_id if cur_room_id else '0'
    cur_room_info = rooms.get(str(cur_room_id), {})
    cur_room_info['room_id'] = cur_room_id
    return cur_room_info


def get_db_relation_info(db_dict):
    for db in db_dict['children']:
        err_code, room = get_db_room(db['db_id'])
        db['room_name'] = room.get('room_name', '') if err_code == 0 else ''
        rows = dao.get_lower_db(db['db_id'])
        if rows:
            db['children'] = rows
            get_db_relation_info(db)


def pretty_size(val):
    """传进来的值是以字节为单位,转为 kB MB GB TB的值
    Args:
        val ([type]): [description]

    Returns:
        [type]: [description]
    """
    if val >= 100 * 1024 * 1024 * 1024 * 1024:
        return f'{val/1024/1024/1024/1024:.0f}TB'
    elif val >= 100 * 1024 * 1024 * 1024:
        return f'{val/1024/1024/1024:.0f}GB'
    elif val >= 100 * 1024 * 1024:
        return f'{val/1024/1024:.0f}MB'
    elif val >= 100 * 1024:
        return f'{val/1024:.0f}kB'
    else:
        return f'{val}'


def pretty_ms(val):
    """传进来的值是以字节为单位,转为单位为 s min h d 的值

    Args:
        val ([type]): [description]

    Returns:
        [type]: [description]
    """
    if val >= 10 * 24 * 3600 * 1000:
        return f'{val/24/3600/1000:.0f}d'
    elif val >= 10 * 3600 * 1000:
        return f'{val/3600/1000:.0f}h'
    elif val >= 10 * 60 * 1000:
        return f'{val/60/1000:.0f}MB'
    elif val >= 1000:
        return f'{val/1000:.0f}s'
    else:
        return f'{val}ms'



if __name__ == '__main__':
    pass
