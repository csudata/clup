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
@description: ha的一些管理功能的实现
"""

import copy
import json
import logging
import os
import time
import traceback

import cluster_state
import config
import dao
import database_state
import db_encrypt
import dbapi
import general_task_mgr
import lb_mgr
import long_term_task
import node_state
import pg_db_lib
import pg_helpers
import pg_utils
import ping_lib
import polar_helpers
import polar_lib
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


def get_last_lsn(cluster_id):
    clu_db_list = dao.get_cluster_db_list(cluster_id)
    ret_data = []
    cluster_dict = dao.get_cluster(cluster_id)
    if cluster_dict is None or (not clu_db_list):
        logging.info(f"get_last_lsn when cluster({cluster_id}) has been deleted")
        return 0, []
    db_port = cluster_dict['port']
    db_user = clu_db_list[0]['db_user']
    db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])
    for db_dict in clu_db_list:
        if db_dict['state'] != node_state.NORMAL:  # 不是正常状态的
            ret_data.append([db_dict['db_id'], db_dict['host'], db_dict['is_primary'], 'unknown', 'unknown'])
            continue
        err_code, lsn, timeline = pg_utils.get_last_lsn(db_dict['host'], db_port, db_user, db_pass)
        if err_code != 0:
            ret_data.append([db_dict['db_id'], db_dict['host'], db_dict['is_primary'], 'error', 'error'])
        else:
            ret_data.append([db_dict['db_id'], db_dict['host'], db_dict['is_primary'], timeline, lsn])
    return 0, ret_data


def get_repl_delay(cluster_id):
    """
    获得各个备库的延迟
    :return:
    """
    clu_db_list = dao.get_cluster_db_list(cluster_id)
    cluster_dict = dao.get_cluster(cluster_id)
    if cluster_dict is None or (not clu_db_list):
        logging.info(f"get_repl_delay when cluster({cluster_id}) has been deleted")
        return 0, []

    pri_db = None
    for db_dict in clu_db_list:
        if db_dict['is_primary'] and db_dict['state'] == node_state.NORMAL:
            pri_db = db_dict
            break

    # 如果没有找到正常的主库，则找到一个异常主库
    if not pri_db:
        for db_dict in clu_db_list:
            if db_dict['is_primary']:
                pri_db = db_dict
                break

    delay_data = []
    if pri_db:  # 如果没有找到主库，是无法取到相应延迟的
        db_port = cluster_dict['port']
        db_user = clu_db_list[0]['db_user']
        db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])

        try:
            # delay_data = pg_utils.get_repl_delay(pri_db['host'], db_port, db_user, db_pass, cur_wal)
            for db in clu_db_list:
                sql = f"SELECT count(*) FROM clup_db WHERE up_db_id = {db['db_id']} "
                rows = dbapi.query(sql)
                if rows[0]['count'] == 0:
                    continue
                db_dict = {
                    'db_name': 'template1',
                    'host': pri_db['host'],
                    'port': db_port,
                    'db_user': db_user,
                    'db_pass': db_pass,
                    'real_pass': True
                }
                try:
                    conn = dao.get_db_conn(db_dict)

                    # 从主库获取current_wal
                    cur_wal_rows = dao.get_current_wal_lsn(conn)
                except Exception as e:
                    logging.error(f'get current wal lsn ERROR: {repr(e)}')
                    cur_wal_rows = []
                else:
                    conn.close()
                if cur_wal_rows:
                    cur_wal = cur_wal_rows[0]['cur_wal']
                    cur_delay_data = pg_utils.get_repl_delay(db['host'], db_port, db_user, db_pass, cur_wal)
                    delay_data.extend(cur_delay_data)
        except Exception:
            logging.error(f"Can not get replication delay: {traceback.format_exc()}")
            delay_data = []

    db_port = cluster_dict['port']
    current_lsn = 'unknown'
    ret_data = []
    for db_dict in clu_db_list:
        delay_row = {
            'host': db_dict['host'],
            'port': db_port,
            'state': db_dict['state'],
            'is_primary': db_dict['is_primary']
        }

        is_find = False
        for row in delay_data:
            if row['repl_name'] == db_dict['repl_app_name']:
                delay_list = ['sent_delay', 'write_delay', 'flush_delay', 'replay_delay']
                for k in delay_list:
                    if row[k] is None:
                        delay_row[k] = 'unknown'
                    else:
                        # 如果延时小于0 就默认为0
                        delay_row[k] = int(row[k]) if int(row[k]) >= 0 else 0
                delay_row['is_sync'] = row['is_sync']
                delay_row['repl_state'] = row['state']
                delay_row['current_lsn'] = row['current_lsn']
                is_find = True
                if current_lsn == 'unknown':
                    current_lsn = row['current_lsn']
                break
        if not is_find:
            if db_dict['is_primary']:
                value = 'N/A'
                delay_row['current_lsn'] = current_lsn
            else:
                value = 'unknown'
            delay_row['current_lsn'] = value
            delay_row['sent_delay'] = value
            delay_row['write_delay'] = value
            delay_row['flush_delay'] = value
            delay_row['replay_delay'] = value
            delay_row['is_sync'] = value
            delay_row['repl_state'] = value
        ret_data.append(delay_row)

    return 0, ret_data


def can_be_failback(cluster_id: str, db_id: int):
    """
    探测能否当前情况下修复这个节点
    :param cluster_id:
    :param db_id:
    :return:
        返回0: 表示可以;
        返回-1: 表示不可以;
        返回1表示: 不需要
    """

    pre_msg = f"Check can be failback(cluster={cluster_id}, db_id={db_id})"
    clu_db_list = dao.get_cluster_db_list(cluster_id)
    cluster_dict = dao.get_cluster(cluster_id)
    if cluster_dict is None or (not clu_db_list):
        logging.info(f"can_be_failback when cluster({cluster_id}) has been deleted")
        return 1, f"can_be_failback when cluster({cluster_id}) has been deleted"

    state = cluster_dict['state']
    if state == cluster_state.REPAIRING or state == cluster_state.FAILOVER or state == cluster_state.CHECKING:
        str_state = cluster_state.to_str(state)
        err_msg = f"Cluster is being operated on(state={str_state}). Please try repairing the node later!"
        logging.info(f"{pre_msg}:{err_msg}")
        return -1, err_msg

    # 先找到正常的主数据库
    logging.info(f'{pre_msg}: find primary database ...')
    for db_dict in clu_db_list:
        if db_dict['is_primary'] and db_dict['state'] == node_state.NORMAL:
            pri_db = db_dict
            break
    else:
        msg = "can not find normal primary database"
        logging.info(f'{pre_msg}: {msg}')
        return -1, msg

    logging.info(f"{pre_msg}: find primary database: {pri_db['host']}:{pri_db['port']}")
    logging.info('{pre_msg}: find fault database ...')
    # 找到要修复的数据库
    for db_dict in clu_db_list:
        if db_dict['db_id'] == db_id:
            failback_db_dict = db_dict
            break
    else:
        err_msg = 'can not find fault database, failback completed.'
        logging.info(f"{pre_msg}:{err_msg}")
        return 1, err_msg

    if failback_db_dict['state'] != node_state.FAULT:
        err_msg = f"This database({failback_db_dict['host']}) state is not Fault, not need failback."
        logging.info(f"{pre_msg}:{err_msg}")
        return 1, err_msg
    return 0, ''


def failback_trigger_func(task_id, msg_prefix, cluster_dict, before_cluster_dict, pri_host, db_port, failback_host, db_user, db_pass):

    # 如果配置了切换时调用的数据库函数，则调用此函数
    trigger_db_name = cluster_dict['trigger_db_name']
    trigger_db_func = cluster_dict['trigger_db_func']
    if trigger_db_name and trigger_db_func:
        log_info(task_id,
            f"{msg_prefix}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary ...")

        sql = "select {0}(%s, %s, %s, %s, %s);".format(trigger_db_func)
        err_code, err_msg = probe_db.exec_sql(
            pri_host, db_port, trigger_db_name, db_user, db_pass,
            sql,
            (2, msg_prefix, failback_host, json.dumps(before_cluster_dict), json.dumps(cluster_dict))
        )
        if err_code != 0:
            log_error(task_id, f"{msg_prefix}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary error: {err_msg}")


def failback(task_id: int, db_dict, restore_cluster_state):
    cluster_id = db_dict['cluster_id']
    db_id = db_dict['db_id']
    up_db_id = db_dict['up_db_id']
    msg_prefix = f"Failback(cluster={cluster_id}, db_id={db_id})"

    version = dao.get_db_version(db_id)
    # 后面把集群设置为之前的状态,如果是FAILED需要手工修复的就设为离线
    try:
        log_info(task_id, f'{msg_prefix}: begin...')
        clu_db_list = dao.get_cluster_db_list(cluster_id)
        before_cluster_dict = dao.get_cluster(cluster_id)
        if before_cluster_dict is None or (not clu_db_list):
            err_code = -1
            err_msg = f"failback when cluster({cluster_id}) has been deleted"
            return err_code, err_msg
        before_cluster_dict['db_list'] = copy.copy(clu_db_list)
        cluster_dict = copy.copy(before_cluster_dict)
        cluster_dict['db_list'] = clu_db_list
        db_port = cluster_dict['port']

        # 先找到正常的主数据库
        pri_db_dict = {}
        log_info(task_id, f'{msg_prefix}: find primary database ...')
        for db in clu_db_list:
            if db['is_primary'] and db['state'] == node_state.NORMAL:
                # 默认先找到主库
                pri_db_dict = db
                continue
            if db['db_id'] == up_db_id and db['state'] == node_state.NORMAL:
                # 如果所选上级库不是主库，替换掉
                pri_db_dict = db
                break
        if not pri_db_dict:
            err_code = -1
            err_msg = "can not find normal primary database"
            return err_code, err_msg
        pri_host = pri_db_dict['host']
        db_port = pri_db_dict['port']
        log_info(task_id, f"{msg_prefix}: find primary database: {pri_host}:{db_port}")
        log_info(task_id, f"{msg_prefix}: find fault database ...")
        # 找到要修复的数据库
        for db in clu_db_list:
            if db['db_id'] == db_id:
                failback_db_dict = db
                break
        else:
            err_code = -1
            err_msg = f'{msg_prefix}: can not find fault database, failback completed.'
            return err_code, err_msg

        failback_host = failback_db_dict['host']
        if failback_db_dict['state'] != node_state.FAULT:
            err_code = -1
            err_msg = f"This database({failback_host}) state is not Fault, not need failback."
            return err_code, err_msg

        dao.set_node_state(db_id, node_state.REPAIRING)
        log_info(task_id, f"{msg_prefix}: Try change recovery")

        # 12版本需要考虑postgresql.auto.conf文件中有没有配置primary_conninfo
        version = str(pri_db_dict['version'])
        # 没有版本信息或os_user信息或pg_bin_path信息，则获得这些信息并更新数据库
        if (not version) or (not pri_db_dict['os_user']) or (not pri_db_dict['pg_bin_path']):
            err_code, renew_dict = pg_helpers.renew_pg_bin_info(db_id)
            if err_code != 0:
                return err_code, err_msg
            pri_db_dict.update(renew_dict)

        # 12即更高版本的数据库，直接有个alter system更新primary_conninfo
        if int(version.split('.')[0]) >= 12:
            db = dao.get_db_info(db_id)
            up_db = dao.get_db_info(up_db_id)
            if not db or not up_db:
                err_code = -1
                err_msg = f'No relevant database information found.: db_id({db_id} or {up_db_id})'
                return err_code, err_msg
            db = db[0]
            up_db = up_db[0]
            err_code, item_dict = pg_helpers.check_auto_conf(db['host'], db['pgdata'], ['primary_conninfo'])
            if err_code == 0 and item_dict.get('primary_conninfo'):
                # 修改postgresql.auto.conf文件需要使用SQL alter system set xxx 自动修改文件
                log_info(task_id, f'{msg_prefix}: db({db_id}) alter system set primary_conninfo')
                db_pass = db_encrypt.from_db_text(up_db['db_pass'])
                up_conninfo = f"application_name={db['repl_app_name']} user={up_db['db_user']} password={db_pass}" \
                    f" host={up_db['host']} port={up_db['port']} sslmode=disable sslcompression=1"
                err_code, err_msg = pg_helpers.alter_system_conf(db_id, "primary_conninfo", up_conninfo)
                log_info(task_id, f'{msg_prefix}: db({db_id}) alter system set primary_conninfo end : {err_code}: {err_msg}')

        # 先更换上级库，如果更换完上级库后，流复制正常了，说明备库是正常的，不需要修复
        # 通过db_type设置cluster_type
        polar_type = polar_lib.get_polar_type(db_id)
        cluster_type = None
        if not polar_type:
            cluster_type = 1
            err_code, err_msg = pg_helpers.change_up_db_by_db_id(db_id, up_db_id)
        else:
            cluster_type = 11
            err_code, err_msg = polar_helpers.change_recovery_up_db(db_id, up_db_id, polar_type)

        if err_code == 0:
            log_info(task_id, f"{msg_prefix}: change db({db_id}) sr to db({up_db_id}) success.")
        else:
            log_info(task_id, f"{msg_prefix}: change db({db_id}) sr to db({up_db_id}) failed: "
            f"It is possible that the database has already been deleted and will be rebuilt later. This error message can be ignored. The error message is: {err_code}, {err_msg}")

        log_info(task_id, f"Failback: fault database: ({failback_db_dict['host']}:{failback_db_dict['port']})")
        db_user = clu_db_list[0]['db_user']
        db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])
        msg_prefix = f"Failback(cluster={cluster_id}, host={failback_db_dict['host']})"

        # 等5秒后检查流复制是否正常
        time.sleep(5)
        log_info(task_id, f'{msg_prefix}: checking failback database replication steaming is ok...')
        sql = "SELECT count(*) AS cnt  FROM pg_stat_replication WHERE application_name=%s AND state='streaming'"
        err_code, data = probe_db.run_sql(pri_db_dict['host'],
                                          db_port,
                                          'template1',
                                          db_user,
                                          db_pass,
                                          sql,
                                          (failback_db_dict['repl_app_name'],))
        if err_code != 0:
            err_msg = f'{msg_prefix}: failed to checking failback database replication steaming: {data}'
            return err_code, err_msg
        if data[0]['cnt'] > 0:
            err_msg = 'Repair succeeded.'

            # 修复成功的话重置failback的次数
            update_dict = json.dumps({"failback_count": 0})
            sql = "UPDATE clup_db SET db_detail = db_detail || (%s::jsonb) WHERE db_id = %s"
            dbapi.execute(sql, (update_dict, db_id))

            return 0, ''

        # 如果没有设置重新搭建，则直接返回失败
        if not db_dict.get('is_rewind'):
            err_code = -1
            err_msg = "The streaming replication status is abnormal. Please choose 'rewind' or rebuild the standby node to repair it."
            log_info(task_id, f'{msg_prefix}: {err_msg}')
            return err_code, err_msg

        # 备库在切换了上级库后，流复制还是不正常，需要重新搭建
        err_code, pdict = pg_helpers.get_db_params(up_db_id, db_id)
        if err_code != 0:
            err_msg = f'{msg_prefix}: Failed to retrieve database information: {pdict}'
            return err_code, err_msg
        pdict['task_id'] = task_id
        pdict['msg_prefix'] = msg_prefix
        pdict['task_type'] = 'ha'
        pdict['cluster_id'] = cluster_id
        pdict['before_cluster_dict'] = before_cluster_dict
        pdict['tblspc_dir'] = db_dict.get('tblspc_dir')
        pdict['rm_pgdata'] = db_dict.get('rm_pgdata')
        pdict['is_rebuild'] = db_dict.get('is_rebuild')
        pdict['db_user'] = db_user
        pdict['db_pass'] = db_pass
        pdict['db_id'] = db_id
        pdict['port'] = db_port
        pdict['version'] = version
     
        err_code, err_msg = rpc_utils.get_rpc_connect(pdict['host'])
        if err_code != 0:
            err_msg = f"{msg_prefix}: connect {pdict['host']} failed: {err_msg}"
            return err_code, err_msg
        rpc = err_msg
        try:
            # polardb 不进行pg_rewind操作
            if cluster_type == 1:
                # 旧版本程序没有pg_bin_path目录
                if not pdict['pg_bin_path']:
                    log_info(task_id, f'{msg_prefix}: old version no pg_bin_path, use which postres to get it ...')
                    cmd = f"su - {pdict['os_user']} -c 'which postgres'"
                    err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
                    if err_code != 0:
                        return err_code, err_msg
                    out_msg = out_msg.strip()
                    pg_bin_path = os.path.dirname(out_msg)
                    pdict['pg_bin_path'] = pg_bin_path
                    log_info(task_id, f'{msg_prefix}: pg_bin_path is {pg_bin_path}')

                log_info(task_id, 'Start trying pg_rewind recovery')
                err_code, err_msg = pg_db_lib.pg_rewind(rpc, pdict)
                if err_code == 0:
                    # 修复成功的话重置failback的次数
                    update_dict = json.dumps({"failback_count": 0})
                    sql = "UPDATE clup_db SET db_detail = db_detail || (%s::jsonb) WHERE db_id = %s"
                    dbapi.execute(sql, (update_dict, db_id))

                    return err_code, err_msg
                if err_code != 0:
                    log_info(task_id, f'{msg_prefix}: pg_rewind failed: {err_msg}')
                    if not pdict['is_rebuild']:
                        return err_code, err_msg

            # 通过pg_rewind仍然不能修复，则只能通过重新搭建备库的方式进行修复了
            pgdata = pdict['pgdata']
            log_info(task_id, f'{msg_prefix}: begin stop this database ...')
            pg_db_lib.stop(rpc, pgdata)
            log_info(task_id, f'{msg_prefix}: stop this database finished.')

            # 把需要改名或删除的旧目录放到old_dir_list中
            need_bak_dir_set = set()
            if rpc.os_path_exists(pgdata):
                need_bak_dir_set.add(pgdata)
                tbl_root_dir = f"{pgdata}/pg_tblspc"
                tbl_oid_list = rpc.os_listdir(tbl_root_dir)
                if tbl_oid_list is not None:  # 如果tbl_root_dir不存在，则tbl_oid_list是None
                    for tbl_oid in tbl_oid_list:
                        tmp_err_code, tmp_err_msg = rpc.os_readlink(f"{tbl_root_dir}/{tbl_oid}")
                        if tmp_err_code != 0:
                            continue
                        need_bak_dir_set.add(tmp_err_msg)

            # 表空间的新目录，如果不为空，也需要删除内容或改名
            tbl_list = pdict.get('tblspc_dir')
            try:
                for k in tbl_list:
                    tblspc_dir = k['new_dir']
                    if not rpc.dir_is_empty(tblspc_dir):
                        need_bak_dir_set.add(tblspc_dir)
            except Exception as e:
                err_code = -1
                err_msg = str(e)
                return err_code, err_msg

            if pdict['rm_pgdata']:
                for old_dir in need_bak_dir_set:
                    log_info(task_id, f'{msg_prefix}: delete directory: {old_dir}/*')
                    cmd = f"/bin/rm -rf {old_dir}/*"
                    err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
                    if err_code != 0:
                        err_msg = f"An error occurred while deleting the data directory({old_dir}/*).: {err_msg}"
                        return err_code, err_msg
            else:
                tm = time.strftime('%Y%m%d%H%M%S')
                for old_dir in need_bak_dir_set:
                    bak_old_dir = f'{old_dir}_{tm}'
                    log_info(task_id, f'{msg_prefix}: rename {old_dir} to {bak_old_dir}')
                    err_code, err_msg = rpc.os_rename(old_dir, bak_old_dir)
                    if err_code != 0:
                        err_msg = f"rename {old_dir} to {bak_old_dir} failed: {err_msg}"
                        return err_code, err_msg
            log_info(task_id, f'{msg_prefix}: begin build standby ...')


            # 获得unix_socket_directories，一般搭建备库时，配置.bashrc中的PGHOST
            log_info(task_id, f"{msg_prefix}: get unix_socket_directories from primary database({pri_db_dict['host']})...")
            sql = "select setting from pg_settings where name='unix_socket_directories'"
            err_code, rows = probe_db.run_sql(pri_db_dict['host'],
                                            db_port,
                                            'template1',
                                            db_user,
                                            db_pass,
                                            sql,
                                            ())
            if err_code != 0:
                err_msg = f'{msg_prefix}: failed to get unix_socket_directories: {rows}'
                return err_code, err_msg

            unix_socket_directories = rows[0]['setting']
            cells = unix_socket_directories.split(',')
            unix_socket_dir = cells[0].strip()
            log_info(task_id, f"{msg_prefix}: get unix_socket_directories from primary database({pri_db_dict['host']}) is {unix_socket_directories}")
            pdict['unix_socket_dir'] = unix_socket_dir

            if cluster_type == 11:
                if polar_type == "reader":
                    err_code, err_msg = polar_helpers.repair_build_polar_reader(task_id, failback_host, db_id, pdict)
                elif polar_type == "standby":
                    err_code, err_msg = polar_helpers.repair_build_polar_standby(task_id, failback_host, db_id, pdict)
            else:
                err_code, err_msg = long_term_task.build_pg_standby(task_id, failback_host, db_id, pdict)
            if err_code != 0:
                err_msg = f'Build standby database failed: {err_msg}'
                return err_code, err_msg
            # 修复成功的话重置failback的次数
            update_dict = json.dumps({"failback_count": 0})
            sql = "UPDATE clup_db SET db_detail = db_detail || (%s::jsonb) WHERE db_id = %s"
            dbapi.execute(sql, (update_dict, db_id))

            return 0, err_msg
        finally:
            rpc.close()
        # failback_end(pdict)
    except Exception:
        err_code = -1
        err_msg = f'{msg_prefix}: Unexpected error occurred: {traceback.format_exc()}'
    finally:
        if err_code != 0:
            ret_code = -1
            db_state = database_state.FAULT
            n_state = node_state.FAULT
            # log_error(task_id, f'{msg_prefix}: {err_msg}')
        else:
            ret_code = 1
            err_msg = 'Success'
            db_state = database_state.RUNNING
            n_state = node_state.NORMAL
            try:
                db_user = clu_db_list[0]['db_user']
                db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])
                failback_trigger_func(task_id, msg_prefix, cluster_dict, before_cluster_dict, pri_host, db_port, failback_host, db_user, db_pass)
            except Exception as e:
                logging.error(f"call failback trigger function failed: {repr(e)}")

            # 把节点加回
            str_cstlb_list = cluster_dict['cstlb_list']
            if not str_cstlb_list:
                cstlb_list = []
            else:
                cstlb_list = str_cstlb_list.split(',')
                cstlb_list = [k.strip() for k in cstlb_list]
            for lb_host in cstlb_list:
                backend_addr = f"{failback_db_dict['host']}:{db_port}"
                err_code, msg = lb_mgr.add_backend(lb_host, backend_addr)
                if err_code != 0:
                    log_error(task_id, f"Can not add host({backend_addr}) to cstlb({lb_host}): {msg}")

        general_task_mgr.complete_task(task_id, ret_code, err_msg)
        dao.set_cluster_state(cluster_id, restore_cluster_state)
        dao.update_db_state(db_id, db_state)
        dao.set_node_state(db_id, n_state)
        return -1, err_msg


def test_sr_can_switch(cluster_id, db_id: int, primary_db, keep_cascaded=False):
    """
    测试streaming replication集群是否能切换主库
    :cluster_id: 高可用集群的id
    :param db_id: 要成为新主库的id
    :param primary_db: 原先主库的id
    :return:
    """
    pre_msg = f"Can be switch to primary(cluster={cluster_id}, dbid={db_id})"
    if keep_cascaded:
        clu_db_list = dao.get_cluster_db_list(cluster_id)
    else:
        clu_db_list = dao.get_db_and_lower_db(primary_db)
    cluster_dict = dao.get_cluster(cluster_id)
    if cluster_dict is None or (not clu_db_list):
        logging.info(f"test_sr_can_switch when cluster({cluster_id}) has been deleted")
        return 1, f"cluster({cluster_id}) has been deleted"
    state = cluster_dict['state']
    if state != cluster_state.NORMAL and state != cluster_state.OFFLINE:
        err_msg = "The cluster is not in Online or Offline status, so it cannot be switched!"
        logging.info(f"{pre_msg}:{err_msg}")
        return -1, err_msg

    # 检查现在目标数据库是否存在
    # new_pri_db = None
    for db_dict in clu_db_list:
        if db_dict['db_id'] == db_id:
            new_pri_db = db_dict
            break
    else:
        db_id_list = [db['db_id'] for db in clu_db_list]
        db_id_list.remove(primary_db)
        err_msg = f"To switch the standby to the primary, the standby({db_id}) must be a direct subordinate of the original primary({primary_db}). "
        err_msg += "If it is not, the cascade relationship should be adjusted first before proceeding with the switch."
        logging.info(f"{pre_msg}:{err_msg}")
        return 1, err_msg

    # 找到目前的主数据库
    # old_pri_db = {}
    for db_dict in clu_db_list:
        if db_dict['is_primary'] and db_dict['state'] == node_state.NORMAL:
            old_pri_db = db_dict
            break
    else:
        err_msg = "can not find current primary database or current primary database is not ok!"
        logging.info(f"{pre_msg}:{err_msg}")
        return -1, err_msg

    if old_pri_db['db_id'] == db_id:
        err_msg = f'database(db_id={db_id}) already is primary, no need switch!'
        logging.info(f"{pre_msg}:{err_msg}")
        return 1, err_msg

    db_port = cluster_dict['port']

    # 从第一个库中获得repl_user和repl_pass, cluster_data中的db_repl_user和db_repl_user废弃了
    # db_repl_user = cluster_dict['db_repl_user']
    # db_repl_pass = db_encrypt.from_db_text(cluster_dict['db_repl_pass'])
    repl_user = clu_db_list[0]['repl_user']
    repl_pass = db_encrypt.from_db_text(clu_db_list[0]['repl_pass'])

    # 获得要成为新主库的数据库当期的wal文件在原主库上是否存在
    err_code, new_pri_last_wal_file = probe_db.get_last_wal_file(new_pri_db['host'], db_port,
                                                     repl_user, repl_pass)
    if err_code != 0:
        err_msg = f"get new primary({new_pri_db['host']}) last wal file failed: {new_pri_last_wal_file}"
        logging.info(f"{pre_msg}:{err_msg}")
        return 1, err_msg

    err_code, str_ver = pg_db_lib.pgdata_version(old_pri_db['host'], old_pri_db['pgdata'])
    if err_code != 0:
        logging.error(f"In can_be_switch_primary, get pg version error: {str_ver}")
        return -1, str_ver
    pg_ver = float(str_ver)
    if pg_ver >= 10:
        wal_dir = 'pg_wal'
    else:
        wal_dir = 'pg_xlog'

    xlog_file_full_name = os.path.join(old_pri_db['pgdata'], wal_dir, new_pri_last_wal_file)
    err_code, exists = rpc_utils.os_path_exists(old_pri_db['host'], xlog_file_full_name)
    if err_code != 0:
        return -1, exists

    if not exists:
        err_msg = f"The new node({new_pri_db['host']}) that wants to become the primary does not have the necessary WAL logs({xlog_file_full_name}) from the current primary({old_pri_db['host']})."
        err_msg += " (the WAL logs are too outdated), so the switch cannot be made."
        logging.info(f"{pre_msg}:{err_msg}")
        return 1, err_msg
    return 0, ''


def sr_switch(task_id, cluster_id, db_id, primary_db, keep_cascaded=False):
    """
    streaming replication集群切换主库
    :param primary_db:
    :param task_id:
    :param cluster_id:
    :param db_id:
    :return:
    """
    cluster = dao.get_cluster_name(cluster_id)
    cluster = cluster.get('cluster_name', cluster_id)
    pre_msg = f"Switch to primary(cluster={cluster}, db_id={db_id})"
    log_info(task_id, f'{pre_msg}: begin ...')
    try:
        res = dao.test_and_set_cluster_state(cluster_id, [cluster_state.NORMAL, cluster_state.OFFLINE], cluster_state.REPAIRING)
        if res is None:
            err_msg = f"{pre_msg}: The cluster is not in Online or Offline status, so it cannot be switched!!"
            log_info(task_id, err_msg)
            general_task_mgr.complete_task(task_id, -1, err_msg)
            return 1, err_msg
        clu_state = res
    except Exception as e:
        log_error(task_id, f'{pre_msg}: END.')
        err_msg = f"{pre_msg}: Unable to set the status of the cluster: {str(e)}"
        log_error(task_id, err_msg)
        general_task_mgr.complete_task(task_id, -1, err_msg)
        return -1, err_msg

    try:
        # 先做检查，看能否操作
        if keep_cascaded:
            clu_db_list = dao.get_cluster_db_list(cluster_id)
        else:
            clu_db_list = dao.get_db_and_lower_db(primary_db)
        cluster_dict = dao.get_cluster(cluster_id)
        if cluster_dict is None or (not clu_db_list):
            msg = f"cluster({cluster_id}) has been deleted"
            logging.info(msg)
            general_task_mgr.complete_task(task_id, -1, f"cluster({cluster_id}) has been deleted")
            return
        err_code, db_room = pg_helpers.get_db_room(db_id)
        if err_code != 0:
            err_msg = f"Failed to obtain database(db_id={db_id}) room information: {db_room}"
            general_task_mgr.complete_task(task_id, -1, err_msg)
            return

        # 更新cluster_vip
        before_cluster_dict = pg_helpers.get_new_cluster_data(cluster_id, db_room['room_id'])
        # before_cluster_dict = dao.get_cluster(cluster_id)
        before_cluster_dict['db_list'] = copy.copy(clu_db_list)
        cluster_dict = copy.copy(before_cluster_dict)
        cluster_dict['db_list'] = clu_db_list
        db_port = cluster_dict['port']

        # 检查现在目标数据库是否存在
        for db_dict in clu_db_list:
            if db_dict['db_id'] == db_id:
                new_pri_db = db_dict
                break
        else:
            err_msg = f'{pre_msg}: can not find this new primary database(dbid={db_id})!!!'
            log_info(task_id, err_msg)
            log_info(task_id, f'{pre_msg}: completed.')
            general_task_mgr.complete_task(task_id, -1, err_msg)
            return 1, err_msg

        db_pass = db_encrypt.from_db_text(new_pri_db['db_pass'])
        primary_conninfo = f"application_name=repl_app_name user={new_pri_db['db_user']} password={db_pass}" \
            f" host={new_pri_db['host']} port={new_pri_db['port']} sslmode=disable sslcompression=1"
        old_conninfo_dict = {}  # 记录旧的配置信息，下面发生错误回滚需要用到
        # 12以上版本需要考虑postgresql.auto.conf文件是否存在primary_conninfo配置
        for db_dict in clu_db_list:
            if 'version' not in db_dict:
                err_code, err_msg = pg_helpers.renew_pg_bin_info(db_id)
                if err_code != 0:
                    return err_code, err_msg
                renew_dict = err_msg
                version = str(renew_dict['version'])
            else:
                version = str(db_dict['version'])
            if err_code != 0 or int(version.split('.')[0]) < 12:
                break
            if db_dict['db_id'] == db_id:
                continue
            err_code, item_dict = pg_helpers.check_auto_conf(db_dict['host'], db_dict['pgdata'], ['primary_conninfo'])
            if err_code != 0 or not item_dict.get('primary_conninfo'):
                # 检查数据库的postgresql.auto.conf文件中有没有配primary_conninfo
                continue
            old_conninfo_dict[db_dict['db_id']] = item_dict.get('primary_conninfo')
            conn_info = primary_conninfo.replace('repl_app_name', db_dict['repl_app_name'])
            pg_helpers.alter_system_conf(db_dict['db_id'], "primary_conninfo", conn_info)

        pre_msg = f"Switch to primary(cluster={cluster_id}, host={new_pri_db['host']})"
        # 找到目前的主数据库
        log_info(task_id, f'{pre_msg}: find current primary database...')
        # old_pri_db = {}
        for db_dict in clu_db_list:
            if db_dict['is_primary'] and db_dict['state'] == node_state.NORMAL:
                old_pri_db = db_dict
                break
        else:
            err_msg = "can not find current primary database..."
            log_info(task_id, f'{pre_msg}: {err_msg}')
            general_task_mgr.complete_task(task_id, -1, err_msg)
            return 1, "Can not find current primary database!!!"

        if old_pri_db['db_id'] == db_id:
            err_msg = f'{pre_msg}: already is primary, no need switch!'
            log_info(task_id, err_msg)
            general_task_mgr.complete_task(task_id, -1, err_msg)
            return 1, err_msg
    except Exception as e:
        error_msg = f'{pre_msg}: unexpected error occurred: {str(e)}'
        log_info(task_id, f'{pre_msg}: unexpected error occurred: {traceback.format_exc()}')
        try:
            dao.set_cluster_state(cluster_id, clu_state)
        except Exception:
            log_error(task_id, f'{pre_msg}: restore cluster state to normal failed: {traceback.format_exc()}')

        general_task_mgr.complete_task(task_id, -1, error_msg)
        return -1, error_msg

    try:
        vip = cluster_dict['vip']
        if not cluster_dict.get('save_old_room_vip') or new_pri_db['room_id'] == old_pri_db['room_id']:
            log_info(task_id, f"{pre_msg}: drop vip from current primary database({ old_pri_db['host']})")
            try:
                rpc_utils.check_and_del_vip(old_pri_db['host'], vip)
            except Exception:
                log_info(task_id, f"{pre_msg}: Unexpected error occurred during delete vip({vip}) from host({old_pri_db['host']}): {traceback.format_exc()}")

        log_info(task_id, f"{pre_msg}: begin stopping current primary database({old_pri_db['host']})...")
        old_pri_db.setdefault('wait_time', 30)  # 停止数据库等待的时间
        err_code, err_msg = pg_db_lib.stop(old_pri_db['host'], old_pri_db['pgdata'])
        if err_code != 0:
            msg = f"{pre_msg}: current primary database({old_pri_db['host']}) can not be stopped. ({err_msg})"
            log_error(task_id, f"{pre_msg}: current primary database({old_pri_db['host']}) can not be stopped; ({err_msg})")
            general_task_mgr.complete_task(task_id, -1, msg)
            return -1, msg

        log_info(task_id, f"{pre_msg}: current primary database({old_pri_db['host']}) stopped.")

        # 从第一个库中获得repl_user和repl_pass, cluster_data中的db_repl_user和db_repl_user废弃了
        # db_repl_user = cluster_dict['db_repl_user']
        # db_repl_pass = db_encrypt.from_db_text(cluster_dict['db_repl_pass'])
        repl_user = clu_db_list[0]['repl_user']
        repl_pass = db_encrypt.from_db_text(clu_db_list[0]['repl_pass'])

        # 因为将成为新主库的节点的WAL日志有可能是落后旧主库，如果落后太多，所需要的WAL在旧主库上已经被删除掉了，则不能切换成主库
        err_msg = ''
        try:
            err_code, new_pri_last_wal_file = probe_db.get_last_wal_file(new_pri_db['host'], db_port,
                                                     repl_user, repl_pass)

            if err_code != 0:
                err_msg = f"{pre_msg}: get new pirmary({new_pri_db['host']}) last wal file failed: {new_pri_last_wal_file}"
                log_error(task_id, err_msg)
                raise UserWarning

            # err_code, str_ver = rpc_utils.pg_version(old_pri_db['host'], old_pri_db['pgdata'])
            err_code, str_ver = pg_db_lib.pgdata_version(old_pri_db['host'], old_pri_db['pgdata'])
            if err_code != 0:
                logging.error(f"In switch_primary, get pg version error: {str_ver}")
                raise UserWarning
            pg_ver = float(str_ver)
            if pg_ver >= 10:
                wal_dir = 'pg_wal'
            else:
                wal_dir = 'pg_xlog'
            xlog_file_full_name = os.path.join(old_pri_db['pgdata'], wal_dir, new_pri_last_wal_file)
            err_code, is_exists = rpc_utils.os_path_exists(old_pri_db['host'], xlog_file_full_name)
            if err_code != 0:
                err_msg = f"{pre_msg}: call rpc os_path_exists({old_pri_db['host']}, {xlog_file_full_name}) failed when find last wal file in " \
                          f"original primary({old_pri_db['host']})," \
                          f"so it can not became primary: {is_exists}"
                log_error(task_id, err_msg)
                raise UserWarning
            if not is_exists:
                err_msg = f"{pre_msg}: new pirmary({new_pri_db['host']}) last wal file({xlog_file_full_name}) not exists in original primary({old_pri_db['host']}), " \
                          "so it can not became primary!"
                log_error(task_id, err_msg)
                raise UserWarning
        except Exception as e:
            # 出现错误，开始回滚前面的操作
            log_info(task_id, f"{pre_msg}: switch failed, start the original primary database({old_pri_db['host']}).")
            pg_db_lib.start(old_pri_db['host'], old_pri_db['pgdata'])
            log_info(task_id, f"{pre_msg}: the original primary database({old_pri_db['host']}) is started.")
            try:
                log_info(task_id, f"{pre_msg}: add vip({vip}) to the original primary database({old_pri_db['host']})...")
                rpc_utils.check_and_add_vip(old_pri_db['host'], vip)
                log_info(task_id, f"{pre_msg}: add vip({vip}) to the original primary database({old_pri_db['host']}) completed.")
            except Exception:
                log_error(task_id,
                          f"{pre_msg} : an expected error occurred during add vip ({vip})"
                          f" to original primary database({old_pri_db['host']}): {traceback.format_exc()}")

            if old_conninfo_dict:
                # 如果前面改了postgresql.auto.conf文件，需要回滚
                for db in clu_db_list:
                    if db['db_id'] not in old_conninfo_dict.keys():
                        continue
                    pg_helpers.alter_system_conf(db_dict['db_id'], "primary_conninfo", old_conninfo_dict[db['db_id']])
                    # 重启数据库
                    pg_db_lib.restart(db['host'], db['pgdata'])

            if isinstance(e, UserWarning):
                log_info(task_id, f"{new_pri_db['host']}: failed!!!")
                general_task_mgr.complete_task(task_id, -1, err_msg)
                return 1, err_msg
            else:
                error_msg = f"{pre_msg}: with unexpected error,{str(e)},switch failed!!!"
                log_error(task_id, error_msg)
                general_task_mgr.complete_task(task_id, -1, error_msg)
                return 1, repr(e)

        # 需要把新主库从负载均衡器cstlb中剔除掉(因为新主库原先是备库，而备库的IP是在cstlb中的)
        log_info(task_id, f"{pre_msg}: begin remove new primary database({new_pri_db['host']}) from cstlb ...")
        str_cstlb_list = cluster_dict.get('cstlb_list', '')
        if not str_cstlb_list:
            cstlb_list = []
        else:
            cstlb_list = str_cstlb_list.split(',')
            cstlb_list = [k.strip() for k in cstlb_list]
        for lb_host in cstlb_list:
            backend_addr = f"{new_pri_db['host']}:{new_pri_db['port']}"
            err_code, err_msg = lb_mgr.delete_backend(lb_host, backend_addr)
            if err_code != 0:
                log_error(task_id, f"Can not remove host({backend_addr}) from cstlb({lb_host}): {err_msg}")
        log_info(task_id, f"{pre_msg}: remove new primary database({new_pri_db['host']}) from cstlb finished.")

        # 先把新主库关掉，然后把旧主库上比较新的xlog文件都拷贝过来：
        log_info(task_id, f'{pre_msg}: stop new pirmary database then sync wal from old primary ...')
        err_code, err_msg = rpc_utils.pg_cp_delay_wal_from_pri(
            new_pri_db['host'],
            old_pri_db['host'],
            old_pri_db['pgdata'],
            new_pri_db['pgdata']
        )

        if err_code != 0:
            err_msg = f"stop new pirmary database then sync wal from old primary failed: {err_msg}"
            general_task_mgr.complete_task(task_id, -1, err_msg)
            return -1, err_msg

        log_info(task_id, f'{pre_msg}: stop new pirmary database then sync wal from old primary finished')

        # 再重新启动新主库(目前还处于只读standby模式)
        log_info(task_id, f'{pre_msg}: restart new pirmary database and wait it is ready ...')
        err_code, err_msg = pg_db_lib.start(new_pri_db['host'], new_pri_db['pgdata'])
        if err_code != 0:
            return -1, err_msg

        # 等待新主库滚日志到最新
        while True:
            err_code, err_msg = pg_db_lib.is_ready(new_pri_db['host'], new_pri_db['pgdata'], new_pri_db['port'])
            if err_code == 0:
                break
            elif err_code == 1:
                log_info(task_id, f"{pre_msg}: {err_msg}")
                time.sleep(1)
                continue
            else:
                log_error(task_id, f"{pre_msg}: {err_msg}")
                general_task_mgr.complete_task(task_id, -1, "Switch failed.")
                return -1, err_msg
        log_info(task_id, f'{pre_msg}: new pirmary database started and it is ready.')

        # 因为旧主库切换成了Standby库，需要把旧主库添加到负载均衡器cstlb中：
        log_info(task_id, f"{pre_msg}: begin add old primary database({old_pri_db['host']}) to cstlb...")
        for lb_host in cstlb_list:
            backend_addr = f"{old_pri_db['host']}:{old_pri_db['port']}"
            err_code, err_msg = lb_mgr.add_backend(lb_host, backend_addr)
            if err_code != 0:
                log_error(task_id, f"Can not add host({backend_addr}) to cstlb({backend_addr}): {err_msg}")
        log_info(task_id, f"{pre_msg}: add old primary database({old_pri_db['host']}) to cstlb finished.")

        # 把选中的这个备库激活成主库
        log_info(task_id, f'{pre_msg}: promote new primary ...')
        err_code, err_msg = pg_db_lib.promote(new_pri_db['host'], new_pri_db['pgdata'])
        if err_code != 0:
            # FIXME: promote不成功，只发一个告警，继续切换，但新主库可以处于一个standby的状态
            err_msg = f"{pre_msg}: promote failed: {str(err_msg)} "
            log_error(task_id, err_msg)

        log_info(task_id, f'{pre_msg}: promote new primary completed.')

        err_code, rooms = pg_helpers.get_db_room(db_id)
        vip = rooms.get('vip', vip) if err_code == 0 else vip
        log_info(task_id, f"{pre_msg}: add primary vip({vip}) to new primary({new_pri_db['host']}) ...")
        try:
            rpc_utils.check_and_add_vip(new_pri_db['host'], vip)
            log_info(task_id, f"{pre_msg}: add primary vip({vip}) to new primary({new_pri_db['host']}) completed.")
        except Exception:
            log_error(task_id, f"{pre_msg} : unexpected error occurred during add vip ({vip}) "
                               f"to new primary({new_pri_db['host']}): {traceback.format_exc()}")

        old_pri_db['is_primary'] = 0
        new_pri_db['is_primary'] = 1
        dao.set_cluster_db_attr(cluster_id, old_pri_db['db_id'], 'is_primary', 0)
        dao.set_cluster_db_attr(cluster_id, new_pri_db['db_id'], 'is_primary', 1)

        # 需要把其它备库指向新的主库
        log_info(task_id, f"{pre_msg}: change all standby database upper level primary database to new primary...")
        if keep_cascaded:
            pg_helpers.change_up_db_by_db_id(old_pri_db['db_id'], new_pri_db['db_id'])
            # 更新数据库存的上级主库
            dao.update_up_db_id('null', new_pri_db['db_id'], 1)
            # pg_helpers.change_up_db_by_db_id(new_pri_db['db_id'], None)

        else:
            for p in clu_db_list:
                dao.update_up_db_id(new_pri_db['db_id'], p['db_id'], 0)
                if p['db_id'] != db_id and (not p['is_primary']) and p['state'] == node_state.NORMAL:
                    if not p['instance_type']:
                        p['instance_type'] = 'physical'

                    err_code, err_msg = pg_db_lib.pg_change_standby_updb(p['host'], p['pgdata'], repl_user, repl_pass,
                                                p['repl_app_name'], new_pri_db['host'], new_pri_db['port'])
                    if err_code != 0:
                        log_info(task_id, f'{pre_msg}: change host-{p["host"]} updb faild: {err_msg}')

            # 更新数据库存的上级主库
            dao.update_up_db_id('null', new_pri_db['db_id'], 1)

        log_info(task_id, f"{pre_msg}: "
                          "change all standby database upper level primary database to new primary completed.")

        # 如果配置了切换时调用的数据库函数，则调用此函数
        trigger_db_name = cluster_dict['trigger_db_name']
        trigger_db_func = cluster_dict['trigger_db_func']
        # 如果配置了切换时调用的数据库函数，则调用此函数
        if trigger_db_name and trigger_db_func:
            log_info(task_id,
                     f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary ...")

            db_user = clu_db_list[0]['db_user']
            db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])
            sql = "select {0}(%s, %s, %s, %s, %s);".format(trigger_db_func)
            err_code, err_msg = probe_db.exec_sql(
                new_pri_db['host'], new_pri_db['port'], trigger_db_name, db_user, db_pass,
                sql,
                (2, pre_msg, old_pri_db['host'], json.dumps(before_cluster_dict), json.dumps(cluster_dict)))

            if err_code != 0:
                log_error(task_id, f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary error: {err_msg}")
        # 更新机房信息
        log_info(task_id, f"Update cluster(cluster_id: {cluster_id}) computer room information")
        err_code, err_msg = pg_helpers.update_cluster_room_info(cluster_id)
        if err_code != 0:
            msg = f"Update cluster(cluster_id: {cluster_id}) computer room information FAiled:  {err_msg}"
            log_error(task_id, msg)
            general_task_mgr.complete_task(task_id, -1, msg)
            return -1, msg

        ret_msg = f'{pre_msg}: success.'
        log_info(task_id, ret_msg)
        general_task_mgr.complete_task(task_id, 1, 'Success')
        return 0, "Switch success"
    except Exception as e:
        error_msg = f"{pre_msg}: with unexpected error,{traceback.format_exc()}, switch failed!!!"
        log_info(task_id, error_msg)
        general_task_mgr.complete_task(task_id, -1, error_msg)
        return -1, error_msg
    finally:
        dao.set_cluster_state(cluster_id, clu_state)


def test_polar_can_switch(cluster_id, db_id: int, primary_db):
    """
    测试streaming replication集群是否能切换主库
    :cluster_id: 高可用集群的id
    :param db_id: 要成为新主库的id
    :param primary_db: 原先主库的id
    :return:
    """
    pre_msg = f"Can be switch to primary(cluster={cluster_id}, dbid={db_id})"
    clu_db_list = dao.get_db_and_lower_db(primary_db)
    cluster_dict = dao.get_cluster(cluster_id)
    if cluster_dict is None or (not clu_db_list):
        logging.info(f"test_sr_can_switch when cluster({cluster_id}) has been deleted")
        return 1, f"cluster({cluster_id}) has been deleted"

    state = cluster_dict['state']
    if state != cluster_state.NORMAL and state != cluster_state.OFFLINE:
        err_msg = "The cluster is not in Online or Offline status, so it cannot be switched!!"
        logging.info(f"{pre_msg}:{err_msg}")
        return -1, err_msg

    # 检查现在目标数据库是否存在
    # new_pri_db = None
    for db_dict in clu_db_list:
        if db_dict['db_id'] == db_id:
            # new_pri_db = db_dict
            break
    else:
        db_id_list = [db['db_id'] for db in clu_db_list]
        db_id_list.remove(primary_db)
        err_msg = f"To switch the standby to the primary,the standby({db_id}) must be a direct subordinate of the original primary({primary_db})."
        err_msg += "If it is not, the cascade relationship should be adjusted first before proceeding with the switch!"
        logging.info(f"{pre_msg}: {err_msg}")
        return 1, err_msg

    # 找到目前的主数据库
    # old_pri_db = {}
    for db_dict in clu_db_list:
        if db_dict['is_primary'] and db_dict['state'] == node_state.NORMAL:
            old_pri_db = db_dict
            break
    else:
        err_msg = "can not find current primary database or current primary database is not ok!"
        logging.info(f"{pre_msg}:{err_msg}")
        return -1, err_msg

    if old_pri_db['db_id'] == db_id:
        err_msg = f'database(db_id={db_id}) already is primary, no need switch!'
        logging.info(f"{pre_msg}:{err_msg}")
        return 1, err_msg

    return 0, 'Can switch'


def polar_switch(task_id, cluster_id, new_pri_db_id, old_pri_db_id):
    """
    streaming replication polardb集群切换主库
    :param primary_db:
    :param task_id:
    :param cluster_id:
    :param db_id:
    :return:
    """
    cluster_info = dao.get_cluster_name(cluster_id)
    cluster = cluster_info.get('cluster_name', cluster_id)
    pre_msg = f"Switch to primary(cluster={cluster}, db_id={new_pri_db_id})"

    log_info(task_id, f'{pre_msg}: begin ...')
    # 设置集群状态
    try:
        # 获得集群锁之后，把集群状态改成REPAIRING，防止数据库健康检查程序的误操作
        res = dao.test_and_set_cluster_state(cluster_id, [cluster_state.NORMAL, cluster_state.OFFLINE], cluster_state.REPAIRING)
        if res is None:
            err_msg = f"{pre_msg}: The status of the cluster is not Online or Offline, please try again later!"
            log_info(task_id, err_msg)
            return 1, err_msg
        clu_state = res
    except Exception as e:
        log_error(task_id, f'{pre_msg}: END.')
        err_msg = f"{pre_msg}: Unable to set the cluster status: {str(e)}"
        log_error(task_id, err_msg)
        return -1, err_msg

    # 获取数据库所有备库信息
    try:
        clu_db_list = dao.get_db_and_lower_db(old_pri_db_id)
        cluster_dict = dao.get_cluster(cluster_id)
        if cluster_dict is None or (not clu_db_list):
            msg = f"cluster({cluster_id}) has been deleted"
            logging.info(msg)
            return -1, msg
        err_code, db_room = pg_helpers.get_db_room(new_pri_db_id)
        if err_code != 0:
            err_msg = f"Failed to obtain database(db_id={new_pri_db_id}) room information: {db_room}"
            return -1, err_msg

        before_cluster_dict = pg_helpers.get_new_cluster_data(cluster_id, db_room['room_id'])
        # before_cluster_dict = dao.get_cluster(cluster_id)
        before_cluster_dict['db_list'] = copy.copy(clu_db_list)
        cluster_dict = copy.copy(before_cluster_dict)
        cluster_dict['db_list'] = clu_db_list

        # 检查现在目标数据库是否存在
        for db_dict in clu_db_list:
            if db_dict['db_id'] == new_pri_db_id:
                new_pri_db_dict = db_dict
                break
        else:
            err_msg = f'{pre_msg}: can not find this new primary database(dbid={new_pri_db_id})!!!'
            log_info(task_id, err_msg)
            return -1, err_msg

        pre_msg = f"Switch to primary(cluster={cluster_id}, host={new_pri_db_dict['host']})"
        # 找到目前的主数据库
        log_info(task_id, f'{pre_msg}: find current primary database...')
        for db_dict in clu_db_list:
            if db_dict['is_primary'] and db_dict['state'] == node_state.NORMAL:
                old_pri_db_dict = db_dict
                break
        else:
            err_msg = "can not find current primary database..."
            log_info(task_id, f'{pre_msg}: {err_msg}')
            return -1, "Can not find current primary database!!!"

        if old_pri_db_dict['db_id'] == new_pri_db_id:
            err_msg = f'{pre_msg}: already is primary, no need switch!'
            log_info(task_id, err_msg)
            return -1, err_msg
    except Exception:
        error_msg = f"{pre_msg}: with unexpected error,{traceback.format_exc()}."
        log_info(task_id, error_msg)
        try:
            dao.set_cluster_state(cluster_id, clu_state)
        except Exception:
            log_error(task_id, f'{pre_msg}: restore cluster state to normal failed, {traceback.format_exc()}.')
        return -1, error_msg

    try:
        vip = cluster_dict['vip']
        if not cluster_dict.get('save_old_room_vip') or new_pri_db_dict['room_id'] == old_pri_db_dict['room_id']:
            log_info(task_id, f"{pre_msg}: drop vip from current primary database({ old_pri_db_dict['host']})")
            try:
                rpc_utils.check_and_del_vip(old_pri_db_dict['host'], vip)
            except Exception:
                log_info(task_id, f"{pre_msg}: delete vip({vip}) from host({old_pri_db_dict['host']}) with unexpected error,{traceback.format_exc()}.")

        # 获取新主库的房间信息
        err_code, rooms = pg_helpers.get_db_room(new_pri_db_id)
        vip = rooms.get('vip', vip) if err_code == 0 else vip
        log_info(task_id, f"{pre_msg}: add primary vip({vip}) to new primary({new_pri_db_dict['host']}) ...")
        try:
            rpc_utils.check_and_add_vip(new_pri_db_dict['host'], vip)
            log_info(task_id, f"{pre_msg}: add primary vip({vip}) to new primary({new_pri_db_dict['host']}) completed.")
        except Exception:
            log_error(task_id, f"{pre_msg}: add vip ({vip}) with unexpected error "
                     f"to new primary({new_pri_db_dict['host']}): {traceback.format_exc()}")

        old_pri_db_dict['is_primary'] = 0
        new_pri_db_dict['is_primary'] = 1
        dao.set_cluster_db_attr(cluster_id, old_pri_db_dict['db_id'], 'is_primary', 0)
        dao.set_cluster_db_attr(cluster_id, new_pri_db_dict['db_id'], 'is_primary', 1)

        # 需要把其它备库指向新的主库
        log_info(task_id, f"{pre_msg}: Redirect all the standby databases to the new primary...")
        for db_dict in clu_db_list:
            if db_dict["db_id"] != new_pri_db_id and (not db_dict['is_primary']) and db_dict['state'] == node_state.NORMAL:
                # 停掉数据库,应跳过数据库状态不是运行中
                if db_dict["db_state"] == 0:
                    log_info(task_id, f"{pre_msg}: begin stopping  database({db_dict['host']})...")
                    db_dict['wait_time'] = 5
                    err_code, err_msg = pg_db_lib.stop(db_dict['host'], db_dict['pgdata'], 5)
                    if err_code != 0:
                        log_info(task_id, f"{pre_msg}: can not stop database({db_dict['host']}), try using model immediate...")
                        stop_err_code, stop_err_msg = polar_lib.stop_immediate(db_dict['host'], db_dict['pgdata'], 5)
                        if stop_err_code != 0:
                            return -1, stop_err_msg
                    dao.update_db_state(db_dict['db_id'], database_state.STOP)
                    log_info(task_id, f"{pre_msg}: current database({db_dict['host']}) stopped.")

                # 如果备库的agent状态异常应跳过后续步骤
                if db_dict["db_state"] == -1:
                    # 更新数据库的集群状态，置为Fault，后续只能通过加回集群修复
                    dao.update_ha_state(db_dict["db_id"], node_state.FAULT)
                    continue
                # 如果是旧主库，则需要先获取新主库的配置文件recovery.conf
                log_info(task_id, f"{pre_msg}: Configure the recovery.conf file, current database({db_dict['host']}).")
                if db_dict["db_id"] == old_pri_db_id:
                    err_code, err_msg = polar_lib.update_recovery(db_dict["db_id"], new_pri_db_dict["host"], new_pri_db_dict["port"], new_pri_db_dict["pgdata"])
                else:
                    err_code, err_msg = polar_lib.update_recovery(db_dict["db_id"], new_pri_db_dict["host"], new_pri_db_dict["port"])
                if err_code != 0:
                    return -1, err_msg

                # 将流复制密码写入到.pgpass中
                os_user = db_dict['os_user']
                repl_user = db_dict['repl_user']
                repl_pass = db_encrypt.from_db_text(db_dict["repl_pass"])
                up_db_port = new_pri_db_dict["port"]
                up_db_repl_ip = new_pri_db_dict["repl_ip"]
                err_code, err_msg = rpc_utils.get_rpc_connect(db_dict['host'])
                if err_code == 0:
                    rpc = err_msg
                    err_code, err_msg = pg_db_lib.dot_pgpass_add_item(
                        rpc, os_user, up_db_repl_ip, up_db_port, 'replication', repl_user, repl_pass,
                    )
                    rpc.close()
            # 在clup_db中更新数据库的上级库
            dao.update_up_db_id(new_pri_db_dict['db_id'], db_dict['db_id'], 0)
        # 更新新主库的上级主库为null
        dao.update_up_db_id('null', new_pri_db_dict['db_id'], 1)
        log_info(task_id, f"{pre_msg}: All the standby databases have been successfully pointed to the new primary database.")

        # 更新新主库数据库的polar_type
        log_info(task_id, f"{pre_msg}: Update the polar_type of the database!")
        err_code, err_msg = polar_lib.update_polar_type(new_pri_db_dict["db_id"], "master")
        if err_code != 0:
            log_info(task_id, f"{pre_msg}: {err_msg},Please manually check and modify later!")
            return -1, err_msg

        # 更新旧主库数据库的polar_type
        err_code, err_msg = polar_lib.update_polar_type(old_pri_db_dict["db_id"], "reader")
        if err_code != 0:
            log_info(task_id, f"{pre_msg}: {err_msg},Please manually check and modify later!")

        # 停掉新主库
        log_info(task_id, f"{pre_msg}: Stop the new primary database({new_pri_db_dict['host']})...")
        new_pri_db_dict['wait_time'] = 5
        err_code, err_msg = pg_db_lib.stop(new_pri_db_dict['host'], new_pri_db_dict['pgdata'])
        if err_code != 0:
            return -1, err_msg
        dao.update_db_state(new_pri_db_dict['db_id'], database_state.STOP)
        log_info(task_id, f"{pre_msg}: current new primary database({new_pri_db_dict['host']}) stopped.")

        # 配置新主库的信息
        log_info(task_id, f'{pre_msg}: rename the new primary database recovery.conf to recovery.done...')
        err_code, err_msg = polar_lib.mv_recovery(new_pri_db_id)
        if err_code != 0:
            return -1, err_msg
        log_info(task_id, f'{pre_msg}: rename option is finished.')

        # 配置完成，开始启动数据库
        for db_dict in clu_db_list:
            # 如果之前的数据库不是启动的或者对应的集群状态不是NORMAL则跳过
            if db_dict["state"] != node_state.NORMAL or db_dict["db_state"] != 0:
                continue
            log_info(task_id, f"{pre_msg}: begin starting database({db_dict['host']})...")
            polar_type = polar_lib.get_polar_type(db_dict['db_id'])
            polar_type_list = ['master', 'reader']
            if polar_type in polar_type_list:
                err_code, err_msg = polar_lib.start_pfs(db_dict['host'], db_dict['db_id'])
                if err_code != 0 and err_code != 1:
                    return -1, err_msg
            err_code, err_msg = pg_db_lib.start(db_dict['host'], db_dict['pgdata'])
            if err_code != 0:
                return -1, err_msg
            dao.update_db_state(db_dict['db_id'], database_state.RUNNING)
            log_info(task_id, f"{pre_msg}: current database({db_dict['host']}) is running.")

        trigger_db_name = cluster_dict['trigger_db_name']
        trigger_db_func = cluster_dict['trigger_db_func']
        # 如果配置了切换时调用的数据库函数，则调用此函数
        if trigger_db_name and trigger_db_func:
            log_info(task_id,
                     f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary ...")

            db_user = clu_db_list[0]['db_user']
            db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])
            sql = "select {0}(%s, %s, %s, %s, %s);".format(trigger_db_func)
            err_code, err_msg = probe_db.exec_sql(
                new_pri_db_dict['host'], new_pri_db_dict['port'], trigger_db_name, db_user, db_pass,
                sql,
                (2, pre_msg, old_pri_db_dict['host'], json.dumps(before_cluster_dict), json.dumps(cluster_dict)))

            if err_code != 0:
                log_error(task_id, f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary error: {err_msg}")
        # 更新机房信息
        log_info(task_id, f"Update cluster(cluster_id: {cluster_id}) computer room information")
        err_code, err_msg = pg_helpers.update_cluster_room_info(cluster_id)
        if err_code != 0:
            msg = f"Update cluster(cluster_id: {cluster_id}) computer room information FAiled:  {err_msg}"
            log_error(task_id, msg)
            return -1, msg

        ret_msg = f'{pre_msg}: success.'
        log_info(task_id, ret_msg)
        return 0, 'OK'
    except Exception:
        error_msg = f'{pre_msg}: with unexpected error,{traceback.format_exc()}.'
        log_info(task_id, error_msg)
        return -1, error_msg
    finally:
        dao.set_cluster_state(cluster_id, clu_state)


def task_polar_switch(task_id, cluster_id, new_pri_db_id, old_pri_db_id):
    err_code, err_msg = polar_switch(task_id, cluster_id, new_pri_db_id, old_pri_db_id)
    if err_code != 0:
        general_task_mgr.complete_task(task_id, -1, err_msg)
    else:
        general_task_mgr.complete_task(task_id, 1, 'Success')


def change_meta(cluster_id, key, value):
    pre_msg = f"Chanage cluster({cluster_id}) meta: key={key}, value={value}"
    logging.info(f"{pre_msg}: Begin ...")
    try:
        cluster_dict = dao.get_cluster(cluster_id)
        if cluster_dict is None:
            return -1, "Cluster does not exist."
        # 设置元数据，其中key的形式为 db_list.0.port, value为5432
        k_tree = key.split('.')
        if k_tree[0] == 'db_list':
            db_id = int(k_tree[1]) + 1
            attr = k_tree[2]
            dao.set_cluster_db_attr(cluster_id, db_id, attr, value)
        elif k_tree[0] == 'state':
            dao.set_cluster_state(cluster_id, value)
        else:
            if value.isdigit():
                value = int(value)
            cluster_dict = dao.get_cluster(cluster_id)
            if k_tree[0] not in cluster_dict:
                return 1, f"Invalid key: {k_tree[0]}"
            attr_data = cluster_dict[k_tree[0]]
            child_data = attr_data
            for k in k_tree[1:-1]:
                if k.isdigit():
                    if not isinstance(child_data, list):
                        return 1, f"Invalid key: {key}"
                    idx = int(k)
                    if idx >= len(child_data):
                        return 1, f"Invalid key: {key}"
                    child_data = child_data[idx]
                else:
                    if not isinstance(child_data, dict):
                        return 1, f"Invalid key: {key}"
                    if k not in child_data:
                        return 1, f"Invalid key: {key}"
                    child_data = child_data[k]
            if len(k_tree) > 1:
                child_data[k_tree[-1]] = value
            else:
                attr_data = value
            dao.set_cluster_data_attr(cluster_id, k_tree[0], attr_data)
        # 释放集群锁
        logging.info(f"{pre_msg}: Finished.")
        return 0, 'ok'

    except Exception as e:
        logging.info(f'{pre_msg}: completed.')
        err_msg = f"{pre_msg}: Unable to set the status of the cluster: {str(e)}"
        logging.error(f"{pre_msg}: Unable to set the status of the cluster: {traceback.format_exc()}")
        return -1, err_msg


def online_sr_cluster(cluster_dict):
    cluster_id = cluster_dict['cluster_id']
    pre_msg = f"begin online cluster({cluster_id})"
    logging.info(f"{pre_msg} ...")

    # 在上线之前，先做一系列的检查，检查的错误结果放在err_result中
    clu_db_list = dao.get_cluster_db_list(cluster_id)
    cluster_dict['db_list'] = clu_db_list
    err_result = []

    str_ip_list = config.get('probe_island_ip')
    probe_island_ip_list = []
    if str_ip_list:
        probe_island_ip_list = str_ip_list.split(',')
        probe_island_ip_list = [ip.strip() for ip in probe_island_ip_list]

    for db in clu_db_list:
        ip = db['host']
        probe_island_ip_list.append(ip)

    # 最多探测三个IP
    probe_island_ip_list = probe_island_ip_list[:3]
    island = True
    try:
        for ip in probe_island_ip_list:
            ip = ip.strip()
            res = ping_lib.ping_ip(ip, 2, 3)
            if res == -1:
                # dao.set_cluster_db_state(cluster_id, bad_db_dict['db_id'], node_state.UNKNOWN)
                continue
            else:
                island = False
                break
    except Exception as e:
        err_result.append([f"The configuration item 'probe_island_ip={str_ip_list}' in clup.conf is incorrect, which is causing the ping to fail: {repr(e)}",
        "The configuration item 'probe_island_ip' cannot be a broadcast address or an address of a certain network. It needs to be a valid IP address of a normal host!"])
        return err_result
    if island:
        err_result.append([f"The machine has become an isolated network island as it cannot ping any other hosts in the network, including the IP address {str_ip_list}",
         f"It could also be that the configuration item 'probe_island_ip={str_ip_list}' in clup.conf is incorrect"])
        return err_result

    for db in clu_db_list:
        ip = db['host']
        logging.info(f"{pre_msg}: check if the clup-agent on {ip} is started.")
        err_code, err_msg = rpc_utils.get_rpc_connect(ip)
        if err_code != 0:
            logging.info(f"{pre_msg}: can not connect the clup-agent on {ip} is started.")
            err_result.append([f"Unable to connect to IP address ({ip})", "It's possible that clup-agent is not running on this host"])
            continue
        rpc = err_msg
        try:
            err_code, ret = rpc.check_os_env()
            if err_code != 0:
                logging.info(f"{pre_msg}: Execution of OS-level check on host (IP: {ip}) failed: {ret}")
                err_result.append([f"Cannot perform OS-level check on host (IP: {ip})", str(ret)])
            else:
                if len(ret) > 0:
                    logging.info(f"{pre_msg}: When performing OS-level checks on the host (IP: {ip}), the following issues were discovered: {str(ret)}")
                    err_result.extend([([f"{pre_msg}: {i[0]}", i[1]]) for i in ret])

            pgdata = db['pgdata']
            ret = rpc.os_path_exists(pgdata)
            if not ret:
                logging.info(f"{pre_msg}: The data directory({pgdata}) of the database was not found on the host (IP: {ip}).")
                err_result.append([f"The database directory({pgdata}) does not exist on the host({ip}).",
                 "Please check the configuration or confirm whether the database instance is created on the host."])
            else:
                logging.info(f"{pre_msg}: The data directory({pgdata}) of the database exists on the host({ip}).")
        finally:
            rpc.close()

    # 检查集群数据库是否已启动
    for db in clu_db_list:
        db_dict = dao.get_db_state(db["db_id"])
        if db_dict['db_state'] != 0:
            err_result.append(f"Please check if the database(id={db['db_id']}) host=({db['host']}) is already started.")
            return err_result

    # 开始检查主备库之间的流复制
    logging.info(f"{pre_msg}: Start checking if replication is functioning properly...")
    pri_db = None
    for db in clu_db_list:
        if db['is_primary'] == 1:
            pri_db = db
            break
    if not pri_db:
        logging.info(f"{pre_msg}: No primary in the streaming replication!")
        err_result.append(["There is no primary configured in the database.", "Please check the configuration of the database in the cluster"])
        return err_result

    db_port = cluster_dict['port']

    # 从第一个库中获得repl_user和repl_pass, cluster_data中的db_repl_user和db_repl_user废弃了
    # ha_db_user = cluster_dict['ha_db_user']
    # ha_db_pass = db_encrypt.from_db_text(cluster_dict['ha_db_pass'])
    # db_repl_user = cluster_dict['db_repl_user']
    db_user = clu_db_list[0]['db_user']
    db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])
    repl_user = clu_db_list[0]['repl_user']


    sql = " SELECT db_id, host, port " \
        "FROM clup_db ,(SELECT up_db_id FROM clup_db WHERE cluster_id=%s) AS t " \
        " WHERE t.up_db_id=db_id"
    up_db_list = dbapi.query(sql, (cluster_id, ))

    if len(up_db_list) == 0:
        up_db_list = [{'db_id': pri_db['db_id'], 'host': pri_db['host']}]

    repl_data = []
    logging.info(f"{pre_msg}: Starting to check if the replication stream between standby and primary database is normal. ...")
    for db in up_db_list:
        sql = 'select usename, application_name, client_addr::text as client_addr, state from pg_stat_replication'
        err_code, data = pg_utils.sql_query_dict(db['host'], db_port, 'template1', db_user, db_pass, sql)

        if err_code != 0:
            logging.info(f"{pre_msg}: run {sql} on host({db['host']}) failed: {data}")
            err_result.append([f"run sql({sql}) on host({db['host']}) failed: {data}", "Please check the error log, it might be due to the database not being started."])
            logging.info(f"{pre_msg}: Check failed, online failed!")
            return err_result
        repl_data.extend(data)
    pri_ip = pri_db['host']
    for db in clu_db_list:
        if db['is_primary'] == 1:
            continue
        is_find = False
        state = None
        real_repl_user = ''
        repl_app_name = db['repl_app_name']
        tmp_pri_ip = pri_ip
        for row in up_db_list:
            if db['up_db_id'] == row['db_id']:
                tmp_pri_ip = row['host']
        for row in repl_data:
            if row['application_name'] == repl_app_name:
                is_find = True
                state = row['state']
                real_repl_user = row['usename']
                break
        if is_find:
            if state != 'streaming':
                logging.info(f"{pre_msg}: The standby database({db['host']}) is connected to the primary database({tmp_pri_ip}), but the replication state is not streaming, but rather in a certain state({state}).")
                err_result.append([f"The standby database({db['host']}) is connected to the primary database({tmp_pri_ip}), but the replication state is not streaming, but rather in a certain state({state})",
                                   "Please check the configuration of streaming replication."])
            if real_repl_user != repl_user:
                logging.info(f"{pre_msg}: The standby database({db['host']}) is connected to the primary database({tmp_pri_ip}), but the user({real_repl_user}) being used is not the configured username({repl_user}).")
                err_result.append([f"The standby database({db['host']}) is connected to the primary database({tmp_pri_ip}), but the user({real_repl_user}) being used is not the configured username({repl_user}).",
                                   "Please check the configured username for streaming replication."])
        else:
            logging.info(f"{pre_msg}: The standby database({db['host']}, application_name={repl_app_name}) is not connected to the primary database({tmp_pri_ip}).")
            err_result.append([f"The standby database({db['host']},application_name={repl_app_name}) didn't connect to the primary database({tmp_pri_ip}), and the reason for the failure is:",
                               f"1. The application_name in the primary_conninfo of the standby database's replication configuration is not configured as {db['host']}.",
                               "2. The standby database is too far behind the primary database, causing the required WAL log files to be already removed on the primary.;",
                               "3. Replication configuration error caused the standby database to fail to connect to the primary database.",
                               "Please check the configuration of streaming replication."])

    db_port = cluster_dict['port']
    probe_db_name = cluster_dict['probe_db_name']
    logging.info(f"{pre_msg}: Please check if the database({probe_db_name}) is created on the database instance on the host machine({pri_ip}) ...")
    sql = 'SELECT count(*) as cnt FROM pg_database WHERE datname=%s'
    err_code, rows = pg_utils.sql_query_dict(pri_ip, db_port, 'template1', db_user, db_pass, sql,
                                             (probe_db_name,))
    if err_code != 0:
        err_msg = f"Cannot connect to the database using the username({db_user}) and password in the cluster configuration: {rows}"
        logging.info(f"{pre_msg}: Cluster online failed: {err_msg}")
        err_result.append([f"Unable to connect to the database on the host({pri_ip}) using the username({db_user}) and password specified in the cluster configuration:{rows}",
                           "Please check if the username and password for managing the cluster's database are correct"])
        logging.info(f"{pre_msg}: Online failed!")
        return err_result

    if rows[0]['cnt'] == 0:
        logging.info(f"{pre_msg}: The database({probe_db_name}) has not been created in the database instance on the host({pri_ip})，Creating now ...")
        sql = f'CREATE DATABASE {probe_db_name};'
        err_code, err_msg = pg_utils.sql_exec(pri_ip, db_port, 'template1', db_user, db_pass, sql)
        if err_code != 0:
            msg = f"{pre_msg}: Unable to create a probe database({probe_db_name}): {err_msg}"
            logging.info(f"Cluster online failed: {msg}")
            err_result.append([f"{pre_msg}: Failed to create the database for probing({probe_db_name})", err_msg])
            logging.info(f"{pre_msg}: Online failed!")
            return err_result
    else:
        logging.info(f"{pre_msg}: The database({probe_db_name}) already exists in the database instance on the host({pri_ip}), there's no need to create it.")

    probe_table = 'cs_sys_heartbeat'
    logging.info(f"{pre_msg}: Check if a table({probe_table}) exists in database({probe_db_name}) in the database instance on the host({pri_ip}) ...")
    sql = "SELECT count(*) as cnt FROM pg_class WHERE relname=%s and relkind='r'"
    err_code, rows = pg_utils.sql_query_dict(pri_ip, db_port, probe_db_name, db_user, db_pass, sql,
                                             (probe_table,))
    if err_code != 0:
        logging.info(f"{pre_msg}: An error occurred when checking if a table({probe_table}) is created in database({probe_db_name}) on the database instance on the host({pri_ip}): {rows}")
        err_result.append([f"An error occurred when checking if a table({probe_table}) is created in database({probe_db_name}) on the database instance on the host({pri_ip}): {rows}",
                           "Please check the error logs to determine the cause."])
        logging.info(f"{pre_msg}: Online failed!")
        return err_result
    if rows[0]['cnt'] == 0:
        logging.info(f"{pre_msg}: In the database instance on the host({pri_ip}), the table({probe_table}) does not exist in the database({probe_db_name}), so it will be created ...")
        sql = 'CREATE TABLE cs_sys_heartbeat(hb_time TIMESTAMPTZ);INSERT INTO cs_sys_heartbeat VALUES(now())'
        err_code, err_msg = pg_utils.sql_exec(pri_ip, db_port, probe_db_name, db_user, db_pass, sql)
        if err_code != 0:
            logging.info(f"{pre_msg}: Failed to create a table({probe_table}) in the database({probe_db_name}) on the database instance on the host({pri_ip}): {err_msg}")
            err_result.append([f"Failed to create a table({probe_table}) in the database({probe_db_name}) on the database instance on the host({pri_ip}): {err_msg}",
                "Please check the error logs to determine the cause."])
            logging.info(f"{pre_msg}: Online failed!")
            return err_result
    else:
        logging.info(f"{pre_msg}: The table({probe_table}) already exists in the database({probe_db_name}) in the database instance on the host({pri_ip}), so there is no need to create it.")

    trigger_db_name = cluster_dict['trigger_db_name']
    trigger_db_func = cluster_dict['trigger_db_func']
    if trigger_db_name and trigger_db_func:
        logging.info(f"{pre_msg}: Start checking if the database trigger action configuration is correct....")
        sql = f"select {trigger_db_func}(0, 'online', %s, %s, %s);"
        err_code, data = pg_utils.sql_query_dict(
            pri_db['host'],
            db_port,
            trigger_db_name,
            db_user,
            db_pass,
            sql,
            (pri_db['host'], json.dumps(cluster_dict), json.dumps(cluster_dict))
        )
        if err_code != 0:
            logging.info(f"{pre_msg}: run {sql} failed: {data}")
            err_result.append([f"Execution of database trigger action failed.: run {sql} failed: {data}",
            f"Please check if the functions({trigger_db_func}) in the database({trigger_db_name}) exist and can be executed correctly."])
            logging.info(f"{pre_msg}: Check failed, Online failed!")
            return err_result

    if len(err_result) > 0:
        logging.info(f"{pre_msg}: Check failed, Online failed!")
        return err_result[0]
    dao.set_cluster_state(cluster_id, cluster_state.NORMAL)
    logging.info(f"{pre_msg}: Online successful.")
    return err_result


def online(cluster_id):
    err_result = []
    try:
        cluster_dict = dao.get_cluster(cluster_id)
        err_result = []
        if cluster_dict['state'] not in [0, -1]:
            logging.info(f"The cluster({cluster_id}) is not in an offline or failed state, so it cannot be put online!")
            err_result.append(["The cluster is not in an offline or failed state, so it cannot be put online!", "Invalid operation."])
            return -1, err_result
        cluster_type = cluster_dict['cluster_type']
        if cluster_type == 1:
            err_result = online_sr_cluster(cluster_dict)
            if len(err_result) > 0:
                return -1, err_result
        elif cluster_type == 11:
            err_result = online_sr_cluster(cluster_dict)
            if len(err_result) > 0:
                return -1, err_result
        else:
            return -1, "cluster_type error: (ha_mgr.online)"
        if len(err_result) > 0:
            return -1, err_result

    except Exception as e:
        logging.error(f"An unknown error occurred during the cluster({cluster_id}) online process: {traceback.format_exc()}")
        err_result.append(["Unknown error", str(e)])
        return -1, err_result

    return 0, []


def offline(cluster_id):
    try:
        dao.set_cluster_state(cluster_id, cluster_state.OFFLINE)
        return 0, ''
    except Exception as e:
        logging.error(traceback.format_exc())
        return -1, repr(e)


def check_auto_failback(cluster_id, db_id, up_db_id, restore_cluster_state):

    tblspc_dir = pg_helpers.get_pg_tblspc_dir(db_id)
    sql = "SELECT db_detail-> 'is_rewind' as is_rewind, " \
          "db_detail->'is_rebuild' as is_rebuild, " \
          "db_detail->'failback_count' as failback_count, " \
          "db_detail->'rm_pgdata' as rm_pgdata FROM clup_db WHERE db_id=%s"
    rows = dbapi.query(sql, (db_id,))
    if not rows:
        dao.set_cluster_state(cluster_id, restore_cluster_state)
        dao.update_db_state(db_id, database_state.FAULT)
        dao.set_node_state(db_id, node_state.FAULT)
        return

    pdict = {
        'cluster_id': cluster_id,
        'db_id': db_id,
        'up_db_id': up_db_id,
        'tblspc_dir': tblspc_dir,
        'is_rewind': rows[0].get('is_rewind', False),
        'is_rebuild': rows[0].get('is_rebuild', False),
        'rm_pgdata': rows[0].get('rm_pgdata', 0),
        'failback_count': rows[0].get('failback_count')
    }
    if not pdict['failback_count']:
        failback_count = 0
    else:
        failback_count = int(pdict['failback_count'])
    # ========= 超过failback次数，直接返回
    if failback_count >= 3:
        dao.set_cluster_state(cluster_id, restore_cluster_state)
        dao.update_db_state(db_id, database_state.FAULT)
        dao.set_node_state(db_id, node_state.FAULT)
        return

    logging.info('Starting automatic cluster recovery')
    task_name = f"failback {cluster_id}(db={db_id})"
    task_id = general_task_mgr.create_task(task_type_def.FAILBACK, task_name, {'cluster_id': cluster_id})
    general_task_mgr.run_task(task_id, failback, (pdict, restore_cluster_state))
    # 更新failback的次数
    update_dict = json.dumps({"failback_count": failback_count + 1})
    sql = "UPDATE clup_db SET db_detail = db_detail || (%s::jsonb) WHERE db_id = %s"
    dbapi.execute(sql, (update_dict, pdict['db_id']))
