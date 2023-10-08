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
@description: 处理HA的切换、故障处理等逻辑的模块
"""

import copy
import json
import logging
import time
import traceback

import cluster_state
import dao
import database_state
import db_encrypt
import general_task_mgr
import lb_mgr
import node_state
import pg_db_lib
import pg_helpers
import ping_lib
import polar_lib
import probe_db
import rpc_utils
import run_lib
import task_type_def


def task_log_info(task_id, msg):
    logging.info(msg)
    general_task_mgr.log_info(task_id, msg)


def task_log_warn(task_id, msg):
    logging.warning(msg)
    general_task_mgr.log_warn(task_id, msg)


def task_log_error(task_id, msg):
    logging.warning(msg)
    general_task_mgr.log_error(task_id, msg)


def failover_sr_cluster(cluster_id, pg, db_port, err_msg_list):
    """
    这个函数结束的时候不要设置集群的状态，在调用此函数的地方，根据返回结果会自动设置集群的状态
    """

    dao.set_cluster_state(cluster_id, cluster_state.FAILOVER)
    is_primary = pg['is_primary']
    db_id = pg['db_id']

    # 记录日志
    if is_primary:
        task_name = f"failover {cluster_id}(primary dbid={db_id})"
    else:
        task_name = f"failover {cluster_id}(standby dbid={db_id})"

    task_id = general_task_mgr.create_task(task_type_def.FAILOVER, task_name, {'cluster_id': cluster_id})
    message = f"Cluster({cluster_id}): Find database({pg['host']}:{db_port}) failed, begin failover..."
    for err_msg in err_msg_list:
        general_task_mgr.log_info(task_id, err_msg)
    general_task_mgr.log_info(task_id, message)

    host_is_ok = False
    err_code, err_msg = rpc_utils.get_rpc_connect(pg['host'])
    if err_code == 0:
        host_is_ok = True
        rpc = err_msg
        rpc.close()
        rpc = None

    cnt = 0
    retry_cnt = 3
    # 检查是否是polardb，如果是且为master或reader节点则需要启动pfs
    polar_type_list = ['master', 'reader']
    polar_type = pg.get('polar_type', None)

    if host_is_ok:
        task_log_info(task_id, f"Cluster({cluster_id}): Host({pg['host']}) is ok, only database({pg['host']}:{db_port}) failed, restart database ....")
        if polar_type in polar_type_list:
            err_code, err_msg = polar_lib.start_pfs(pg['host'], pg['db_id'])
            if err_code != 0 and err_code != 1:
                return -1, err_msg
        # end start pfs
        while cnt < retry_cnt:
            ret, err_msg = pg_db_lib.start(pg['host'], pg['pgdata'])
            if ret < 0:
                task_log_info(task_id, f"Cluster({cluster_id}): restart database({pg['host']}:{db_port}) failed({str(err_msg)}), sleep 5 seconds, then retry is ...")
                time.sleep(5)
            else:
                # 使用psql检查一下有没有启动成功,这里需要延时一段时间，防止数据库处于启动中的状态无法查询成功
                time.sleep(5)
                err_code, err_msg = pg_helpers.psql_test(pg['db_id'], "SELECT 1")
                if err_code != 0:
                    task_log_error(task_id, f"Cluster({cluster_id}): restart database({pg['host']}:{db_port}) failed({str(err_msg)}), Attempt to restart the database")
                    ret, err_msg = pg_db_lib.stop(pg['host'], pg['pgdata'])
                    cnt += 1
                    continue
                break
            cnt += 1

        if cnt >= retry_cnt:
            err_msg = f"Cluster({cluster_id}): can not start database({pg['host']}:{db_port}), failover ..."
            task_log_info(task_id, err_msg)
        else:
            # 重启数据库成功
            err_msg = f"Cluster({cluster_id}): successful start up database({pg['host']}:{db_port})"
            general_task_mgr.complete_task(task_id, 1, err_msg)
            return 0, err_msg
    else:
        task_log_info(task_id, f"Cluster({cluster_id}): Host({pg['host']}) is not ok, failover database({pg['host']}:{db_port})...")

    ping_ip_list = dao.get_cluster_db_ip_list(cluster_id)
    island = True
    for ip in ping_ip_list:
        res = ping_lib.ping_ip(ip, 2, 3)
        if res == -1:
            # dao.set_cluster_db_state(cluster_id, bad_db_dict['db_id'], node_state.UNKNOWN)
            continue
        else:
            island = False
            break

    if island:
        err_msg = "Clup became Isolated network, so can not failover."
        task_log_info(task_id, err_msg)
        general_task_mgr.complete_task(task_id, -1, err_msg)
        return -1, err_msg
    try:
        is_primary = pg['is_primary']
        db_id = pg['db_id']

        if is_primary:
            if polar_type == 'master':
                err_code, err_msg = failover_polar_primary_db(task_id, cluster_id, db_id)
            else:
                err_code, err_msg = failover_primary_db(task_id, cluster_id, db_id)
        else:
            if not polar_type:
                err_code, err_msg = failover_standby_db(task_id, cluster_id, db_id)
    except Exception:
        err_code = -1
        trace_msg = traceback.format_exc()
        err_msg = f"Unexpected error occurred: {trace_msg}"

    if err_code < 0:
        general_task_mgr.complete_task(task_id, -1, err_msg)
    else:
        general_task_mgr.complete_task(task_id, 1, err_msg)
    return err_code, err_msg


def failover_standby_db(task_id, cluster_id, db_id):
    db = dao.get_db_info(db_id)
    clu_db_list = dao.get_cluster_db_list(cluster_id)
    before_cluster_dict = dao.get_cluster(cluster_id)
    if before_cluster_dict is None or (not clu_db_list):
        info_msg = f"exit failover standby when cluster({cluster_id}) has been deleted"
        logging.info(info_msg)
        return -1, info_msg

    before_cluster_dict['db_list'] = copy.copy(clu_db_list)
    cluster_dict = copy.copy(before_cluster_dict)
    cluster_dict['db_list'] = clu_db_list
    db_port = cluster_dict['port']

    bad_db_dict = None
    for db_dict in clu_db_list:
        if db_dict['db_id'] == db_id:
            db_dict['state'] = node_state.FAILOVER
            bad_db_dict = db_dict
            break
    if not bad_db_dict:
        msg = f"db(db_id={db_id}) not find!!"
        task_log_warn(task_id, msg)
        return 1, msg

    pri_db_dict = None
    for db_dict in clu_db_list:
        if db_dict['is_primary'] == 1 and db_dict['state'] == node_state.NORMAL:
            pri_db_dict = db_dict
            break

    pre_msg = f"Failover standby database({bad_db_dict['host']}:{db_port})"

    try:
        # 如果此备库上有read_vip，需要把read_vip切换到另一台备库上
        is_have_read_vip = False
        read_vip_host = cluster_dict['read_vip_host']
        read_vip = cluster_dict.get('read_vip')
        str_cstlb_list = cluster_dict['cstlb_list']
        if not str_cstlb_list:
            cstlb_list = []
        else:
            cstlb_list = str_cstlb_list.split(',')
            cstlb_list = [k.strip() for k in cstlb_list]
        # cstlb_ip_list = [ip_port.split(':')[0] for ip_port in cstlb_list]
        if bad_db_dict['host'] == read_vip_host and read_vip:
            is_have_read_vip = True

        if is_have_read_vip:  # 如果本节点上存在只读vip，需要找到另一个合适的备库，然后把只读vip切换过去
            new_pg = None
            for db_dict in clu_db_list:
                if db_dict['db_id'] == db_id:
                    continue
                if db_dict['state'] == node_state.NORMAL and db_dict['is_primary'] == 0:
                    # 如果这台机器上有read vip，则需要在cstlb_ip_list中找备库，而不能是任意的备库
                    # if is_have_read_vip and db_dict['host'] not in cstlb_ip_list:
                    #     continue
                    # 找到一个好的备库，准备把read_vip切换到这台机器上
                    new_pg = db_dict
                    break

            if not new_pg:
                msg = f"{pre_msg}: can not find a normal standby database, so switch failed!!!"
                # 无法把read vip切换到好的机器上，现直接把此备库的状态改为“FAULT”
                dao.set_cluster_db_state(cluster_id, bad_db_dict['db_id'], node_state.FAULT)
                task_log_error(task_id, msg)
                return -1, msg
            else:
                task_log_info(task_id, f"{pre_msg}: read vip({read_vip}) switch to host({new_pg['host']}).")
                dao.set_cluster_db_state(cluster_id, bad_db_dict['db_id'], node_state.FAILOVER)

            if is_have_read_vip:
                new_host = new_pg['host']
                task_log_info(task_id, f"{pre_msg}: switch read vip({read_vip}) to host({new_host})...")
                err_code, err_msg = rpc_utils.check_and_del_vip(read_vip_host, read_vip)
                if err_code != 0:
                    task_log_info(task_id, f"{pre_msg}: host({read_vip_host}) maybe is down, can not del vip({read_vip}): {err_msg}")

                rpc_utils.check_and_add_vip(new_host, read_vip)
                if err_code != 0:
                    task_log_info(task_id, f"{pre_msg}: host({new_host}) maybe is down, can not add vip({read_vip}): {err_msg}")

        if read_vip:  # 如果集群配置了只读vip，说明有读写分离的负载均衡功能，需要从从负载均衡器cstlb中把坏节点剔除掉
            backend_addr = f"{bad_db_dict['host']}:{db_port}"
            task_log_info(task_id, f'{pre_msg}: begin remove bad host({backend_addr}) from cstlb ...')
            for lb_host in cstlb_list:
                err_code, msg = lb_mgr.delete_backend(lb_host, backend_addr)
                if err_code != 0:
                    task_log_error(task_id, f"Can not remove host({backend_addr}) from load balance({lb_host}): {msg}")
            task_log_info(task_id, f'{pre_msg}: remove bad host({backend_addr}) from cstlb finished.')

        task_log_info(task_id, f"{pre_msg}: save node state to meta server...")
        try:
            dao.set_cluster_db_state(cluster_id, bad_db_dict['db_id'], node_state.FAULT)
            bad_db_dict['state'] = node_state.FAULT
            task_log_info(task_id, f"{pre_msg}: save node state to meta server completed.")
        except Exception as e:
            task_log_error(task_id,
                    f"{pre_msg}: Unexpected error occurred during save node state to meta server: {repr(e)}")

        # 如果配置了切换时调用的数据库函数，则调用此函数
        trigger_db_name = cluster_dict['trigger_db_name']
        trigger_db_func = cluster_dict['trigger_db_func']
        if trigger_db_name and trigger_db_func and pri_db_dict:
            task_log_info(task_id,
                f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary ...")

            db_user = clu_db_list[0]['db_user']
            db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])
            sql = f"select {trigger_db_func}(%s, %s, %s, %s, %s);"
            err_code, err_msg = probe_db.exec_sql(
                pri_db_dict['host'], db_port, trigger_db_name, db_user, db_pass,
                sql,
                (1, pre_msg, bad_db_dict['host'], json.dumps(before_cluster_dict), json.dumps(cluster_dict))
            )

            if err_code != 0:
                task_log_error(task_id, f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary error: {err_msg}")

        msg = f"{pre_msg}: all commpleted."
        # leifliu Test
        # task_log_info(task_id, msg)
        # 如果当前坏的备库有下级，则把下级库接到此备库的上级库，当然如果出现了特殊情况，此库没有上级库，则什么页不做
        # 将当前数据库的子节点接到他的上级主库上面
        lower_rows = dao.get_lower_db(db_id)
        lower_list = [row['db_id'] for row in lower_rows]
        if lower_list:
            # 如果有下级备库就接到上级
            up_rows = dao.get_up_db(db_id)
            if len(up_rows) == 0:
                msg += f'; The current database(db_id: {db_id}) does not have a parent primary database.'
                return 0, msg
            change_msg = ""
            for db in lower_list:
                err_code, err_msg = pg_helpers.change_up_db_by_db_id(db, up_rows[0]['up_db_id'])
                if err_code != 0:
                    change_msg += err_msg
            msg += "; standby change up_db" + change_msg
    except Exception:
        return -1, traceback.format_exc()
    return 0, msg


def failover_primary_db(task_id, cluster_id, db_id):
    db = dao.get_db_info(db_id)
    clu_db_list = dao.get_cluster_db_list(cluster_id)
    before_cluster_dict = dao.get_cluster(cluster_id)
    if before_cluster_dict is None or (not clu_db_list):
        msg = f"exit failover primary when cluster({cluster_id}) has been deleted"
        logging.info(msg)
        return -1, msg

    before_cluster_dict['db_list'] = copy.copy(clu_db_list)
    cluster_dict = copy.copy(before_cluster_dict)
    cluster_dict['db_list'] = clu_db_list
    db_port = cluster_dict['port']

    vip = cluster_dict['vip']
    read_vip_host = cluster_dict.get('read_vip_host', '')
    read_vip = cluster_dict.get('read_vip', '')
    bad_db_dict = None
    # 先找到主库，把主库上的write vip先删除掉，如果主库的机器出问题了，连接不上去，则不管了
    # FIXME: 目前是去不掉原先的旧主库上的vip，就不管了，存在IP地址冲突的风险，后续可能需要用IPMI等手段强制把旧主库的主机关掉
    for db_dict in clu_db_list:
        if db_dict['db_id'] == db_id:
            db_dict['state'] = node_state.FAILOVER
            bad_db_dict = db_dict
            pre_msg = f"Switch primary database({bad_db_dict['host']}:{db_port})"
            task_log_info(task_id, f"{pre_msg}: begin delete write vip({vip}) ...")
            try:
                rpc_utils.check_and_del_vip(db_dict['host'], vip)
                task_log_info(task_id, f"{pre_msg}: delete write vip({vip}) completed.")
            except OSError:
                task_log_info(task_id, f"{pre_msg}: host({db_dict['host']}) maybe is down, can not delete write vip({vip})")
            except Exception:
                task_log_error(task_id, f"{pre_msg}: Unexpected error occurred "
                                   f"during delete write vip ({vip}) from host({db_dict['host']}): {traceback.format_exc()}")
            break

    if not bad_db_dict:
        msg = f"Switch primary database(dbid={db_id}): can not find this fault primary database!"
        task_log_warn(task_id, msg)
        return 1, msg

    pre_msg = f"Failover primary database({bad_db_dict['host']}:{db_port})"
    # 先把当前数据库的状态设置为“FAILOVER”的状态，等FAILOVER结束了，再把状态改成“FAULT”
    dao.set_cluster_db_state(cluster_id, db_id, node_state.FAILOVER)

    # 找到所有正常工作的备库
    all_good_stb_db = []
    for p in clu_db_list:
        if p['db_id'] != db_id and (not p['is_primary']) and p['state'] == node_state.NORMAL:
            p['scores'] = p.get('scores') if p.get('scores') else 0
            all_good_stb_db.append(p)

    # 在同一机房选择新的主库自动切换
    if not cluster_dict.get('auto_failover', False):
        room_info = pg_helpers.get_current_cluster_room(cluster_id)
        cur_room_id = str(room_info['room_id'])
        all_good_stb_db = [db for db in all_good_stb_db if db.get('room_id', '0') == cur_room_id]

    # 让原先主库的直接下级备库的接收流复制先暂停
    # 获取坏掉主库的一级备库
    lower_rows = dao.get_lower_db(db_id)
    for db_dict in lower_rows:
        host = db_dict['host']
        task_log_info(task_id, f"{pre_msg}: pause standby walreciver in host({host}) ...")
        err_code, err_msg = pg_db_lib.pause_standby_walreciver(host, db_dict)
        # 如果暂停失败，就不管了
        if err_code != 0:
            task_log_info(task_id, f"{pre_msg}: pause standby({host}:{db_dict['port']}) walreciver: {err_msg}")
            continue
        task_log_info(task_id, f"{pre_msg}: pause standby walreciver in host({host}) success")

    # 从第一个库中获得repl_user和repl_pass, cluster_data中的db_repl_user和db_repl_user废弃了
    repl_user = clu_db_list[0]['repl_user']
    repl_pass = db_encrypt.from_db_text(clu_db_list[0]['repl_pass'])

    new_pri_lsn = 0
    new_pri_pg = None
    new_pri_scores = 0
    new_read_vip_host = ''
    max_lsn = 0
    max_lsn_pg = None
    # 先根据scores排序，scores值越大，优先级越低
    all_good_stb_db.sort(key=lambda x: x['scores'], reverse=False)
    for p in all_good_stb_db:
        err_code, str_lsn, _ = probe_db.get_last_lsn(p['host'], db_port, repl_user, repl_pass)
        if err_code != 0:
            task_log_info(task_id, f"{pre_msg}: db({ p['host']}:{db_port}) probe failed! {str_lsn}")
            continue

        lsn = pg_db_lib.lsn_to_int(str_lsn)
        if lsn > max_lsn:
            max_lsn = lsn
            max_lsn_pg = p
        # 第一次时，先把new_pri_pg设置为第一个备库
        if not new_pri_pg:
            new_pri_scores = p['scores']
            new_pri_lsn = lsn
            new_pri_pg = p
            continue

        if p['scores'] == new_pri_scores:
            if lsn > new_pri_lsn:
                new_pri_lsn = lsn
                new_pri_pg = p
                continue

    if not new_pri_pg:
        bad_db_dict['state'] = node_state.FAULT
        dao.set_cluster_db_state(cluster_id, db_id, node_state.FAULT)
        msg = f"{pre_msg}: can't find available standby database, switch failed!!!"
        task_log_error(task_id, msg)
        return -1, msg
    else:
        task_log_info(task_id, f"{pre_msg}: switch to new host({new_pri_pg['host']})...")

    # 如果新主库的lsn落后max_lsn_pg，则需要把新主库先与最大max_lsn_pg同步
    # 先把新主库关掉，然后把旧主库上比较新的xlog文件都拷贝过来：
    if max_lsn > lsn:
        task_log_info(task_id, f"{pre_msg}: stop new pirmary database then sync wal from max lsn standby({max_lsn_pg['host']}) ... ")
        # 停掉新主库
        err_code, err_msg = pg_db_lib.stop(new_pri_pg['host'], new_pri_pg['pgdata'])
        if err_code != 0:
            task_log_info(task_id, f"{pre_msg}: Stop new pirmary database failed, {err_msg}")

        err_code, err_msg = rpc_utils.pg_cp_delay_wal_from_pri(
            new_pri_pg['host'],
            max_lsn_pg['host'],
            max_lsn_pg['pgdata'],
            new_pri_pg['pgdata']
        )

        if err_code != 0:
            task_log_info(task_id, f"{pre_msg}: Sync wal from max lsn standby({max_lsn_pg['host']}) failed, {err_msg}")
            # 是否直接返回，不再继续切换主库
            return -1, err_msg

        task_log_info(task_id, f"{pre_msg}: Sync wal from max lsn standby({max_lsn_pg['host']}) success.")

        # 启动新主库
        err_code, err_msg = pg_db_lib.start(new_pri_pg['host'], new_pri_pg['pgdata'])
        if err_code != 0:
            msg = f"{pre_msg}: Start new pirmary database failed, which host is {new_pri_pg['host']}, {err_msg}"
            task_log_info(task_id, msg)
            return -1, msg

    # 新主库上重新加载还原wal_retrieve_retry_interval的值
    if new_pri_pg:
        err_code, err_msg = pg_db_lib.reload(new_pri_pg['host'], new_pri_pg['pgdata'])
        if err_code != 0:
            task_log_info(task_id, f"{pre_msg}: reload db in host({host}) failed")
        task_log_info(task_id, f"{pre_msg}: reset wal_retrieve_retry_interval in new_pri_host({host}) success")

    # 做为新主库的备库上原先如果有只读vip，尽量把其移动到其他备库上
    if read_vip and read_vip_host == new_pri_pg['host']:
        # 找一个放只读vip的候选备库
        new_read_vip_host = None
        if read_vip_host:
            for p in all_good_stb_db:
                if p['host'] != new_pri_pg['host']:
                    new_read_vip_host = p['host']

        if new_read_vip_host:
            # 如果新的主库上面有之前的只读VIP，需要将只读VIP切换到其他备库上，
            task_log_info(task_id, f"need delete read vip {read_vip} from {read_vip_host}")

            try:
                rpc_utils.check_and_del_vip(read_vip_host, read_vip)
                task_log_info(task_id, f"{pre_msg}: delete read vip({read_vip}) completed.")
            except OSError:
                task_log_info(task_id, f"{pre_msg}: host({read_vip_host}) maybe is down, can not delete read vip({read_vip})")
            task_log_info(task_id, f"need add read vip {read_vip} to {new_read_vip_host}")
            try:
                rpc_utils.check_and_add_vip(new_read_vip_host, read_vip)
                task_log_info(task_id, f'{pre_msg}: add read vip({read_vip}) to other standby ({new_read_vip_host}) completed.')
            except Exception:
                task_log_error(task_id, f"{pre_msg} : unexpected error occurred during add read vip ({read_vip}) "
                    f"to other standby({new_read_vip_host}): {traceback.format_exc()}")
            dao.set_new_read_vip_host(cluster_id, new_read_vip_host)
        else:
            task_log_info(task_id, f"{pre_msg}: no other standby, so read vip {read_vip} still keep in new primary host({new_pri_pg['host']}).")

    # 保存修改
    try:
        bad_db_dict['is_primary'] = 0
        new_pri_pg['is_primary'] = 1
        dao.set_cluster_db_attr(cluster_id, bad_db_dict['db_id'], 'is_primary', 0)

        dao.set_cluster_db_attr(cluster_id, new_pri_pg['db_id'], 'is_primary', 1)
        dao.update_up_db_id('null', new_pri_pg['db_id'], is_primary=1)
        task_log_info(task_id, f"{pre_msg}: save node state to meta server completed.")
    except Exception as e:
        msg = f"{pre_msg}: Unexpected error occurred during save node state to meta server: {repr(e)}"
        task_log_error(task_id, msg)
        task_log_info(task_id, f"{pre_msg} failed!!!")
        return -1, msg
    finally:
        bad_db_dict['state'] = node_state.FAULT
        dao.set_cluster_db_state(cluster_id, db_id, node_state.FAULT)

    # 因为选中的备库会变成主库，所以需要从负载均衡器cstlb中把节点剔除掉
    task_log_info(task_id, f"{pre_msg}: begin remove new primary database({new_pri_pg['host']}) from cstlb ...")
    str_cstlb_list = cluster_dict['cstlb_list']
    if not str_cstlb_list:
        cstlb_list = []
    else:
        cstlb_list = str_cstlb_list.split(',')
        cstlb_list = [k.strip() for k in cstlb_list]
    for lb_host in cstlb_list:
        backend_addr = f"{new_pri_pg['host']}:{db_port}"
        err_code, msg = lb_mgr.delete_backend(lb_host, backend_addr)
        if err_code != 0:
            task_log_error(task_id, f"Can not remove host({backend_addr}) from cstlb({lb_host}): {msg}")
    task_log_info(task_id, f"{pre_msg}: remove new primary database({bad_db_dict['host']}) from cstlb finished.")

    try:
        if not cluster_dict.get('failover_keep_cascaded', False):  # 如果不保留级联关系，则把需要把其它备库都指向新的主库
            task_log_info(task_id, f"{pre_msg}: change all standby database upper level primary database to host({new_pri_pg['host']})...")
            for p in clu_db_list:
                if not p['instance_type']:
                    p['instance_type'] = 'physical'
                if p['db_id'] != db_id and (not p['is_primary']) and p['state'] == node_state.NORMAL:
                    pg_db_lib.pg_change_standby_updb(p['host'], p['pgdata'], repl_user, repl_pass,
                                                    p['repl_app_name'], new_pri_pg['host'], db_port)
                    dao.update_up_db_id(new_pri_pg['db_id'], p['db_id'], p['is_primary'])
            task_log_info(task_id,
                    f"{pre_msg}: "
                    f"change all standby database upper level primary database to host({new_pri_pg['host']}) completed.")
        else:  # 如果保留级联关系，则把旧主库指向新主库
            pg_helpers.change_up_db_by_db_id(db_id, new_pri_pg['db_id'])
            # 将坏掉的主库的下级备库都接到新的主库上面
            if lower_rows:
                msg = ""
                for db in lower_rows:

                    err_code, err_msg, data = pg_helpers.check_sr_conn(new_pri_pg['db_id'], db['db_id'])
                    if err_code == 0 and data.get('cnt') == 1:
                        continue
                    err_code, err_msg = pg_helpers.change_up_db_by_db_id(db['db_id'], new_pri_pg['db_id'])
                    if err_code != 0:
                        msg += err_msg
                if msg:
                    task_log_error(task_id, f"change up db(db_id: {new_pri_pg['db_id']}]) error: {msg}")

        # 把新主库的上级库设置为空
        dao.update_up_db_id('null', new_pri_pg['db_id'], 1)
        # pg_helpers.change_up_db_by_db_id(new_pri_pg['db_id'], None)

    except Exception:
        msg = f"{pre_msg}: change all standby database upper level primary database to host({new_pri_pg['host']}) failed: " \
              f"Unexpected error occurred: {traceback.format_exc()}"
        task_log_error(task_id, msg)
        task_log_info(task_id, f"{pre_msg} failed!!!")
        return -1, msg
    finally:
        bad_db_dict['state'] = node_state.FAULT
        dao.set_cluster_db_state(cluster_id, db_id, node_state.FAULT)

    # 把选中的这个备库激活成主库
    task_log_info(task_id, f"{pre_msg}: Promote standby database({new_pri_pg['host']}) to primary...")
    err_code, err_msg = pg_db_lib.promote(new_pri_pg['host'], new_pri_pg['pgdata'])
    if err_code != 0:
        msg = f"{pre_msg}: promote standby database({new_pri_pg['host']}) to primary failed: {err_msg}"
        task_log_error(task_id, msg)
        task_log_info(task_id, f"{pre_msg} failed!!!")
        return -1, msg
    else:
        task_log_info(task_id, f"{pre_msg}: Promote standby database({new_pri_pg['host']}) to primary completed.")

    step = f"{pre_msg}: Update cluster(cluster_id: {cluster_id}) computer room information"
    task_log_info(task_id, step)
    err_code, err_msg = pg_helpers.update_cluster_room_info(cluster_id)
    if err_code != 0:
        msg = f"{step} Failed: {err_msg}"
        task_log_error(task_id, msg)
        return -1, msg

    step = f"{pre_msg}: check new primary database is running ..."
    err_code, is_run = pg_db_lib.is_running(new_pri_pg['host'], new_pri_pg['pgdata'])
    if err_code == 0 and not is_run:
        pg_db_lib.start(new_pri_pg['host'], new_pri_pg['pgdata'])
    if err_code != 0:
        task_log_error(task_id, f"({step} error: {err_msg}")
    task_log_info(task_id, f"({step} success ")

    # 如果配置了切换时调用的数据库函数，则调用此函数，不管调用是否成功，都认为切换成功，只是打印了一个错误日志
    trigger_db_name = cluster_dict['trigger_db_name']
    trigger_db_func = cluster_dict['trigger_db_func']
    if trigger_db_name and trigger_db_func:
        task_log_info(task_id,
                 f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary ...")

        db_user = clu_db_list[0]['db_user']
        db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])

        sql = "select {0}(%s, %s, %s, %s, %s);".format(trigger_db_func)
        err_code, err_msg = probe_db.exec_sql(
            new_pri_pg['host'], db_port, trigger_db_name, db_user, db_pass,
            sql,
            (2, pre_msg, bad_db_dict['host'], json.dumps(before_cluster_dict), json.dumps(cluster_dict)))

        if err_code != 0:
            task_log_error(task_id, f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary error: {err_msg}")

    ret_msg = f"{pre_msg}: switch to new host({new_pri_pg['host']}) completed."
    return 0, ret_msg


# 修复polardb主库
def failover_polar_primary_db(task_id, cluster_id, db_id):
    db = dao.get_db_info(db_id)
    clu_db_list = dao.get_cluster_db_list(cluster_id)
    before_cluster_dict = dao.get_cluster(cluster_id)
    if before_cluster_dict is None or (not clu_db_list):
        msg = f"exit failover primary when cluster({cluster_id}) has been deleted"
        logging.info(msg)
        return -1, msg

    before_cluster_dict['db_list'] = copy.copy(clu_db_list)
    cluster_dict = copy.copy(before_cluster_dict)
    cluster_dict['db_list'] = clu_db_list
    db_port = cluster_dict['port']

    vip = cluster_dict['vip']
    read_vip_host = cluster_dict.get('read_vip_host', '')
    read_vip = cluster_dict.get('read_vip', '')
    bad_db_dict = None
    # 先找到主库，把主库上的write vip先删除掉，如果主库的机器出问题了，连接不上去，则不管了
    # FIXME: 目前是去不掉原先的旧主库上的vip，就不管了，存在IP地址冲突的风险，后续可能需要用IPMI等手段强制把旧主库的主机关掉
    for db_dict in clu_db_list:
        if db_dict['db_id'] == db_id:
            db_dict['state'] = node_state.FAILOVER
            bad_db_dict = db_dict
            pre_msg = f"Switch primary database({bad_db_dict['host']}:{db_port})"
            task_log_info(task_id, f"{pre_msg}: begin delete write vip({vip}) ...")
            try:
                rpc_utils.check_and_del_vip(db_dict['host'], vip)
                task_log_info(task_id, f"{pre_msg}: delete write vip({vip}) completed.")
            except OSError:
                task_log_info(task_id, f"{pre_msg}: host({db_dict['host']}) maybe is down, can not delete write vip({vip})")
            except Exception:
                task_log_error(task_id, f"{pre_msg}: Unexpected error occurred "
                                   f"during delete write vip ({vip}) from host({db_dict['host']}): {traceback.format_exc()}")
            break

    if not bad_db_dict:
        msg = f"Switch primary database(dbid={db_id}): can not find this fault primary database!"
        task_log_warn(task_id, msg)
        return 1, msg

    pre_msg = f"Failover primary database({bad_db_dict['host']}:{db_port})"
    # 先把当前数据库的状态设置为“FAILOVER”的状态，等FAILOVER结束了，再把状态改成“FAULT”
    dao.set_cluster_db_state(cluster_id, db_id, node_state.FAILOVER)

    # 找到所有正常工作的备库
    all_good_stb_db = []
    for p in clu_db_list:
        if p['db_id'] != db_id and (not p['is_primary']) and p['state'] == node_state.NORMAL:
            p['scores'] = p.get('scores') if p.get('scores') else 0
            all_good_stb_db.append(p)

    # 在同一机房选择新的主库自动切换
    if not cluster_dict.get('auto_failover', False):
        room_info = pg_helpers.get_current_cluster_room(cluster_id)
        cur_room_id = str(room_info['room_id'])
        all_good_stb_db = [db for db in all_good_stb_db if db.get('room_id', '0') == cur_room_id]

    # polarDB 共享存储集群无需判断LSN
    new_pri_pg = None
    new_read_vip_host = ''
    # 先根据scores排序
    all_good_stb_db.sort(key=lambda x: x['scores'], reverse=False)
    for p in all_good_stb_db:
        # 如果是polardb的standby节点就跳过，不能切换到本地存储的standby节点
        polar_type = polar_lib.get_polar_type(p["db_id"])
        if polar_type == "standby":
            continue

        # 直接将排序最前的一个备库作为新主库
        if not new_pri_pg:
            new_pri_pg = p

        if not new_read_vip_host and read_vip_host:
            # 配了只读VIP，需要将只读VIP切换到其他备库上，只有一个主库正常的时候就不管了，两个VIP都放在主库上
            new_read_vip_host = p['host']

    if not new_pri_pg:
        bad_db_dict['state'] = node_state.FAULT
        dao.set_cluster_db_state(cluster_id, db_id, node_state.FAULT)
        msg = f"{pre_msg}: can't find available standby database, switch failed!!!"
        task_log_error(task_id, msg)
        return -1, msg
    else:
        task_log_info(task_id, f"{pre_msg}: switch to new host({new_pri_pg['host']})...")

    if new_read_vip_host and read_vip and read_vip_host == new_pri_pg['host']:
        # 如果新的主库上面有之前的只读VIP，需要将只读VIP切换到其他备库上，
        task_log_info(task_id, f"need delete read vip {read_vip} from {read_vip_host}")

        try:
            rpc_utils.check_and_del_vip(read_vip_host, read_vip)
            task_log_info(task_id, f"{pre_msg}: delete read vip({read_vip}) completed.")
        except OSError:
            task_log_info(task_id, f"{pre_msg}: host({read_vip_host}) maybe is down, can not delete read vip({read_vip})")
        task_log_info(task_id, f"need add read vip {read_vip} to {new_read_vip_host}")
        try:
            rpc_utils.check_and_add_vip(new_read_vip_host, read_vip)
            task_log_info(task_id, f'{pre_msg}: add read vip({read_vip}) to other standby ({new_read_vip_host}) completed.')
        except Exception:
            task_log_error(task_id, f"{pre_msg} : unexpected error occurred during add read vip ({read_vip}) "
                               "to other standby({new_read_vip_host}): {traceback.format_exc()}")
        dao.set_new_read_vip_host(cluster_id, new_read_vip_host)

    # 提升reader节点为主库前需要在主库机器执行reset命令，确保共享磁盘pfs文件系统安全
    is_failed = False
    try:
        reset_cmd = bad_db_dict['reset_cmd'].strip()
        bad_db_host = bad_db_dict['host']
        if bad_db_dict['polar_type'] == 'master':
            if reset_cmd:
                task_log_info(task_id, f"{pre_msg}: need run reset cmd on old primary host({bad_db_host}) ...")
                task_log_info(task_id, f"{pre_msg}: run reset cmd on old primary host({bad_db_host}): {reset_cmd}")
            else:
                is_failed = True
                msg = f"{pre_msg}: Unable to configure the reset_cmd command, unable to stop the host({bad_db_host}) of the original primary database, and the execution of {pre_msg} fails."
                task_log_info(task_id, msg)
                general_task_mgr.complete_task(task_id, -1, msg)
                return -1, msg

            # 执行reset cmd
            err_code, err_msg, out_msg = run_lib.run_cmd_result(reset_cmd)
            if err_code != 0:
                is_failed = True
                msg = f"{pre_msg}: run ({reset_cmd}) in host({bad_db_host}) failed: {err_msg} {out_msg}"
                task_log_info(task_id, msg)
                # return -1, msg

            # 从数据库数据cluster_dict取ignore_reset_cmd_return_code参数
            # if config.getint('ignore_reset_cmd_return_code') == 0:
            if cluster_dict['ignore_reset_cmd_return_code'] == 0:
                if err_code != 0:
                    msg = f"{pre_msg}: reset host({bad_db_host}) failed"
                    task_log_info(task_id, msg)
                    general_task_mgr.complete_task(task_id, -1, msg)
                    return -1, msg

    except Exception as e:
        is_failed = True
        msg = f"{pre_msg}: run ({reset_cmd}) in host({bad_db_host}) failed: except error {repr(e)}"
        task_log_info(task_id, msg)
        return -1, msg
    finally:
        if is_failed:
            general_task_mgr.complete_task(task_id, -1, msg)
            bad_db_dict['state'] = node_state.FAULT
            dao.set_cluster_db_state(cluster_id, db_id, node_state.FAULT)

    # 刚reset机器之后，等2秒钟会再继续
    time.sleep(2)

    # 保存修改
    try:
        bad_db_dict['is_primary'] = 0
        new_pri_pg['is_primary'] = 1

        # 修改新旧主库的信息
        dao.update_up_db_id(new_pri_pg['db_id'], db_id, is_primary=0)
        dao.update_up_db_id('null', new_pri_pg['db_id'], is_primary=1)

        # 更新新主库的polar_type, 更新失败的话需要手动修改
        err_code, err_msg = polar_lib.update_polar_type(new_pri_pg["db_id"], "master")
        if err_code != 0:
            task_log_info(task_id, f"(db_id: {new_pri_pg['db_id']}): {err_msg}")
        # 更新旧主库的polar_type, 更新失败的话需要手动修改
        err_code, err_msg = polar_lib.update_polar_type(db_id, "reader")
        if err_code != 0:
            task_log_info(task_id, f"(db_id: ({db_id}): {err_msg}")

        task_log_info(task_id, f"{pre_msg}: save node state to meta server completed.")
    except Exception as e:
        msg = f"{pre_msg}: Unexpected error occurred during save node state to meta server: {repr(e)}"
        task_log_error(task_id, msg)
        task_log_info(task_id, f"{pre_msg} failed!!!")
        return -1, msg
    finally:
        bad_db_dict['state'] = node_state.FAULT
        dao.set_cluster_db_state(cluster_id, db_id, node_state.FAULT)

    try:
        need_restart_db = []
        # 如果不保留级联关系，则把需要把其它备库都指向新的主库
        if not cluster_dict.get('failover_keep_cascaded', False):
            task_log_info(task_id, f"{pre_msg}: change all standby database upper level primary database to host({new_pri_pg['host']})...")
            for p in clu_db_list:
                if p['db_id'] != db_id and (not p['is_primary']) and p['state'] == node_state.NORMAL:
                    need_restart_db.append(p)
            task_log_info(task_id, f"{pre_msg}: "
                    f"change all standby database upper level primary database to host({new_pri_pg['host']}) completed.")
        # 如果保留级联关系，则把旧主库指向新主库,当前没有这项设置
        else:
            # 获取坏掉主库的一级备库
            lower_rows = dao.get_lower_db(db_id)
            # 将坏掉的主库的下级备库都接到新的主库上面
            if lower_rows:
                # msg = ""
                for db in lower_rows:
                    err_code, err_msg, data = pg_helpers.check_sr_conn(new_pri_pg['db_id'], db['db_id'])
                    if err_code == 0 and data.get('cnt') == 1:
                        continue
                    need_restart_db.append(db)

        for db_dict in need_restart_db:
            # stop polarDB
            task_log_info(task_id, f"{pre_msg}: begin stopping database host: ({db_dict['host']})...")
            db_dict['wait_time'] = 5
            err_code, err_msg = pg_db_lib.stop(db_dict['host'], db_dict['pgdata'])
            if err_code != 0:
                return -1, err_msg

            # stop pfs
            task_log_info(task_id, f"{pre_msg}: begin stopping  pfs host: ({db_dict['host']})...")
            polar_type_list = ['master', 'reader']
            polar_type = db_dict.get('polar_type', None)
            if polar_type in polar_type_list:
                err_code, err_msg = polar_lib.stop_pfs(db_dict['host'], db_dict['db_id'])
                task_log_error(task_id, f"stop pfs failed, {err_msg}.")

            # 此出调用的函数中会重启数据库，当前会让其重启失败，后面再做启动操作
            polar_lib.update_recovery(db_dict['db_id'], new_pri_pg['host'], new_pri_pg['port'])
            dao.update_up_db_id(new_pri_pg['db_id'], db_dict['db_id'], db_dict['is_primary'])

        # 把新主库的上级库设置为空
        dao.update_up_db_id('null', new_pri_pg['db_id'], 1)
    except Exception:
        msg = f"{pre_msg}: change all standby database upper level primary database to host({new_pri_pg['host']}) failed: " \
              f"Unexpected error occurred: {traceback.format_exc()}"
        task_log_error(task_id, msg)
        task_log_info(task_id, f"{pre_msg} failed!!!")
        return -1, msg
    finally:
        bad_db_dict['state'] = node_state.FAULT
        dao.set_cluster_db_state(cluster_id, db_id, node_state.FAULT)

    # 把选中的这个备库激活成主库
    task_log_info(task_id, f"{pre_msg}: Promote standby database({new_pri_pg['host']}) to primary...")
    err_code, err_msg = pg_db_lib.promote(new_pri_pg['host'], new_pri_pg['pgdata'])
    if err_code != 0:
        msg = f"{pre_msg}: promote standby database({new_pri_pg['host']}) to primary failed: {err_msg}"
        task_log_error(task_id, msg)
        task_log_info(task_id, f"{pre_msg} failed!!!")
        return -1, msg
    else:
        task_log_info(task_id, f"{pre_msg}: Promote standby database({new_pri_pg['host']}) to primary completed.")

    try:
        # 配置完成，开始启动数据库
        for db_dict in need_restart_db:
            task_log_info(task_id, f"{pre_msg}: begin starting database host:({db_dict['host']})...")
            polar_type_list = ['master', 'reader']
            polar_type = db_dict.get('polar_type', None)
            if polar_type in polar_type_list:
                err_code, err_msg = polar_lib.start_pfs(db_dict['host'], db_dict['db_id'])
                if err_code != 0 and err_code != 1:
                    return 400, err_msg

            err_code, err_msg = pg_db_lib.start(db_dict['host'], db_dict['pgdata'])
            if err_code != 0:
                return 400, err_msg
            dao.update_db_state(db_dict['db_id'], database_state.RUNNING)
            task_log_info(task_id, f"{pre_msg}: current database host: ({db_dict['host']}) is running.")
        # Test End

        # 更新集群的机房信息
        step = f"{pre_msg}: Update cluster(cluster_id: {cluster_id}) computer room information"
        task_log_info(task_id, step)
        err_code, err_msg = pg_helpers.update_cluster_room_info(cluster_id)
        if err_code != 0:
            msg = f"{step} Failed: {err_msg}"
            task_log_error(task_id, msg)
            return -1, msg

        step = f"{pre_msg}: Check and start new primary db, db_id: ({new_pri_pg['db_id']})."
        task_log_info(task_id, step)
        # err_code, err_msg = pg_helpers.set_primary(new_pri_pg['db_id'])
        # 检查新主库是否启动成功
        err_code, is_run = pg_db_lib.is_running(new_pri_pg['host'], new_pri_pg['pgdata'])
        if err_code == 0 and not is_run:
            polar_type_list = ['master', 'reader']
            polar_type = new_pri_pg.get('polar_type', None)
            if polar_type in polar_type_list:
                err_code, err_msg = polar_lib.start_pfs(new_pri_pg['host'], new_pri_pg['db_id'])
                if err_code != 0 and err_code != 1:
                    return err_code, err_msg
            pg_db_lib.start(new_pri_pg['host'], new_pri_pg['pgdata'])
        if err_code != 0:
            task_log_error(task_id, step)
        task_log_info(task_id, f"{step} Success")

        # 如果配置了切换时调用的数据库函数，则调用此函数，不管调用是否成功，都认为切换成功，只是打印了一个错误日志
        trigger_db_name = cluster_dict['trigger_db_name']
        trigger_db_func = cluster_dict['trigger_db_func']
        if trigger_db_name and trigger_db_func:
            task_log_info(task_id,
                f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary ...")

            db_user = clu_db_list[0]['db_user']
            db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])

            sql = "select {0}(%s, %s, %s, %s, %s);".format(trigger_db_func)
            err_code, err_msg = probe_db.exec_sql(
                new_pri_pg['host'], db_port, trigger_db_name, db_user, db_pass,
                sql,
                (2, pre_msg, bad_db_dict['host'], json.dumps(before_cluster_dict), json.dumps(cluster_dict)))

            if err_code != 0:
                task_log_error(task_id, f"{pre_msg}: trigger db({trigger_db_name}) function({trigger_db_func}) in primary error: {err_msg}")

        ret_msg = f"{pre_msg}: switch to new host({new_pri_pg['host']}) completed."
    except Exception:
        return -1, traceback.format_exc()
    return 0, ret_msg
