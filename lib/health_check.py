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
@description: 健康检查模块
"""

import json
import logging
import threading
import time
import traceback
import urllib.error
import urllib.request

import cluster_state
import config
import csuapp
import dao
import database_state
import db_encrypt
import ha_logic
import ha_mgr
import helpers
import lb_mgr
import node_state
import pg_db_lib
import pg_helpers
import probe_db
import rpc_utils

ASYNC_CLUSTER_LIST = []
SYNC_CLUSTER_LIST = []
FAILBACK_DB_LIST = []


def sr_switch_read_vip(read_vip_host, read_vip, cluster_id):
    rows = dao.get_cluster_db_host_list(cluster_id)
    host_list = [row['host'] for row in rows if row['is_primary'] == 0]
    primary_host_list = [row['host'] for row in rows if row['is_primary'] == 1]
    # 删除掉当前不能连接的host
    # 将主库ip放到最后，如果所有备库都连接不上，就放到主库上
    host_list.extend(primary_host_list)
    if read_vip_host in host_list:
        host_list.remove(read_vip_host)
    for host in host_list:
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            continue
        rpc = err_msg
        try:
            logging.info(f'check and add vip({read_vip}) to host({host})')
            err_code, err_msg = rpc.check_and_add_vip(read_vip)
            if err_code < 0:
                logging.error(f"Cluster({cluster_id}): Can not check and add read vip({read_vip}) in host({read_vip_host}): {err_msg}")
        finally:
            rpc.close()

        # 更新数据库中read_vip_host
        dao.set_new_read_vip_host(cluster_id, host)
        return 0

    # 如果都连接不上就返回-1
    return -1


def sr_check_del_read_vip(cluster_id):
    vip = dao.get_cluster_vip(cluster_id)
    if not vip['read_vip']:
        return
    rows = dao.get_cluster_db_host_list(cluster_id)
    host_list = [row['host'] for row in rows]
    if vip['read_vip_host'] in host_list:
        host_list.remove(vip['read_vip_host'])
    read_vip = vip['read_vip']
    for host in host_list:
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            continue
        rpc = err_msg
        try:
            err_code, ret = rpc.vip_exists(read_vip)
            if err_code != 0 or not ret:
                # 如果连接失败，直接跳过
                continue
            # 如果存在，则删除
            logging.info(f'remove read vip({read_vip}) from host ({host})')
            rpc_utils.check_and_del_vip(host, read_vip)
        finally:
            rpc.close()


def sr_check_del_write_vip(cluster_id):
    primary_host = dao.get_primary_host(cluster_id)
    host_list = dao.get_cluster_db_host_list(cluster_id)
    vip_detail = dao.get_cluster_vip(cluster_id)
    room = pg_helpers.get_current_cluster_room(cluster_id)
    vip = vip_detail['vip']
    for db in host_list:
        host = db['host']
        if host == primary_host.get('host') or room['room_id'] != db['room_id']:
            continue
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            continue
        rpc = err_msg
        try:
            err_code, ret = rpc.vip_exists(vip)
            if err_code != 0 or not ret:
                # 如果连接失败，直接跳过
                continue
            # 如果存在，则删除
            logging.info(f'remove write vip({vip}) from host ({host})')
            rpc.check_and_del_vip(vip)
        finally:
            rpc.close()


def get_count_db(cluster_id):
    # 统计集群中剩余多少个正常的数据库
    count = 0
    rows = dao.get_cluster_db(cluster_id)
    for row in rows:
        if row['db_state'] == database_state.RUNNING:
            # 如果是running状态就跳过，减少查询次数
            count += 1
            continue
        err_code, is_run = pg_db_lib.is_running(row['host'], row['pgdata'])
        if err_code == 0 and is_run:
            count += 1
    return count


def sr_check_count_db(cluster_id):
    # 如果不在异步集群列表中，检测集群中只剩一个正常数据库，将集群改为异步模式，集群id添加到异步列表中，避免下一次再检测
    if cluster_id not in ASYNC_CLUSTER_LIST:
        count = get_count_db(cluster_id)
        primary_info = dao.get_primary_info(cluster_id)
        if not primary_info:
            logging.error(f'No primary database found in the cluster({cluster_id}).')
            return
        err_code, err_msg = rpc_utils.get_rpc_connect(primary_info['host'])
        if err_code != 0:
            return
        rpc = err_msg
        try:
            if count == 1:
                logging.info(f'Statistics normal database (count={count})')
                # 如果只剩一个主库正常，改为异步模式，并且添加到列表中
                logging.info(f"Because only one survived, to avoid hang, (db_id: {primary_info['db_id']}) change sync to async")
                err_code, ret = pg_db_lib.change_sync_to_async(rpc, primary_info['pgdata'])
                if err_code:
                    logging.error(f"(db_id: {primary_info['db_id']}) change sync to async ERROR: {ret}")
                err_code, err_msg = pg_db_lib.reload(primary_info['host'], primary_info['pgdata'])
                if err_code != 0:
                    logging.error(f"(db_id: {primary_info['db_id']}) change sync to async reload ERROR: {err_msg}")
                ASYNC_CLUSTER_LIST.append(cluster_id)
                if cluster_id in SYNC_CLUSTER_LIST:
                    SYNC_CLUSTER_LIST.remove(cluster_id)
        finally:
            rpc.close()


def sr_check_async_to_sync(cluster_id):
    # 如果不在异步集群列表中，检测集群中只剩一个正常数据库，将集群改为异步模式，集群id添加到异步列表中，避免下一次再检测
    if cluster_id not in SYNC_CLUSTER_LIST:
        primary_info = dao.get_primary_info(cluster_id)
        count = get_count_db(cluster_id)
        err_code, err_msg = rpc_utils.get_rpc_connect(primary_info['host'])
        if err_code != 0:
            return
        rpc = err_msg
        try:
            if count > 1:
                logging.info(f"If sr is sync, (db_id: {primary_info['db_id']}) restore from async to sync")
                err_code, ret = pg_db_lib.change_async_to_sync(rpc, primary_info['pgdata'])
                rpc.close()
                if err_code:
                    logging.error(f"(db_id: {primary_info['db_id']}) restore from async to sync ERROR: {ret}")
                err_code, err_msg = pg_db_lib.reload(primary_info['host'], primary_info['pgdata'])
                if err_code != 0:
                    logging.error(f"(db_id: {primary_info['db_id']}) restore from async to sync reload ERROR: {err_msg}")
                SYNC_CLUSTER_LIST.append(cluster_id)
                if cluster_id in ASYNC_CLUSTER_LIST:
                    ASYNC_CLUSTER_LIST.remove(cluster_id)
        finally:
            rpc.close()


def sr_ha_check_vip(cluster_dict, clu_db_list):
    cluster_id = cluster_dict['cluster_id']
    vip = cluster_dict['vip'].strip()
    read_vip = cluster_dict['read_vip'].strip()
    read_vip_host = cluster_dict['read_vip_host'].strip()

    # 检查vip
    for db_dict in clu_db_list:
        host = db_dict['host']
        if db_dict['is_primary'] and db_dict['state'] == node_state.NORMAL:
            err_code, err_msg = rpc_utils.get_rpc_connect(host)
            if err_code != 0:
                logging.error(f"Cluster({cluster_id}): Can not check and add vip({vip}) in host({host}): maybe host is down.")
                continue
            rpc = err_msg
            try:
                err_code, ret = rpc.vip_exists(vip)
                if err_code == 0 and ret:
                    continue
                logging.info(f"Cluster({cluster_id}): VIP({vip}) needs to be added on host({host})")
                err_code, err_msg = rpc.check_and_add_vip(vip)
                if err_code < 0:
                    logging.error(f"Cluster({cluster_id}): Can not check and add vip({vip}) in host({host}): {err_msg}")
                    continue
            finally:
                rpc.close()

    if read_vip:  # 如果设置了读vip，则进行检查
        err_code, err_msg = rpc_utils.get_rpc_connect(read_vip_host)
        if err_code != 0:
            logging.error(f"Cluster({cluster_id}): Can not check and add read vip({read_vip}) in host({read_vip_host}): maybe host is down.")
            logging.info(f"Cluster({cluster_id}): read vip({read_vip}) needs to be switched to other host({read_vip_host})")
            err = sr_switch_read_vip(read_vip_host, read_vip, cluster_id)
            if err != 0:
                logging.error(f"Cluster({cluster_id}): All the databases are broken.")
        rpc = err_msg
        try:
            err_code, ret = rpc.vip_exists(read_vip)
            if err_code != 0 or ret:
                # 如果连接失败或者存在read vip，直接返回
                return

            logging.info(f"Cluster({cluster_id}): read vip({read_vip}) needs to be added on host({read_vip_host})")
            err_code, err_msg = rpc.check_and_add_vip(read_vip)
            if err_code < 0:
                logging.error(f"Cluster({cluster_id}): Can not check and add read vip({read_vip}) in host({read_vip_host}): {err_msg}")
        finally:
            rpc.close()


def sr_ha_check_cstlb(cluster_dict, clu_db_list):
    cluster_id = cluster_dict['cluster_id']
    db_port = cluster_dict['port']
    str_cstlb_list = cluster_dict['cstlb_list']
    if not str_cstlb_list:
        return
    cstlb_list = str_cstlb_list.split(',')
    if len(cstlb_list) <= 0:
        return

    res_clupstb = []
    for db in clu_db_list:
        if db['is_primary'] != 1 and db['state'] == node_state.NORMAL:
            ip_port = db['host'] + ':' + str(db_port)
            res_clupstb.append(ip_port)
    token = config.get("cstlb_token")
    for lb_addr in cstlb_list:
        try:
            response = urllib.request.urlopen('http://' + lb_addr + '/backend/list?token=' + token)
        except ConnectionRefusedError as e:
            logging.error(f"Cluster({cluster_id}): Unable to connect to the load balancer: {lb_addr}: \n {str(e)}")
            continue
        except urllib.error.URLError as e:
            logging.error(f"Cluster({cluster_id}): Unable to connect to the load balancer: {lb_addr}: \n {str(e)}")
            continue
        except Exception:
            logging.error(f"Cluster({cluster_id}): Unexpected error occurred during check csltb: {traceback.format_exc()}")
            continue

        res_cstlbstb = json.loads(response.read()).keys()
        diff = list(set(res_cstlbstb) ^ set(res_clupstb))
        if len(diff) == 0:
            continue

        cstlb_del = [x for x in diff if x not in res_clupstb]
        cstlb_add = [x for x in diff if x not in res_cstlbstb]

        logging.info(f'need delete {cstlb_del} from {lb_addr}')
        for backend_addr in cstlb_del:
            status, data = lb_mgr.delete_backend(lb_addr, backend_addr)
            if status != 0:
                logging.error(data)

        logging.info(f'need add {cstlb_add} to {lb_addr}')
        for backend_addr in cstlb_add:
            status, data = lb_mgr.add_backend(lb_addr, backend_addr)
            if status != 0:
                logging.error(data)


def probe_postgres_db(cluster_id, host, db_port, db_name, db_user, db_pass, sql, timeout, retry_interval, retry_cnt):
    msg = ''
    i = 0
    err_msg_list = []
    cluster = dao.get_cluster_name(cluster_id)
    cluster = cluster.get('cluster_name', cluster_id)
    while i < retry_cnt:
        err_code, err_msg = probe_db.probe_postgres(host, db_port,
                    db_name, db_user, db_pass, sql, timeout)
        if err_code != 0:
            current_time_str = helpers.get_current_time_str()
            msg = f"{current_time_str} ProbeDB[cluster_id={cluster_id}, db={host}:{db_port}]: {i + 1} time error: {err_msg}"
            err_msg_list.append(msg)
            logging.info(msg)

        else:
            break
        time.sleep(retry_interval)
        i += 1
    if i < retry_cnt:
        return 0, err_msg_list

    return -1, err_msg_list


# 流复制的HA检查类
class SrHaChecker(threading.Thread):
    def __init__(self, cluster_id):
        threading.Thread.__init__(self, name=f"health-checker-{cluster_id}")
        self.cluster_id = cluster_id

    def run(self):
        while not csuapp.is_exit():
            probe_interval = int(config.get('sr_ha_check_interval', 10))
            try:
                # 先把集群设置为checking状态，防止在检查过程中对集群有其他并发操作
                ret = dao.test_and_set_cluster_state(self.cluster_id, [cluster_state.NORMAL], cluster_state.CHECKING)
                if ret is None:
                    logging.debug(f"cluster({self.cluster_id}) state is not online, next time to check...")
                    time.sleep(probe_interval)
                    continue
            except Exception:
                err_msg = traceback.format_exc()
                logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during set cluster state to  CHECKING: {err_msg}")
                time.sleep(probe_interval)
                continue

            # check the database state which in the cluster
            clu_state = cluster_state.NORMAL
            try:
                cluster_dict = dao.get_cluster(self.cluster_id)
                if cluster_dict is None:
                    logging.info(f"exit health check thread when cluster({self.cluster_id}) has been deleted.")
                    break

                state = cluster_dict['state']
                if state != cluster_state.CHECKING:  # 不是CHECKING状态，则不进行检测
                    logging.debug(f"cluster({self.cluster_id}) state is {state}, not normal, next time to check...")
                    continue

                clu_db_list = dao.get_cluster_db_list(self.cluster_id)
                if len(clu_db_list) == 0:
                    logging.debug(f"exit health check thread when cluster({self.cluster_id}) has been deleted")
                    break

                # 检查数据库是否正常
                try:
                    db_port = cluster_dict['port']
                    cluster_id = cluster_dict['cluster_id']
                    probe_timeout = cluster_dict['probe_timeout']
                    probe_pri_sql = cluster_dict['probe_pri_sql']
                    probe_stb_sql = cluster_dict['probe_stb_sql']
                    probe_interval = cluster_dict['probe_interval']

                    for pg in clu_db_list:
                        host = pg['host']
                        is_primary = pg['is_primary']

                        # aready is failed, not check
                        if pg['state'] != node_state.NORMAL:
                            continue

                        probe_db_name = cluster_dict['probe_db_name']
                        db_user = clu_db_list[0]['db_user']
                        db_pass = db_encrypt.from_db_text(clu_db_list[0]['db_pass'])

                        if 'probe_retry_cnt' not in cluster_dict:
                            probe_retry_cnt = 2
                        else:
                            probe_retry_cnt = cluster_dict['probe_retry_cnt']

                        if 'probe_retry_interval' not in cluster_dict:
                            probe_retry_interval = 4
                        else:
                            probe_retry_interval = cluster_dict['probe_retry_interval']

                        if is_primary:
                            probe_sql = probe_pri_sql
                        else:
                            probe_sql = probe_stb_sql

                        try:
                            ret_code, err_msg_list = probe_postgres_db(
                                self.cluster_id, host, db_port,
                                probe_db_name, db_user, db_pass, probe_sql,
                                probe_timeout, probe_retry_interval, probe_retry_cnt
                            )
                            if ret_code == 0:
                                continue
                        except Exception:
                            logging.error(f"Cluster({self.cluster_id}): Probe db exception: {traceback.format_exc()}")
                            continue

                        try:
                            logging.info(f"Cluster({self.cluster_id}): Find database({host}:{db_port}) failed, begin failover ...")
                            # 如果有数据库不正常，尝试恢复，并且会更改集群状态
                            err_code, err_msg = ha_logic.failover_sr_cluster(self.cluster_id, pg, db_port, err_msg_list)
                            if err_code < 0:
                                clu_state = cluster_state.FAILED
                        except Exception:
                            err_code = -1
                            clu_state = cluster_state.FAILED
                            logging.error(traceback.format_exc())
                except Exception:
                    logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check db: {traceback.format_exc()}")

                # 检查vip
                try:
                    sr_ha_check_vip(cluster_dict, clu_db_list)
                except Exception:
                    logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check vip: {traceback.format_exc()}")
                # 检查并删除重复只读vip
                try:
                    sr_check_del_read_vip(self.cluster_id)
                except Exception:
                    logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check and del vip: {traceback.format_exc()}")
                # 检查并删除重复写vip
                try:
                    sr_check_del_write_vip(self.cluster_id)
                except Exception:
                    logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check and del vip: {traceback.format_exc()}")
                # 检查只剩一个主库正常的情况
                try:
                    # 检查只剩下一个数据库的集群并改为异步模式
                    sr_check_count_db(self.cluster_id)
                    # 如果之前只剩一个数据库的集群有数据库恢复接改为同步模式
                    sr_check_async_to_sync(self.cluster_id)
                except Exception:
                    logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check and del vip: {traceback.format_exc()}")

                # 检查负载均衡
                try:
                    sr_ha_check_cstlb(cluster_dict, clu_db_list)
                except Exception:
                    logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check cstlb: {traceback.format_exc()}")

            except Exception:
                err_msg = traceback.format_exc()
                logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check database: {err_msg}")
            finally:
                dao.set_cluster_state(self.cluster_id, clu_state)
                time.sleep(probe_interval)


            # check and try add the database to cluster
            try:

                if not cluster_dict.get('auto_failback'):  # 如果集群没有设置自动加回的标志，则无需自动加回集群
                    time.sleep(probe_interval)
                    continue

                # 先把集群设置为checking状态，防止在检查过程中对集群有其他并发操作
                ret = dao.test_and_set_cluster_state(self.cluster_id, [cluster_state.NORMAL], cluster_state.CHECKING)
                if ret is None:
                    logging.debug(f"cluster({self.cluster_id}) state is not online, next time to check...")
                    time.sleep(probe_interval)
                    continue
            except Exception:
                err_msg = traceback.format_exc()
                logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during set cluster state to  CHECKING: {err_msg}")
                time.sleep(probe_interval)
                continue

            # check the database
            clu_state = cluster_state.NORMAL
            try:
                cluster_dict = dao.get_cluster(self.cluster_id)
                if cluster_dict is None:
                    logging.info(f"exit health check thread when cluster({self.cluster_id}) has been deleted")
                    break

                state = cluster_dict['state']
                clu_db_list = dao.get_cluster_db_list(self.cluster_id)
                if len(clu_db_list) == 0:
                    logging.debug(f"exit health check thread when cluster({self.cluster_id}) has been deleted")
                    break

                db_port = cluster_dict['port']
                cluster_id = cluster_dict['cluster_id']
                for pg in clu_db_list:
                    host = pg['host']
                    is_primary = pg['is_primary']

                    if pg['state'] == node_state.FAULT and cluster_dict.get('auto_failback') and pg['db_id'] not in FAILBACK_DB_LIST:
                        err_code, err_msg = rpc_utils.get_rpc_connect(pg['host'])
                        if err_code != 0:
                            continue
                        rpc = err_msg
                        rpc.close()

                        db_id = pg['db_id']
                        up_db_id = pg['up_db_id']
                        if not up_db_id:
                            primary = dao.get_primary_info(cluster_id)
                            if primary:
                                up_db_id = primary['db_id']
                            else:
                                continue
                        dao.set_cluster_state(cluster_id, cluster_state.REPAIRING)
                        ha_mgr.check_auto_failback(cluster_id, db_id, up_db_id, clu_state)
                        clu_state = None
                        break

            except Exception as e:
                logging.error(f"Cluster({self.cluster_id}): Unexpected error occurred during check database: {str(e)}")
            finally:
                if clu_state is not None:
                    # 把集群的状态恢复
                    dao.set_cluster_state(self.cluster_id, clu_state)
                    time.sleep(probe_interval)

        logging.info(f"ha cluster({self.cluster_id}) thread stoped")


class ClusterChangeChecker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, name="cluster-change-checker")

    def run(self):
        pre_cluster_list = []
        while not csuapp.is_exit():
            probe_interval = config.getint('db_cluster_change_check_interval')
            begin_time = int(time.time())
            # 用于计算线程休眠时间
            try:

                logging.debug("Begin get ha cluster list...")
                try:
                    cluster_list = dao.get_cluster_id_list()
                except Exception:
                    logging.debug("Maybe primary csumdb lost, try again later...")
                    time.sleep(probe_interval)
                    continue
                logging.debug(f"Get ha cluster list: {str(cluster_list)}")

                for cluster_id in cluster_list:
                    if cluster_id in pre_cluster_list:
                        continue
                    cluster_type = dao.get_cluster_type(cluster_id)
                    if cluster_type == 1:
                        db_checker = SrHaChecker(cluster_id)
                        db_checker.start()
                    # Leifliu Test
                    elif cluster_type == 11:
                        db_checker = SrHaChecker(cluster_id)
                        db_checker.start()
                    logging.info(f"ha cluster({cluster_id}) thread started")
                pre_cluster_list = cluster_list
            except Exception:
                logging.error(f"Cluster: Unexpected error occurred during check database: {traceback.format_exc()}")
            finally:
                last_time = int(time.time())
                sleep_secs = probe_interval - (last_time - begin_time)
                if sleep_secs > 0:
                    time.sleep(sleep_secs)


def start_check():
    # host_checker = HostChecker()
    # host_checker.start()
    # 启动一个检查是否有新增ha cluster的线程，如果发现有一个新的ha cluster，就启动一个新的线程服务这个ha cluster
    logging.info("Start new ha cluster checker thread...")
    cluster_changer_checker = ClusterChangeChecker()
    cluster_changer_checker.start()
    logging.info("new ha cluster checker thread started.")

    # 基于实例的检查
    # logging.info("begin start instance checker thread...")
    # instance_checker = InstanceChecker()
    # instance_checker.start()
    # logging.info("instance checker thread started.")


# 测试
if __name__ == "__main__":
    pass
