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
@description: RPC服务模块
"""

import copy
import logging
import threading

import cluster_state
import config
import csuapp
import dao
import dbapi
import general_task_mgr
import ha_mgr
import pg_db_lib
import pg_helpers
import task_type_def
import utils

import csurpc


class ServiceHandle:
    def __init__(self):
        pass

    @staticmethod
    def register_node(hostname, ip, mem_size, cpu_info, os_type):
        """
        注册clup-agent
        :return:
        """
        if isinstance(cpu_info, int):  # 这是旧版本的agent
            cpu_cores = cpu_info
            cpu_threads = 0
        elif isinstance(cpu_info, dict):
            cpu_cores, cpu_threads = utils.get_cpu_cores(cpu_info)

        hid = dao.register_host(ip, hostname, mem_size, cpu_cores, cpu_threads, os_type)
        ret_dict = {
            'hid': hid,
            'agent_rpc_port': config.getint('agent_rpc_port'),
            # 'agent_ws_port': config.getint('agent_ws_port'),
            # 'agent_ws_token': config.get('agent_ws_token'),
        }
        return 0, ret_dict

    @staticmethod
    def get_all_cluster():
        cluster_list = dao.get_all_cluster()
        for item in cluster_list:
            if item['cluster_type'] == 2:
                err_code, running_on_host1 = pg_db_lib.is_running(item['host1_ip'], item['pgdata'])
                if err_code == 0 and running_on_host1:
                    item['host1_ip'] = f"*{item['host1_ip']}"
                err_code, running_on_host2 = pg_db_lib.is_running(item['host2_ip'], item['pgdata'])
                if err_code == 0 and running_on_host2:
                    item['host2_ip'] = f"*{item['host2_ip']}"
        return 0, cluster_list

    @staticmethod
    def get_cluster_with_db_list(cluster_id):
        cluster = dao.get_cluster_with_db_list(cluster_id)
        if cluster is None:
            return -1, f"cluster(id={cluster_id}) not exists"

        if cluster['cluster_type'] == 2:
            err_code, running_on_host1 = pg_db_lib.is_running(
                cluster['host1_ip'], cluster['pgdata'])
            if err_code == 0 and running_on_host1:
                cluster['host1_ip'] = f"*{ cluster['host1_ip']}"
            err_code, running_on_host2 = pg_db_lib.is_running(
                cluster['host2_ip'], cluster['pgdata'])
            if err_code == 0 and running_on_host2:
                cluster['host2_ip'] = f"*{cluster['host2_ip']}"

        return 0, cluster

    @staticmethod
    def get_last_lsn(cluster_id):
        cluster = dao.get_cluster(cluster_id)
        if cluster is None:
            return -1, f"cluster(id={cluster_id}) not exists"

        cluster_type = cluster['cluster_type']
        if cluster_type != 1:
            return -1, f"This cluster(id={cluster_id}) is not streaming replication cluster!"
        return ha_mgr.get_last_lsn(cluster_id)

    @staticmethod
    def get_repl_delay(cluster_id):
        cluster = dao.get_cluster(cluster_id)
        if cluster is None:
            return -1, f"cluster(id={cluster_id}) not exists"

        cluster_type = cluster['cluster_type']
        if cluster_type != 1:
            return -1, f"This cluster(id={cluster_id}) is not streaming replication cluster!"
        return ha_mgr.get_repl_delay(cluster_id)


    @staticmethod
    def online(cluster_id):
        err_code, err_list = ha_mgr.online(cluster_id)
        return err_code, err_list

    @staticmethod
    def offline(cluster_id):
        err_code, err_msg = ha_mgr.offline(cluster_id)
        return err_code, err_msg

    @staticmethod
    def failback(cluster_id, db_id):
        data = general_task_mgr.get_running_task()
        if len(data) > 0:
            return 1, "There are other operations in progress, please try again later."

        ret = dao.test_and_set_cluster_state(cluster_id, [cluster_state.NORMAL, cluster_state.OFFLINE, cluster_state.FAILED], cluster_state.REPAIRING)
        if ret is None:
            err_msg = f"cluster({cluster_id}) state is failover or in repairing, can not repair now!"
            logging.info(err_msg)
            return -1, (-1, err_msg)

        # 因为是一个长时间运行的操作，所以生成一个后台任务，直接返回
        task_name = f"failback {cluster_id}(dbid={db_id})"
        task_id = general_task_mgr.create_task(task_type_def.FAILBACK, task_name, {'cluster_id': cluster_id})
        pdict = {
            'cluster_id': cluster_id,
            'db_id': db_id,
            # 'up_db_id': dao.get_primary_db(db_id)[0]['db_id']
        }

        general_task_mgr.run_task(task_id, ha_mgr.failback, (pdict,))
        return 0, (task_id, task_name)

    @staticmethod
    def sr_switch(cluster_id, db_id, primary_db):
        data = general_task_mgr.get_running_task()
        if len(data) > 0:
            return 1, "There are other operations in progress, please try again later."

        cluster = dao.get_cluster(cluster_id)
        if cluster is None:
            return -1, f"cluster(id={cluster_id}) not exists"

        cluster_type = cluster['cluster_type']
        if cluster_type != 1:
            return -1, f"This cluster(id={cluster_id}) is not streaming replication cluster!"

        state = cluster['state']
        if state != cluster_state.NORMAL and state != cluster_state.OFFLINE:  # 不是正常或离线状态，不能切换
            return -1, f"cluster(id={cluster_id}) state is not Normal or offline, can not switch!"

        # 先检测是否可以切换，如果不能，直接返回
        ret_code, msg = ha_mgr.test_sr_can_switch(cluster_id, db_id, primary_db)
        if ret_code != 0:
            return -1, msg

        # 因为是一个长时间运行的操作，所以生成一个后台任务，直接返回
        task_name = f"sr_switch {cluster_id}(dbid={db_id})"
        task_id = general_task_mgr.create_task(task_type_def.SWITCH, task_name, {'cluster_id': cluster_id})
        general_task_mgr.run_task(task_id, ha_mgr.sr_switch, (cluster_id, db_id, primary_db))
        return 0, (task_id, task_name)

    @staticmethod
    def change_meta(cluster_id, key, value):
        return ha_mgr.change_meta(cluster_id, key, value)

    @staticmethod
    def get_meta(cluster_id):
        cluster_dict = dao.get_cluster(cluster_id)
        if cluster_dict is None:
            return -1, "Cluster does not exist."
        if cluster_dict['cluster_type'] == 1:
            clu_db_list = dao.get_cluster_db_list(cluster_id)
            cluster_dict['db_list'] = clu_db_list
        return 0, cluster_dict

    @staticmethod
    def get_task_log(task_id, get_task_state=False, seq=0):
        return general_task_mgr.get_task_log(task_id, get_task_state, seq)

    @staticmethod
    def get_task_list(page, limit):
        conds = {}
        total_count, task_list_data = general_task_mgr.get_task_list(conds, page, limit)
        return 0, total_count, task_list_data

    @staticmethod
    def remove_cluster(cluster_id):
        state = dao.get_cluster_state(cluster_id)
        if state is None:
            return -1, f'cluster({cluster_id}) not exists!'
        if state != cluster_state.OFFLINE and state != cluster_state.FAILED:
            return -1, f"Can not delete cluster that state is {cluster_state.to_str(state)}!"
        with dbapi.DBProcess() as dbp:
            dbp.execute("delete from clup_db WHERE cluster_id=%s", (cluster_id,))
            dbp.execute("delete from clup_cluster WHERE cluster_id=%s", (cluster_id,))
        return 0, ''

    @staticmethod
    def task_insert_log(task_id, task_state, msg, task_type):
        task = pg_helpers
        if task_type != 'general':
            task = general_task_mgr

        if task_state == -2:
            task.log_warn(task_id, msg)
        elif task_state == 0:
            task.log_info(task_id, msg)
        elif task_state == -1:
            task.log_error(task_id, msg)


    @staticmethod
    def get_clup_config(key, val=None):
        conf = config.get(key, val)
        return 0, conf


    @staticmethod
    def clup_reload():
        return config.reload()


    @staticmethod
    def complete_task(task_id, task_state, msg, callback_args):
        """
        设置task状态
        task state 1: 成功, 0: 运行中, -1: 失败, -2 警告
        """
        try:
            err_code, err_msg = general_task_mgr.complete_task(task_id=task_id, state=task_state, msg=msg, callback_args=callback_args)
            return err_code, err_msg
        except Exception as e:
            logging.error(f"complete task({task_id}) failed: {repr(e)}")
            return -1, repr(e), ""

    @staticmethod
    def get_log_level(logger_name):
        try:
            tmp_logger = logging.getLogger(logger_name)
            return 0, tmp_logger.level
        except Exception as e:
            return -1, repr(e)


    @staticmethod
    def set_log_level(logger_name, level):
        try:
            tmp_logger = logging.getLogger(logger_name)
            tmp_logger.setLevel(level)
            return 0, ''
        except Exception as e:
            return -1, repr(e)


def run_service():
    try:
        handle = ServiceHandle()
        srv = csurpc.Server('ha-service', handle, csuapp.is_exit,
                password=config.get('internal_rpc_pass'), thread_count=10, debug=1)
        server_rpc_port = config.get('server_rpc_port')
        srv.bind(f"tcp://0.0.0.0:{server_rpc_port}")
        srv.run()
    except Exception as e:
        logging.error(f"rpc service stopped: unexpected error occurred: {str(e)}")


def start():

    t = threading.Thread(target=run_service, args=(), name='rpc_server_main')
    t.setDaemon(True)  # 设置线程为后台线程
    t.start()
