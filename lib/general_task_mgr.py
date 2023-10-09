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
@description: 后台任务管理模块
"""

import json
import logging
import threading
import traceback

import dbapi
import task_type_def

__callback_dict = {}
__lock = threading.Lock()

DBMGR_TASK = 1
CBU_TASK = 2
HA_TASK = 3


def get_task_type_list_by_class(class_type: int) -> list:
    if class_type == 1:
        return [task_type_def.PG_BUILD_STANDBY_TASK, task_type_def.PG_CREATE_INSTANCE_TASK]
    elif class_type == 2:
        return [task_type_def.PG_FULL_BACKUP_TASK, task_type_def.PG_RECOVERY_TASK, task_type_def.PG_COMMON_BACKUP_TASK]
    elif class_type == 3:
        return [task_type_def.FAILOVER, task_type_def.CREATE_SR_CLUSTER, task_type_def.FAILBACK, task_type_def.SWITCH]
    elif class_type == 4:
        return [task_type_def.CLUP_CHECK]
    else:
        return []



def register_task_callback(task_type, complete_callback):
    """
    注册某个任务类型的回调函数
    """

    __lock.acquire()
    __callback_dict[task_type] = complete_callback
    __lock.release()


def create_task(task_type: str, task_name: str, task_data: dict) -> int:
    global __callback_dict
    global __lock

    sql = "INSERT INTO clup_general_task(state, task_type, task_name, task_data)" \
          " values(%s, %s, %s, %s) RETURNING task_id"
    rows = dbapi.query(sql, (0, task_type, task_name, json.dumps(task_data)))
    task_id = rows[0]['task_id']
    return task_id


def run_task(task_id, run_func, args: tuple) -> None:
    new_args = list(args)
    new_args.insert(0, task_id)
    t = threading.Thread(target=run_func, args=new_args)
    t.setDaemon(True)  # 设置线程为后台线程
    t.start()


def get_running_task():
    sql = "SELECT task_id, state, task_type, task_name, create_time FROM clup_general_task WHERE state = 0"
    rows = dbapi.query(sql)
    return rows


def get_task_list(conds: dict, page, limit, task_class):
    """[获取任务列表]

    Args:
        conds (dict): 查询条件
        page ([type]): 第几页
        limit ([type]): 每页多少行
        task_class (int, optional): [TASK大类: 1为数据库管理，2为备份管理，3为HA]
    Returns:
        [int]: [总行数]
        [list]: [返回的各行数据]
    """

    task_type_list = get_task_type_list_by_class(task_class)
    single_quote_task_type_list = [f"'{k}'" for k in task_type_list]
    in_cond = ",".join(single_quote_task_type_list)

    if "search_key" in conds and conds["search_key"] == "%":
        del conds["search_key"]

    where = f' WHERE task_type in ({in_cond})'
    binds = []
    for k in conds:
        where += ' and '
        if k == 'begin_create_time':
            where += 'create_time >= %s'
        elif k == 'end_create_time':
            where += 'create_time <= %s'
        elif k == 'cluster_id':
            where += "(task_data->>'cluster_id')::int = %s"
        elif k == 'search_key':
            where += " (task_name like %s OR last_msg like %s) "
            binds.append(conds[k])
        else:
            where += '{col_name}=%s'.format(col_name=k)
        binds.append(conds[k])
    total_count_sql = "SELECT count(*) as cnt FROM clup_general_task " + where
    if len(where) == 0:
        rows = dbapi.query(total_count_sql)
    else:
        rows = dbapi.query(total_count_sql, tuple(binds))
    total_count = rows[0]['cnt']

    sql = "SELECT task_id, state,  task_data->>'cluster_id' as cluster_id,to_char(create_time, 'YYYY-MM-DD HH24:MI:SS') as create_time, " \
          " task_type, task_name,  last_msg FROM clup_general_task " + where + \
          " ORDER BY create_time DESC limit " + str(limit) + " offset " + str((int(page) - 1) * int(limit))
    if len(where) == 0:
        rows = dbapi.query(sql)
    else:
        rows = dbapi.query(sql, tuple(binds))
    return total_count, rows


def complete_task(task_id, state, msg, callback_args=dict()):
    global __callback_dict
    global __lock

    complete_callback = None
    err_code = 0
    err_msg = ''
    try:
        sql = "SELECT task_type FROM clup_general_task WHERE task_id = %s"
        rows = dbapi.query(sql, (task_id, ))
        if len(rows) == 0:
            err_msg = f"receive agent complete task({task_id}), but task not exists!"
            logging.error(err_msg)
            return -1, err_msg
        task_type = rows[0]['task_type']
        __lock.acquire()
        try:
            if task_type in __callback_dict:
                complete_callback = __callback_dict[task_type]
        finally:
            __lock.release()

        if complete_callback:
            try:
                err_code, err_msg = complete_callback(callback_args)
                if err_code != 0:
                    err_msg = f"call task callback func failed: \n\ttask_id={task_id}\n\tstate={state}\n\tcallback_args={repr(callback_args)}\n\tmsg={msg}.\n\t======detail=====\n\t{err_msg}"
                    logging.error(err_msg)
            except Exception:
                err_msg = f"call task callback func failed:\n\ttask_id={task_id}\n\tstate={state}\n\tcallback_args={repr(callback_args)}\n\tmsg={msg}.\n\t======detail=====\n\t{traceback.format_exc()}"
                logging.error(err_msg)
                err_code = -1

        with dbapi.DBProcess() as dbp:
            if state == 1:
                log_level = 0
            else:
                log_level = -2
            sql = "INSERT INTO clup_general_task_log(task_id, log_level, log) values(%s, %s, %s) RETURNING seq"
            dbp.execute(sql, (task_id, log_level, msg))
            sql = "UPDATE clup_general_task SET state = %s, last_msg = %s WHERE state = 0 AND task_id=%s"
            dbp.execute(sql, (state, msg, task_id))
            dbp.commit()
        return err_code, err_msg
    except Exception as e:
        err_code = -1
        err_msg = repr(e)
        logging.error(f"complete task unexpect error: task_id={task_id}), state={state}, callback_args={repr(callback_args)}, msg={msg}.\n{traceback.format_exc()}")
        return err_code, err_msg


def get_task_log(task_id, get_task_state=False, seq=0):
    sql = "SELECT state FROM clup_general_task WHERE task_id = %s"
    state = 0
    if get_task_state:
        rows = dbapi.query(sql, (task_id,))
        if len(rows) < 1:
            return 1, -1, f"task({task_id}) not exists!"
        state = rows[0]['state']
    sql = "SELECT seq, log_level, log, to_char(create_time, 'YYYY-MM-DD HH24:MI:SS') as create_time" \
          "  FROM clup_general_task_log WHERE task_id=%s and seq > %s ORDER BY seq"
    rows = dbapi.query(sql, (task_id, seq))
    return 0, state, rows


def get_general_task_state(task_id):
    sql = "SELECT state FROM clup_general_task WHERE task_id = %s"
    rows = dbapi.query(sql, (task_id,))
    if len(rows) < 1:
        return -1, f"task({task_id}) not exists!"
    state = rows[0]['state']
    return 0, state


def log_info(task_id, msg):
    write_log(task_id, 0, msg)


def log_warn(task_id, msg):
    write_log(task_id, -1, msg)


def log_error(task_id, msg):
    write_log(task_id, -2, msg)


def log_fatal(task_id, msg):
    write_log(task_id, -3, msg)


def log_debug(task_id, msg):
    write_log(task_id, 1, msg)


def write_log(task_id, log_level, msg):
    sql = "INSERT INTO clup_general_task_log(task_id, log_level, log) values(%s, %s, %s) RETURNING seq"
    rows = dbapi.query(sql, (task_id, log_level, msg))
    seq = rows[0]['seq']
    return seq
