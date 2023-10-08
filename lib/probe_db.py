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
@description: PostgerSQL数据库探测模块
"""

import os
import json
import queue
import time
import uuid
import ctypes
import logging
import threading
import traceback
import multiprocessing

import psycopg2
import psycopg2.extras

import config
import logger
import pg_utils

g_q_request = None   # 进程间发送请求的队列
g_q_reply = None     # 进程间接收已完成的命令的队列

g_cmd_dict = {}
g_cmd_dict_lock = threading.Lock()



REQ_RUN_CMD = 1002
REQ_RECV_CMD_RESULT = 1003

CMD_TYPE_PROBE_PG = 2001
CMD_TYPE_EXEC_SQL = 2002
CMD_TYPE_RUN_SQL = 2003
CMD_TYPE_GET_LAST_LSN = 2004
CMD_TYPE_GET_LAST_WAL_FILE = 2005


def get_cmd_result(cmd_id):
    """[summary]

    Args:
        id ([int]): cmd_id

    Returns:
        run_is_over: 是否运行结束
        err_code:
        err_msg
    """
    global g_cmd_dict
    global g_cmd_dict_lock
    g_cmd_dict_lock.acquire()
    try:
        if cmd_id not in g_cmd_dict:
            return False, -1, f"not this cmd({cmd_id})"
        cmd_dict = g_cmd_dict[cmd_id]
        if cmd_dict['run_is_over']:
            del g_cmd_dict[cmd_id]
            return True, cmd_dict['err_code'], cmd_dict['err_msg']
        else:
            return False, 0, ''
    finally:
        g_cmd_dict_lock.release()


def save_cmd_dict(cmd_dict):
    g_cmd_dict_lock.acquire()
    try:
        cmd_id = cmd_dict['id']
        g_cmd_dict[cmd_id] = cmd_dict
    finally:
        g_cmd_dict_lock.release()


def update_cmd_data(new_cmd_dict):
    cmd_id = new_cmd_dict['id']

    g_cmd_dict_lock.acquire()
    try:
        if cmd_id not in g_cmd_dict:
            return
        cmd_dict = g_cmd_dict[cmd_id]
        cmd_dict.update(new_cmd_dict)
    finally:
        g_cmd_dict_lock.release()


def __probe_pg_db(sql, host, port, db, user, password):
    conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
    return 0, '0'


def __pg_exec_sql(host, port, db, user, password, sql, params):
    conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql, params)
    cur.close()
    conn.close()
    return 0, '0'


def __pg_run_sql(host, port, db, user, password, sql, params):
    conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=port)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params)
    db_data = cur.fetchall()
    cur.close()
    conn.close()
    return 0, db_data


def __get_last_lsn(host, port, user, password):
    err_code, data, time_line = pg_utils.get_last_lsn(host, port, user, password)
    if err_code == 0:
        return 0, json.dumps({"data": data, "time_line": time_line})
    else:
        return err_code, data


def __get_last_wal_file(host, port, user, password):
    err_code, lsn, time_line = pg_utils.get_last_lsn(host, port, user, password)
    if err_code != 0:
        return err_code, lsn
    try:
        sql = "select setting,unit from pg_settings where name='wal_segment_size'"
        conn = psycopg2.connect(database='template1', user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        db_data = cur.fetchall()
        cur.close()
        conn.close()
        val = db_data[0]['setting']
        unit = db_data[0]['unit']
        wal_segment_size = pg_utils.get_val_with_unit(val, unit)
        wal_file = pg_utils.lsn_to_xlog_file_name(time_line, lsn, wal_segment_size)
        return 0, wal_file
    except Exception:
        return -1, traceback.format_exc()


def probe_child_process(request_queue, target_func, cmd_dict):
    """
    :return: 返回错误码和错误信息，如果错误码为0，表示成功，否则表示失败
    """

    try:
        target_args = cmd_dict['args']
        err_code, err_msg = target_func(*target_args)
        cmd_dict['req_action'] = REQ_RECV_CMD_RESULT
        cmd_dict['run_is_over'] = True
        cmd_dict['err_code'] = err_code
        cmd_dict['err_msg'] = err_msg
        request_queue.put(cmd_dict)
    except Exception as e:
        err_msg = traceback.format_exc()
        logging.error(f"run probe cmd({cmd_dict['id']}) failed: {err_msg}")
        cmd_dict['run_is_over'] = True
        cmd_dict['err_code'] = -1
        cmd_dict['err_msg'] = str(e)
        cmd_dict['req_action'] = REQ_RECV_CMD_RESULT
        request_queue.put(cmd_dict)


def clean_expired_cmd(reply_queue):
    """清除过期的命令
    """
    try:
        need_clean_dict = {}
        for cmd_id in g_cmd_dict:
            cmd_dict = g_cmd_dict[cmd_id]
            curr_time = time.time()
            if curr_time - cmd_dict['start_time'] > cmd_dict['survival_secs']:
                need_clean_dict[cmd_id] = cmd_dict

        for cmd_id in need_clean_dict:
            cmd_dict = g_cmd_dict[cmd_id]
            del g_cmd_dict[cmd_id]
            cmd_dict['req_action'] = REQ_RECV_CMD_RESULT
            cmd_dict['err_code'] = -1
            cmd_dict['err_msg'] = 'timeout'
            _cmd_type = cmd_dict['type']
            _target_args = cmd_dict['args']

            desensitized_args = get_desensitized_args(cmd_dict)
            if 'process' in cmd_dict:
                p = cmd_dict['process']
                del cmd_dict['process']
                pid = p.pid
                try:
                    os.kill(pid, 9)
                except OSError:
                    pass
            logging.info(f"clean probe cmd({cmd_id}): {desensitized_args}")
            cmd_dict['run_end_time'] = time.time()
            cmd_dict['run_is_over'] = True
            reply_queue.put(cmd_dict)
    except Exception:
        err_msg = traceback.format_exc()
        logging.error(f"clean expired probe cmd unexpected error: {err_msg}")


# 在系统开始时，就马上启动一个服务进程，这个服务进程接受后续的执行外部命令的请求，收到请求后启动一个新的进程来执行外部命令，这样可以保证fork时没有其他线程，从而保证线程安全
def probe_service(request_queue, reply_queue):
    global g_cmd_dict

    try:
        log_file = os.path.join(config.get_log_path(), 'clup_probe.log')
        logger.reinit(logging.INFO, log_file)
    except Exception as e:
        logging.fatal(f"probe service init log failed: {repr(e)}")

    try:
        # 当父进程结束时，本进程也自动结束
        libc = ctypes.CDLL('libc.so.6')
        PR_SET_PDEATHSIG = 1
        # 设置PR_SET_PDEATHSIG，就是让父进程死亡时，子进程会退出
        libc.prctl(PR_SET_PDEATHSIG, 9)

        last_clean_time = time.time()

        while True:
            curr_time = time.time()
            if curr_time - last_clean_time > 2:
                clean_expired_cmd(reply_queue)
                last_clean_time = time.time()

            try:
                cmd_dict = request_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            req_action = cmd_dict['req_action']
            if req_action == REQ_RUN_CMD:
                # 收到执行命令的请求后，启动一个新的进程，然后执行
                cmd_id = cmd_dict['id']
                cmd_type = cmd_dict['type']
                desensitized_args = get_desensitized_args(cmd_dict)
                logging.info(f"recv probe cmd({cmd_id}): {desensitized_args}")

                if cmd_type == CMD_TYPE_PROBE_PG:
                    p = multiprocessing.Process(target=probe_child_process, args=(request_queue, __probe_pg_db, cmd_dict))
                elif cmd_type == CMD_TYPE_EXEC_SQL:
                    p = multiprocessing.Process(target=probe_child_process, args=(request_queue, __pg_exec_sql, cmd_dict))
                elif cmd_type == CMD_TYPE_RUN_SQL:
                    p = multiprocessing.Process(target=probe_child_process, args=(request_queue, __pg_run_sql, cmd_dict))
                elif cmd_type == CMD_TYPE_GET_LAST_LSN:
                    p = multiprocessing.Process(target=probe_child_process, args=(request_queue, __get_last_lsn, cmd_dict))
                elif cmd_type == CMD_TYPE_GET_LAST_WAL_FILE:
                    p = multiprocessing.Process(target=probe_child_process, args=(request_queue, __get_last_wal_file, cmd_dict))
                else:
                    cmd_dict['run_is_over'] = True
                    cmd_dict['err_code'] = -1
                    cmd_dict['err_msg'] = f'unknown cmd type: {cmd_type}'
                    cmd_dict['run_end_time'] = time.time()
                    logging.info(f"probe cmd({cmd_id}) start failed, reply: {desensitized_args}")
                    reply_queue.put(cmd_dict)
                    continue

                p.start()
                cmd_dict['process'] = p
                cmd_dict['start_time'] = time.time()
                g_cmd_dict[cmd_id] = cmd_dict

            if req_action == REQ_RECV_CMD_RESULT:
                # cmd_dict是从另一个进程发过来的，所以还没有 process对象，先暂存到q_cmd_dict
                q_cmd_dict = cmd_dict
                cmd_id = cmd_dict['id']
                if cmd_id not in g_cmd_dict:  # 可能是过期的任务又执行结束了，这个任务已经被清理掉了
                    continue
                # 把cmd_dict换成g_cmd_dict中的，同时把另一个进程发过来的信息更新进来
                cmd_dict = g_cmd_dict[cmd_id]
                del g_cmd_dict[cmd_id]
                cmd_dict.update(q_cmd_dict)

                # 跨进程传递数据时，把process对象给删除掉
                p = cmd_dict['process']
                del cmd_dict['process']
                try:
                    p.join()
                except ChildProcessError:
                    pass
                _target_args = cmd_dict['args']
                cmd_type = cmd_dict['type']
                desensitized_args = get_desensitized_args(cmd_dict)
                cmd_dict['msg_time'] = time.time()
                logging.info(f"reply probe cmd({cmd_id}): {desensitized_args}  ......")
                reply_queue.put(cmd_dict)
                logging.info(f"reply probe cmd({cmd_id}): {desensitized_args}  OK")
    except Exception:
        err_msg = traceback.format_exc()
        logging.fatal(f"probe service fatal: {err_msg}")
        os._exit(1)
    logging.info("probe service normal exit.")


def start_service():
    global g_q_request
    global g_q_reply

    multiprocessing.set_start_method('fork')
    g_q_request = multiprocessing.Queue(1000)
    g_q_reply = multiprocessing.Queue(1000)

    p = multiprocessing.Process(target=probe_service, args=(g_q_request, g_q_reply))
    p.daemon = False
    p.start()


def get_desensitized_args(cmd_dict):
    """获得脱敏的参数信息，即把密码给去掉

    """
    target_args = cmd_dict['args']
    cmd_type = cmd_dict['type']

    new_args = list(target_args)
    if cmd_type == CMD_TYPE_PROBE_PG:
        new_args[5] = '******'
    elif cmd_type == CMD_TYPE_GET_LAST_LSN:
        new_args[3] = '******'
    elif cmd_type == CMD_TYPE_RUN_SQL:
        new_args[4] = '******'
    elif cmd_type == CMD_TYPE_EXEC_SQL:
        new_args[5] = '******'

    return new_args



def run_with_timeout(cmd_type, target_args, time_out=10):
    """
    :param host:
    :param port:
    :param db:
    :param user:
    :param password:
    :param sql:
    :param time_out:
    :return: 返回错误码和错误信息，如果错误码为0，表示成功，否则表示失败
    """
    global g_q_request
    global g_q_reply


    cmd_dict = {}
    cmd_dict['req_action'] = REQ_RUN_CMD
    cmd_id = uuid.uuid1()
    cmd_dict['id'] = cmd_id
    cmd_dict['run_is_over'] = False
    cmd_dict['survival_secs'] = time_out
    cmd_dict['type'] = cmd_type
    cmd_dict['args'] = target_args
    desensitized_args = get_desensitized_args(cmd_dict)
    # 保存到g_cmd_dict全局变量中
    save_cmd_dict(cmd_dict)

    g_q_request.put(cmd_dict)
    logging.debug(f"run probe cmd({cmd_dict['id']}) {desensitized_args} ...")

    begin_time = time.time()
    last_warn_time = 0
    while True:
        curr_time = time.time()
        if int(curr_time - begin_time) > time_out and curr_time - last_warn_time > 30:
            logging.warning(f"cmd({cmd_dict['id']}) {desensitized_args} wait time {int(curr_time - begin_time)} seconds more than timeout({time_out})!")
            last_warn_time = curr_time

        try:
            reply = g_q_reply.get_nowait()
            # 收到的reply，可能是别的线程中发的命令，没有关系，更新到g_cmd_dict中
            logging.debug(f"probe cmd({cmd_id}) get a reply({reply['id']})")
            update_cmd_data(reply)
            logging.debug(f"probe cmd({cmd_id}), save reply({reply['id']}) to g_cmd_dict.")
        except queue.Empty:
            pass
        run_is_over, err_code, err_msg = get_cmd_result(cmd_id)  # 如果命令已经运行结束，则会从全局变量g_cmd_dict移除
        # logging.debug(f"probe cmd({cmd_id}), get_cmd_result return: {run_is_over}.")
        if not run_is_over:
            time.sleep(0.1)
            continue
        logging.debug(f"probe cmd({cmd_id}) result: err_code={err_code}, err_msg={err_msg} ")
        return err_code, err_msg


def probe_postgres(host, port, db, user, password, sql, time_out=10):
    """
    """
    cmd_type = CMD_TYPE_PROBE_PG
    target_args = (sql, host, port, db, user, password)
    err_code, err_msg = run_with_timeout(cmd_type, target_args, time_out)
    return err_code, err_msg


def get_last_lsn(host, port, user, password, time_out=10):
    """
    获得一个数据库的最后的LSN(log sequence number)
    :param host:
    :param port:
    :param user:
    :param password:
    :param time_out:
    :return:
    """

    cmd_type = CMD_TYPE_GET_LAST_LSN
    target_args = (host, port, user, password)
    err_code, err_msg = run_with_timeout(cmd_type, target_args, time_out)
    if err_code == 0:
        out_data = json.loads(err_msg)
        return 0, out_data['data'], out_data['time_line']
    return err_code, err_msg, ''


def get_last_wal_file(host, port, user, password, time_out=10):
    """
    获得一个数据库的最后的LSN(log sequence number)
    :param host:
    :param port:
    :param user:
    :param password:
    :param time_out:
    :return:
    """

    cmd_type = CMD_TYPE_GET_LAST_WAL_FILE
    target_args = (host, port, user, password)
    err_code, err_msg = run_with_timeout(cmd_type, target_args, time_out)
    return err_code, err_msg


def run_sql(host, port, db, user, password, sql, params, time_out=10):
    """
    :param host:
    :param port:
    :param db:
    :param user:
    :param password:
    :param sql:
    :param params:
    :param time_out:
    :return: err, data, 如果错误码为0，表示成功，则data是返回的json格式的数据，如果错误不为0，data是错误信息
    """
    cmd_type = CMD_TYPE_RUN_SQL
    target_args = (host, port, db, user, password, sql, params)
    err_code, err_msg = run_with_timeout(cmd_type, target_args, time_out)
    return err_code, err_msg


def exec_sql(host, port, db, user, password, sql, params, time_out=10):
    """
    :param host:
    :param port:
    :param db:
    :param user:
    :param password:
    :param sql:
    :param time_out:
    :return: 返回错误码和错误信息，如果错误码为0，表示成功，否则表示失败
    """

    cmd_type = CMD_TYPE_EXEC_SQL
    target_args = (host, port, db, user, password, sql, params)
    err_code, err_msg = run_with_timeout(cmd_type, target_args, time_out)
    return err_code, err_msg
