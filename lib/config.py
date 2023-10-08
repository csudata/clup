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
@description: 配置管理模块
"""

import logging
import os
import sys
import threading
import traceback

import dbapi

# 一个全局锁
__lock = threading.Lock()


def get_root_path():
    if __file__.endswith('.py'):
        root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' + os.sep))
    else:
        pid = os.getpid()
        proc_exe_file = f"/proc/{pid}/exe"
        exe_file = os.readlink(proc_exe_file)
        root_path = os.path.realpath(os.path.join(os.path.dirname(exe_file), '..' + os.sep))
    return root_path


__root_path = get_root_path()
__module_path = os.path.join(__root_path, 'lib')
__cfg_path = os.path.join(__root_path, 'conf')
__config_file = os.path.join(__cfg_path, '.' + os.sep + 'clup.conf')
__data_path = os.path.join(__root_path, 'data')
__bin_path = os.path.join(__root_path, 'bin')
__log_path = os.path.join(__root_path, 'logs')
__tmp_path = os.path.join(__root_path, 'tmp')
__web_root = os.path.join(__root_path, 'ui')
__sql_path = os.path.join(__root_path, 'sql')

if os.path.isdir('/run'):
    __run_path = '/run'
else:
    __run_path = '/var/run'

__data = {}
is_loaded = False


def __load_config():
    config_data = {}
    with open(__config_file, encoding='utf-8') as f:
        for line in f:
            line = line.strip()

            if len(line) < 1:
                continue
            if line[0] == "#":
                continue
            elif line[0] == ";":
                continue
            try:
                pos = line.index('=')
            except ValueError:
                continue
            key = line[:pos].strip()
            value = line[pos + 1:].strip()
            config_data[key] = value
        if 'ignore_reset_cmd_return_code' not in config_data:
            config_data['ignore_reset_cmd_return_code'] = 0
        if 'session_expired_secs' not in config_data:
            config_data['session_expired_secs'] = 600

        if 'db_cluster_change_check_interval' not in config_data:
            config_data['db_cluster_change_check_interval'] = 10
        if int(config_data['db_cluster_change_check_interval']) < 1 and int(config_data['db_cluster_change_check_interval']) > 60:
            config_data['db_cluster_change_check_interval'] = 10
            logging.warning("db_cluster_change_check_interval value %d out of range[1,60], force set it to 10!")

        config_data.setdefault('http_port', 8080)
        if 'http_auth' not in config_data:
            config_data['http_auth'] = 1
    return config_data


def load():
    global __data
    global is_loaded

    try:
        __data = __load_config()
    except Exception as e:
        logging.error(f"Load configuration failed: {repr(e)}:\n{traceback.format_exc()}")
        sys.exit(1)
    is_loaded = True


def load_setting():
    global __data
    try:
        sql = r"SELECT * FROM clup_settings where key not like 'lic\_%%' and key not in ('product_id')"
        rows = dbapi.query(sql)
        for row in rows:
            key = row['key']
            val = row['content']
            __data[key] = val
    except Exception as e:
        logging.fatal(f"Can not load config from clup_setting: {str(e)}")
        os._exit(1)


def get_run_path():
    """
    :return: 返回run目录
    """
    return __run_path


def get_cfg_path():
    """获得conf目录路径"""
    return __cfg_path


def get_module_path():
    return __module_path


def get_data_path():
    return __data_path


def get_bin_path():
    return __bin_path


def get_log_path():
    return __log_path


def get_tmp_path():
    return __tmp_path


def get_web_root():
    return __web_root


def get_sql_path():
    return __sql_path


def get_pid_file():
    global __run_path
    return f"{__run_path}/clup.pid"


def get(key, default=None):
    global __lock
    __lock.acquire()
    try:
        return __data.get(key, default)
    finally:
        __lock.release()


def set_key(key, value):
    global __lock
    __lock.acquire()
    try:
        __data[key] = value
    finally:
        __lock.release()


def getint(key):
    global __lock
    __lock.acquire()
    try:
        return int(__data[key])
    finally:
        __lock.release()


def has_key(key):
    global __lock
    __lock.acquire()
    try:
        return key in __data
    finally:
        __lock.release()


def get_all():
    return __data


def reload():
    global __data
    global is_loaded
    global __lock

    new_data = {}
    try:
        new_data = __load_config()
    except Exception as e:
        err_msg = f"read configuration failed: {repr(e)}:\n{traceback.format_exc()}"
        logging.error(err_msg)
        return -1, repr(err_msg)

    __lock.acquire()
    try:
        __data.update(new_data)
    except Exception as e:
        return -1, f'update config error: {repr(e)} '
    finally:
        __lock.release()
    is_loaded = True
    return 0, 'Clup configuration reload succeeded'
