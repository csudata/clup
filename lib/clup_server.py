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
@description: clupserver主模块
"""

import argparse
import logging
import os
import sys
import time

import auto_upgrade
import config
import csu_web_server
import csuapp
import dao
import health_check
import logger
import probe_db
import psycopg2
import service_hander
import sessions
import ui_req_clup_adm
import ui_req_handler_common
import ui_req_handler_db
import ui_req_handler_ha
import ui_req_handler_host
import ui_req_handler_task
import version

exit_flag = 0


def check_env():
    # 检查psql命令是否存在
    psql_cmd = config.get('psql_cmd')
    if not os.path.exists(psql_cmd):
        logging.fatal(f"Can not find psql({psql_cmd}), please check psql_cmd in config!")
        sys.exit(-1)


def gen_http_handler(obj: object) -> dict:
    """
    :param obj:
    :return:
    """

    o_name = obj.__name__
    prefix = 'ui'
    if not o_name.startswith(prefix):
        logging.error(f"Invalid uiapi object name(must begin 'ui'):{o_name}")
        return {}

    obj_attr_list = dir(obj)
    handler_func_dict = {}
    for attr_name in obj_attr_list:
        if attr_name.startswith("__"):
            continue
        attr_val = getattr(obj, attr_name)
        if not callable(attr_val):
            continue
        handler_func_dict[attr_name] = attr_val
    return handler_func_dict


def check_and_gen_handler():
    obj_list = [
        ui_req_handler_common,
        ui_req_clup_adm,
        ui_req_handler_db,
        ui_req_handler_ha,
        ui_req_handler_host,
        ui_req_handler_task
    ]

    ui_api_dict = {}
    func_name_check_dict = {}
    for obj in obj_list:
        hdict = gen_http_handler(obj)
        for k in hdict:
            if k in func_name_check_dict:
                logging.fatal(f"duplicate function name {k} in module: {obj.__name__} and {func_name_check_dict[k]}")
                sys.exit(-1)
            func_name_check_dict[k] = obj.__name__
        ui_api_dict.update(hdict)
    return ui_api_dict


def start(foreground):
    logging.info("========== CLup starting ==========")

    ui_api_dict = check_and_gen_handler()

    check_env()

    # 在调用csuapp.prepare_run之前不要启动任何进程，线程锁也不要使用，
    # csuapp.prepare_run会fork子进程，后续是在子进程中运行会导致异常
    # 在csuapp.prepare_run之前创建的线程锁、数据库连接在后续的子进程中会异常。
    csuapp.prepare_run('clup', foreground)

    probe_db.start_service()

    # 启动对外的rpc服务
    service_hander.start()

    logging.info("begin check and upgrade...")
    try:
        err_code, err_msg = auto_upgrade.check_and_upgrade()
        if err_code != 0:
            logging.error(f"Upgrade failed: \n {err_msg}")
            sys.exit(1)

    except Exception as e:
        logging.error(f"Upgrade failed: {repr(e)}")
        sys.exit(1)


    # 装载在配置表clup_settings中的配置
    config.load_setting()

    # 把表中的一些中间状态修改成持久化的状态
    logging.info("begin recover pending...")
    dao.recover_pending()
    logging.info("database recover pending finished.")

    # 启动检查进程
    logging.info("Start ha checking thread... ")
    health_check.start_check()

    csu_web_server.start(config.get_web_root(),
                         ("0.0.0.0", config.getint('http_port')),
                         ui_api_dict,
                         sessions,
                         csuapp.is_exit)


    while not csuapp.is_exit():
        time.sleep(1)

    err_code = csuapp.cleanup()
    if err_code == 0:
        logging.info("========== CLup stoppped ==========")
        sys.exit(0)
    else:
        logging.info("========== CLup stoppped with error ==========")
        sys.exit(1)


def stop():
    csuapp.stop('clup', retry_cnt=1, retry_sleep_seconds=1)


def status():
    err_code, status_msg = csuapp.status('clup')
    print(status_msg)
    sys.exit(err_code)


def reg_service():
    script = os.path.join('/opt/clup/bin', "clupserver")
    err_code, err_msg = csuapp.reg_service(
        'clup',
        after_service_list=['network-online.target'],
        service_script=script)
    if err_code != 0:
        print(err_msg)


def main():

    prog = sys.argv[0]
    usage = f"{prog} <command> [options]\n" \
            "    command can be one of the following:\n" \
            "      start  : start ha_server\n" \
            "      stop   : stop haaggent \n" \
            "      status : display ha_server status\n" \
            "      reg_service : register to a system service\n" \
            "      version : display version information.\n" \
            ""
    # parser = OptionParser(usage=usage)
    parser = argparse.ArgumentParser(usage=usage)

    parser.add_argument("-l", "--loglevel", action="store", dest="loglevel", default="info",
                      help="Specifies log level:  debug, info, warn, error, critical, default is info")
    parser.add_argument("-f", "--foreground", action="store_true", dest="foreground",
                      help="Run in foreground, not daemon, only for start command.")

    if len(sys.argv) == 1 or sys.argv[1] == '-h' or sys.argv[1] == '--help':
        print(version.copyright_message())
        # parser.print_help()
        print(f"usage: {usage}")
        sys.exit(0)
    if sys.argv[1] == 'version':
        print(version.copyright_message())
        sys.exit(0)
    orig_args = sys.argv[2:]
    new_args = []
    for arg in orig_args:
        if len(arg) == 1:
            arg = '-' + arg
        new_args.append(arg)
    args = parser.parse_args(new_args)

    log_level_dict = {"debug": logging.DEBUG,
                      "info": logging.INFO,
                      "warn": logging.WARN,
                      "error": logging.ERROR,
                      "critical": logging.CRITICAL,
                      }

    str_log_level = args.loglevel.lower()
    if str_log_level not in log_level_dict:
        sys.stderr.write("Unknown loglevel: " + args.loglevel)
        sys.exit(-1)

    if sys.argv[1] in ['start', 'status', 'reg_service']:
        logging.info(version.copyright_message())

    # 初使用化日志
    log_level = log_level_dict[args.loglevel.lower()]
    os.makedirs(config.get_log_path(), 0o700, exist_ok=True)
    log_file = os.path.join(config.get_log_path(), 'clupserver.log')
    logger.init(log_level, log_file)
    config.load()

    if sys.argv[1] == 'start':
        start(args.foreground)
    elif sys.argv[1] == 'status':
        status()
    elif sys.argv[1] == 'stop':
        stop()
    elif sys.argv[1] == 'reg_service':
        reg_service()
    else:
        sys.stderr.write(f'Invalid command: {sys.argv[1]}\n')
        sys.exit(1)


if __name__ == "__main__":
    main()
