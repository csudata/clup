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
@description: 长时间运行的任务
"""

import json
import logging
import os
import time
import traceback

import cluster_state
import dao
import database_state
import db_encrypt
import dbapi
import general_task_mgr
import pg_db_lib
import polar_lib
import rpc_utils

__author__ = 'tangcheng'



def create_pg_db(task_id, host, db_id, rpc_dict):
    """[summary]

    Args:
        task_id ([type]): [description]
        db_dict ([type]): [description]
    """

    err_code = 0
    err_msg = ''
    rpc = None
    try:
        msg_prefix = f"Create pg(db_id={db_id}) on {host}"

        step = f'Connect to host: {host}'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            return err_code, err_msg
        rpc = err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")
    except Exception:
        dao.update_db_state(db_id, database_state.CREATE_FAILD)
        return -1, traceback.format_exc()

    try:

        step = 'Check the parameters for creating the database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")
        required_params = [
            "host",
            "port",
            "instance_name",
            "os_user",
            "os_uid",
            "pg_bin_path",
            "db_pass",
            "db_user",
            "setting_dict"
        ]

        for para in required_params:
            if para not in rpc_dict:
                err_code = -1
                err_msg = f"{msg_prefix}: {step}step fail: This parameter is not provided in the create database configuration: {para}, configuration: {rpc_dict}"
                return err_code, err_msg

        pg_bin_path = rpc_dict['pg_bin_path']
        _pg_root_path = os.path.abspath(os.path.join(pg_bin_path, os.pardir))
        db_user = rpc_dict['db_user']
        db_pass = rpc_dict['db_pass']
        pgdata = rpc_dict['pgdata']
        port = rpc_dict['port']

        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'create pg os user'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")

        if 'os_uid' not in rpc_dict:
            if rpc_dict['os_user'] == 'halo':
                os_uid = 1000
            else:
                os_uid = 701
        else:
            os_uid = rpc_dict['os_uid']

        os_user = rpc_dict['os_user']
        err_code, err_msg = pg_db_lib.pg_init_os_user(rpc, os_user, os_uid)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Add configuration in file(.bashrc)'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")
        err_code, err_msg = rpc.pwd_getpwnam(os_user)
        if err_code != 0:
            return err_code, err_msg
        pwd_dict = err_msg
        os_user_home = pwd_dict['pw_dir']
        unix_socket_directories = rpc_dict['setting_dict'].get('unix_socket_directories', '/tmp')
        cells = unix_socket_directories.split(',')
        unix_socket_dir = cells[0].strip()
        err_code, err_msg = pg_db_lib.pg_init_bashrc(rpc, os_user_home, pg_bin_path, pgdata, port, unix_socket_dir)
        if err_code != 0:
            return err_code, err_msg

        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create data directory'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")

        logging.info("Create_database : create_pg_data_dir")
        err_code, err_msg = rpc.os_makedirs(pgdata, 0o700, exist_ok=True)
        if err_code != 0:
            return err_code, err_msg

        err_code, err_msg = pg_db_lib.set_pg_data_dir_mode(rpc, os_user, rpc_dict['pgdata'])
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")


        step = 'Execute the initialization command(initdb)'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")
        cmd = f""" su - {os_user} -c '{pg_bin_path}/initdb  --auth-local=peer --auth-host=md5 --username="{db_user}" --pwfile=<(echo "{db_pass}") -D {pgdata} ' """
        err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Configuration file: postgresql.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")

        setting_dict = rpc_dict['setting_dict']
        # 获得数据库的版本
        str_pg_ver = rpc_dict['version']
        cells = str_pg_ver.split('.')
        pg_main_ver = int(cells[0])
        # 当打开归档后，wal_level不能是minimal
        if 'archive_mode' in setting_dict:
            if setting_dict.get('archive_mode') == 'on':
                if 'wal_level' in setting_dict:
                    if setting_dict['wal_level'] == 'minimal':
                        setting_dict['wal_level'] = 'replica'
                else:
                    # 10版本及以上，wal_level默认已经时replica，不需要设置
                    if pg_main_ver <= 9:
                        setting_dict['wal_level'] = 'replica'

        try:
            postgresql_conf = f'{pgdata}/postgresql.conf'
            if rpc.os_path_exists(postgresql_conf):
                rpc.modify_config_type1(postgresql_conf, setting_dict, is_backup=False)
            else:
                return -1, f"pgdata {pgdata} not exists!"
        except Exception as e:
            err_code = -1
            err_msg = str(e)
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Configuration file: pg_hba.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")

        hba_file = f'{pgdata}/pg_hba.conf'
        content = """\nhost replication all all md5\nhost all all all md5"""
        err_code, err_msg = rpc.append_file(hba_file, content)
        if err_code != 0:
            return err_code, err_msg

        # 如果数据库用户名不等于操作系统用户名，需要在pg_hba.conf加本地的用户映射以便于本地不需要密码旧可以登录
        if os_user != db_user:
            ci_dict = {
                r"^local\s+all\s+all\s+peer$": f"local   all             all                                     peer map={db_user}",
                r"^local\s+replication\s+all\s+peer$": f"local   replication     all                                     peer map={db_user}"
            }
            rpc.modify_config_type2(hba_file, ci_dict, is_backup=False)
            pg_ident_conf_file = f'{pgdata}/pg_ident.conf'
            content = f"{db_user}\t{os_user}\t{db_user}"
            tag_line = '# ====== Add by clup map os user'
            rpc.config_file_set_tag_content(pg_ident_conf_file, tag_line, content)
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Start database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")

        err_code, err_msg = pg_db_lib.start(rpc, rpc_dict['pgdata'])
        if err_code < 0:
            err_msg = f'start db error :{err_msg}'
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create extension in the database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start:  {step} ...")

        plug_str = setting_dict.get('shared_preload_libraries', "'pg_stat_statements'")
        plug_list = plug_str.replace("'", '').split(',')
        err_code = err_msg = ''
        for plug in plug_list:
            db_name = 'template1'
            sql = f"CREATE EXTENSION {plug}"
            cmd = f"""su - {os_user} -c "psql -U {db_user} -p {port} -d {db_name}  -c '{sql}' " """
            general_task_mgr.log_info(task_id, f"run cmd: {cmd} ...")
            err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
            if err_code != 0:
                err_msg = f"run cmd: {cmd} failed: {err_msg}"
                return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

    except Exception:
        dao.update_db_state(db_id, database_state.CREATE_FAILD)
        err_code = -1
        err_msg = f"{msg_prefix}: {step} with unexcept error, {traceback.format_exc()}"
        logging.error(err_msg)
    finally:
        if rpc is not None:
            rpc.close()
    return err_code, err_msg


def task_create_pg_db(task_id, host, db_id, rpc_dict):
    err_code, err_msg = create_pg_db(task_id, host, db_id, rpc_dict)
    # 创建完成后更新clup_db中的状态
    if err_code == 0:
        db_state = database_state.RUNNING
    else:
        db_state = database_state.CREATE_FAILD
    try:
        dao.update_db_state(db_id, db_state)
    except Exception as e:
        logging.error(f"dao.update_db_state failed: {repr(e)}")

    if err_code == 0:
        task_state = 1
        # leifliu Test
        err_msg = "Success"
    else:
        task_state = -1
    general_task_mgr.complete_task(task_id, task_state, err_msg)


def build_pg_standby(task_id, host, db_id, rpc_dict):

    err_code = 0
    err_msg = ''

    try:
        msg_prefix = f"Build standby(db_id={db_id}) on {host}"
        step = f'Connect to host: {host}'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")
        rpc = None
        err_code, err_msg = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            return err_code, err_msg
        rpc = err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step}step successful.")
    except Exception:
        dao.update_db_state(db_id, database_state.CREATE_FAILD)
        return -1, traceback.format_exc()

    try:
        step = 'Check the parameters for creating a standby database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")

        required_params = [
            'db_id',  # 备库的db_id
            'up_db_port',  # 上级库端口
            'up_db_repl_ip',  # 上级库流复制ip
            'instance_type',
            'os_user',
            'os_uid',
            'pg_bin_path',
            'db_user',
            'repl_user',
            'repl_pass',
            'pgdata',  # 数据库的数据目录
            'repl_app_name',  # 流复制的application_name
            'version',  # 数据库的版本
            # "setting_list'
        ]

        # 这里填写独立备库的需要的独有参数
        alone_params = [
            "os_user",
            "os_uid"
        ]

        mask_rpc_dict = {}
        mask_rpc_dict.update(rpc_dict)
        if 'repl_pass' in mask_rpc_dict:
            mask_rpc_dict['repl_pass'] = '******'

        if 'instance_type' not in rpc_dict:
            err_code = -1
            err_msg = f"{msg_prefix}: {step} step fail: parameter is not provided in the create database configuration(instance_type) , configuration: {mask_rpc_dict}"
            return err_code, err_msg


        required_params += alone_params

        for para in required_params:
            if para not in rpc_dict:
                err_code = -1
                err_msg = f"{msg_prefix}: {step}step fail: parameter is not provided in the create database configuration: {para}, configuration: {mask_rpc_dict}"
                return err_code, err_msg

        port = rpc_dict['port']
        up_db_repl_ip = rpc_dict['up_db_repl_ip']
        up_db_port = rpc_dict['up_db_port']
        repl_user = rpc_dict['repl_user']
        repl_pass = rpc_dict['repl_pass']
        db_user = rpc_dict['db_user']  # 这是数据库中的超级用户，创建出来的数据库在本地的os_user下，可以不需要密码的就能登录数据库
        version = rpc_dict['version']
        pgdata = rpc_dict['pgdata']
        port = rpc_dict['port']
        pg_bin_path = rpc_dict['pg_bin_path']

        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'create pg os user'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")

        if 'os_uid' not in rpc_dict:
            os_uid = 701
        else:
            os_uid = rpc_dict['os_uid']

        os_user = rpc_dict['os_user']
        step = 'create pg os user'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")

        if 'os_uid' not in rpc_dict:
            if rpc_dict['os_user'] == 'halo':
                os_uid = 1000
            else:
                os_uid = 701
        else:
            os_uid = rpc_dict['os_uid']

        os_user = rpc_dict['os_user']
        err_code, err_msg = pg_db_lib.pg_init_os_user(rpc, os_user, os_uid)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Add configuration in file(.bashrc)'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")
        err_code, err_msg = rpc.pwd_getpwnam(os_user)
        if err_code != 0:
            return err_code, err_msg
        pwd_dict = err_msg
        os_user_home = pwd_dict['pw_dir']
        unix_socket_dir = rpc_dict.get('unix_socket_dir', '/tmp')
        err_code, err_msg = pg_db_lib.pg_init_bashrc(rpc, os_user_home, pg_bin_path, pgdata, port, unix_socket_dir)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create data directory'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")
        err_code, err_msg = rpc.os_makedirs(pgdata, 0o700, exist_ok=True)
        if err_code != 0:
            return err_code, err_msg

        err_code, err_msg = pg_db_lib.set_pg_data_dir_mode(rpc, os_user, rpc_dict['pgdata'])
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'prepare .pgpass for pg_basebackup'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")

        # 连接主库的密码配置到.pgpass中
        err_code, err_msg = pg_db_lib.dot_pgpass_add_item(
            rpc, os_user, up_db_repl_ip, up_db_port, 'replication', repl_user, repl_pass,
        )
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        # 表空间参数
        step = 'Check the tablespace information'
        other_param = rpc_dict.get('other_param')
        tbl_list = rpc_dict.get('tblspc_dir')
        tblspc_str = ''
        if tbl_list:
            # 把表空间需要的目录自动建立起来
            step = 'Create a table space data directory'
            for k in tbl_list:
                tbl_dir = k['new_dir']
                step = f'Create a table space data directory: {tbl_dir}'
                general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")
                err_code, err_msg = rpc.os_makedirs(tbl_dir, 0o700, exist_ok=True)
                if err_code != 0:
                    return err_code, err_msg

                err_code, err_msg = pg_db_lib.set_pg_data_dir_mode(rpc, os_user, tbl_dir)
                if err_code != 0:
                    return err_code, err_msg
                general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

            # 拼接pg_basebackup命令行的表空间映射的擦的参数
            step = 'pg_basebackup'
            tblspc_str = "--format=p "
            try:
                for k in tbl_list:
                    tblspc_str += f""" -T "{k['old_dir']}"="{k['new_dir']}" """
            except Exception as e:
                err_code = -1
                err_msg = str(e)
                return err_code, err_msg

        step = 'pg_basebackup'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")
        other_param = ' -P -Xs ' if not other_param else other_param

        cmd = f"""su  - {os_user} -c "pg_basebackup -h{up_db_repl_ip} -p{up_db_port} -U {repl_user} -D {pgdata} {tblspc_str} {other_param}" """

        # 运行pg_basebackup命令
        general_task_mgr.log_info(task_id, f"{msg_prefix}: run {cmd} ...")
        cmd_id = rpc.run_long_term_cmd(cmd, output_qsize=10, output_timeout=600)
        state = 0
        while state == 0:
            state, err_code, err_msg, stdout_lines, stderr_lines = rpc.get_long_term_cmd_state(cmd_id)
            has_output = False
            if stdout_lines:
                has_output = True
                for line in stdout_lines:
                    general_task_mgr.log_info(task_id, f"{msg_prefix}: {line}")
            if stderr_lines:
                has_output = True
                for line in stderr_lines:
                    general_task_mgr.log_info(task_id, f"{msg_prefix}: {line}")
            if not has_output:
                time.sleep(1)
            if state < 0:
                err_code = -1
                return err_code, err_msg
        rpc.remove_long_term_cmd(cmd_id)
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration file: postgresql.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")

        setting_dict = {
            "port": port
        }
        try:
            postgresql_conf = f'{pgdata}/postgresql.conf'
            if rpc.os_path_exists(postgresql_conf):
                rpc.modify_config_type1(postgresql_conf, setting_dict, is_backup=False)
            else:
                return -1, f"pgdata {pgdata} not exists!"
        except Exception as e:
            return -1, str(e)
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration file: pg_hba.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")

        # 如果数据库用户名不等于操作系统用户名，需要在pg_hba.conf加本地的用户映射以便于本地不需要密码旧可以登录
        tmp_os_user = os_user
        if tmp_os_user != db_user:
            hba_file = f"{pgdata}/pg_hba.conf"
            ci_dict = {
                r"^local\s+all\s+all\s+peer$": f"local   all             all                                     peer map={db_user}",
                r"^local\s+replication\s+all\s+peer$": f"local   replication     all                                     peer map={db_user}"
            }
            rpc.modify_config_type2(hba_file, ci_dict, is_backup=False)
            pg_ident_conf_file = f'{pgdata}/pg_ident.conf'
            content = f"{db_user}\t{os_user}\t{db_user}"
            tag_line = '# ====== Add by clup map os user ======'
            rpc.config_file_set_tag_content(pg_ident_conf_file, tag_line, content)
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Configure stream  replication parameters.'
        repl_app_name = rpc_dict['repl_app_name']
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")
        delay = rpc_dict.get('delay')
        pg_major_int_version = int(str(version).split('.')[0])
        err_code, err_msg = pg_db_lib.set_sr_config_file(
            rpc,
            pg_major_int_version,
            repl_user,
            repl_pass,
            up_db_repl_ip,
            up_db_port,
            repl_app_name,
            pgdata,
            delay
        )

        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Start database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: step start: {step} ...")
        err_code, err_msg = pg_db_lib.start(rpc, rpc_dict['pgdata'])
        if err_code < 0:
            err_msg = f'start pg db error: {err_msg}'
            return 0, err_msg
        # 使用pg_isready检查下数据库是不是可以正常连接了
        err_code, err_msg = pg_db_lib.is_ready(rpc, rpc_dict['pgdata'], rpc_dict['port'])
        if err_code != 0 and err_code != 1:
            general_task_mgr.log_error(task_id, f"Create database success, but database is not ready, {err_msg}.")
            dao.update_db_state(db_id, database_state.STOP)
        else:
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

    except Exception:
        err_code = -1
        err_msg = f"{msg_prefix}: An unknown error occurred in this step:{step}, error : {traceback.format_exc()}"
        logging.error(err_msg)
        dao.update_db_state(db_id, database_state.CREATE_FAILD)
    finally:
        if rpc:
            rpc.close()
    return err_code, err_msg


def task_build_pg_standby(task_id, host, db_id, rpc_dict):
    err_code, err_msg = build_pg_standby(task_id, host, db_id, rpc_dict)
    # 创建完成后更新clup_db中的状态
    if err_code == 0:
        db_state = database_state.RUNNING
    else:
        db_state = database_state.CREATE_FAILD
    try:
        dao.update_db_state(db_id, db_state)
    except Exception as e:
        logging.error(f"dao.update_db_state failed: {repr(e)}")

    if err_code == 0:
        task_state = 1
        err_msg = 'Success'
    else:
        task_state = -1
    general_task_mgr.complete_task(task_id, task_state, err_msg)


def pg_setting_list_to_dict(setting_list):
    """通过clup_init_db_conf中的配置，根据不同的配置项目, 补足ms、B(byte)单位或引号等信息，同时转换成字典的形式

    Args:
        setting_list (list): 需要补足信息的pg配置的字典

    Returns:
        dict: 返回补全信息的字典
    """
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


def modify_db_user_pass(db_host, db_port, db_user, db_pass):
    try:
        db_dict = {
            "db_host": db_host,
            "db_port": db_port,
            "db_user": db_user,
            "db_pass": db_pass
        }

        conn = dbapi.get_db_conn(db_host, db_port, db_user, db_pass)
        # 检查该用户是否存在
        sql = "select userepl from pg_user where usename=%(db_user)s"
        rows = dbapi.conn_query(conn, sql, db_dict)
        if len(rows) == 0:
            # 没有该复制用户，就创建一个
            sql = f"CREATE USER {db_user} replication LOGIN  ENCRYPTED PASSWORD %(db_pass)s"
            dbapi.conn_execute(conn, sql, db_dict)
            conn.close()
            return 0, ''
        if not rows[0]:
            # 用户存在但是没有复制权限
            sql = f"alter user {db_user}  replication login ENCRYPTED PASSWORD %(db_pass)s"
            dbapi.conn_execute(conn, sql, db_dict)
            conn.close()
            return 0, ''
    except Exception as e:
        return -1, f'create user error: {repr(e)}'
    return 0, ''


def create_sr_cluster(task_id, cluster_id, pdict):

    pre_msg = f"Create sr cluster (cluster_id: {cluster_id})"
    try:
        db_list = pdict['db_list']
        # 集群中数据库的application_name
        for db in db_list:
            db['repl_app_name'] = db['host']

        # pri_dict中放主库的参数
        pri_dict = db_list[0]
        # db_dict创建主库和配置主库需要用到的参数
        setting_dict = pg_setting_list_to_dict(pdict['setting_list'])
        setting_dict['port'] = pdict['port']
        pri_dict['port'] = pdict['port']

        # 插入db表
        db_detail = {
            'os_user': pri_dict['os_user'],
            'os_uid': pri_dict['os_uid'],
            'pg_bin_path': pri_dict['pg_bin_path'],
            'db_user': pdict['db_user'],
            'db_pass': pdict['db_pass'],
            'repl_user': pdict['repl_user'],
            'repl_pass': pdict['repl_pass'],
            'version': pri_dict['version'],
            'room_id': "0"
        }
        db_detail['os_user'] = pri_dict['os_user']
        db_detail['os_uid'] = pri_dict['os_uid']
        db_detail['pg_bin_path'] = pri_dict['pg_bin_path']

        pri_dict['repl_app_name'] = pri_dict['repl_ip']
        pri_dict['cluster_id'] = cluster_id
        pri_dict['db_detail'] = json.dumps(db_detail)
        pri_dict['state'] = 1
        pri_dict['is_primary'] = 1
        pri_dict['db_state'] = database_state.CREATING

        sql = "INSERT INTO clup_db (cluster_id, state, pgdata, is_primary, repl_app_name, host," \
              " repl_ip, db_detail, port, db_state, scores, db_type) VALUES " \
              "(%(cluster_id)s, %(state)s, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, " \
              " %(repl_ip)s, %(db_detail)s, %(port)s, %(db_state)s, %(scores)s, 1) RETURNING db_id "
        rows = dbapi.query(sql, pri_dict)
        if len(rows) == 0:
            err_msg = 'Failed to insert clup_db into database configuration.'
            return -1, err_msg
        primary_db_id = rows[0]['db_id']

        # 开始创建主库
        general_task_mgr.log_info(task_id, f'{pre_msg}: create primary db (db_id: {primary_db_id})')

        task_name = f"create_db(db={primary_db_id})"

        rpc_dict = {}
        rpc_dict.update(pri_dict)
        del rpc_dict['db_detail']
        rpc_dict['db_id'] = primary_db_id
        rpc_dict['task_name'] = task_name
        rpc_dict['instance_name'] = rpc_dict['host']
        rpc_dict['db_user'] = pdict['db_user']
        rpc_dict['db_pass'] = db_encrypt.from_db_text(pdict['db_pass'])
        rpc_dict['repl_user'] = pdict['repl_user']
        rpc_dict['repl_pass'] = db_encrypt.from_db_text(pdict['repl_pass'])
        rpc_dict['setting_dict'] = setting_dict

        unix_socket_directories = setting_dict.get('unix_socket_directories', '/tmp')
        cells = unix_socket_directories.split(',')
        unix_socket_dir = cells[0].strip()

        err_code, err_msg = create_pg_db(task_id, rpc_dict['host'], primary_db_id, rpc_dict)
        if err_code != 0:
            dao.update_db_state(rpc_dict['db_id'], database_state.CREATE_FAILD)
            err_msg = f"{pre_msg}: Database creation failure: {err_msg}"
            return err_code, err_msg
        else:
            dao.update_db_state(rpc_dict['db_id'], database_state.RUNNING)

        # 创建复制用户
        general_task_mgr.log_info(task_id, f'{pre_msg}: primary db (db_id: {primary_db_id}) create a replication user')
        err_code, err_msg = dao.create_replication_user(rpc_dict['host'], pdict['port'], rpc_dict['db_user'], rpc_dict['db_pass'], rpc_dict['repl_user'], rpc_dict['repl_pass'])
        if err_code != 0:
            err_msg = f"{err_msg}: Failed to create a stream replication user: (db_id: {primary_db_id})"
            return -1, err_msg

        # ===========搭建备库==========
        cur_index = 0
        for db in db_list:
            cur_index += 1
            if db == pri_dict:
                # 是主库就跳过
                continue
            general_task_mgr.log_info(task_id, f"{pre_msg}: Start building standby on {db['host']}")
            err_code, err_msg = rpc_utils.get_rpc_connect(db['host'])
            if err_code != 0:
                msg = f"Host connection failure({db['host']}), please check service clup-agent is running!"
                dao.update_db_state(rpc_dict['db_id'], database_state.CREATE_FAILD)
                return -1, msg
            rpc = err_msg
            rpc.close()
            # db_dict存储搭建备库需要的参数
            db_dict = {}
            db_dict.update(db)

            # 插入db表
            db_detail = {
                'os_user': pri_dict['os_user'],
                'os_uid': pri_dict['os_uid'],
                'pg_bin_path': pri_dict['pg_bin_path'],
                'db_user': pdict['db_user'],
                'db_pass': pdict['db_pass'],
                'repl_user': pdict['repl_user'],
                'repl_pass': pdict['repl_pass'],
                'version': pri_dict['version'],
                'room_id': "0"
                # 'reset_cmd': db['reset_cmd']
                # 'mem_size': pri_dict['mem_size'],
                # 'conn_cnt': pri_dict['conn_cnt'],
                # 'setting_list': setting_list,
            }

            db_dict['cluster_id'] = cluster_id
            db_dict['state'] = 1,
            db_dict['is_primary'] = 0
            db_dict['repl_app_name'] = db_dict['repl_ip']
            db_dict['db_detail'] = json.dumps(db_detail)
            db_dict['port'] = pdict['port']
            db_dict['db_state'] = database_state.CREATING
            db_dict['up_db_id'] = primary_db_id
            sql = "INSERT INTO clup_db (cluster_id, state, pgdata, is_primary, repl_app_name, host," \
                " repl_ip, db_detail, port, db_state, scores, up_db_id) VALUES " \
                "(%(cluster_id)s, %(state)s, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, " \
                " %(repl_ip)s, %(db_detail)s, %(port)s, %(db_state)s, %(scores)s, %(up_db_id)s) RETURNING db_id "
            rows = dbapi.query(sql, db_dict)
            err_insert_count = 0
            if len(rows) == 0:
                err_insert_count += 1
                err_msg = 'db_id was not returned after the database information was inserted'
                return -1, err_msg
            else:
                db_id = rows[0]['db_id']

            # 开始搭备库
            # rpc_dict 放下创建备库的调用的参数
            rpc_dict = dict()
            rpc_dict['up_db_id'] = primary_db_id
            rpc_dict['up_db_host'] = pri_dict['host']
            rpc_dict['up_db_port'] = pdict['port']
            rpc_dict['up_db_repl_ip'] = pri_dict['repl_ip']
            rpc_dict['db_id'] = db_id
            rpc_dict['os_user'] = db['os_user']
            rpc_dict['os_uid'] = db['os_uid']
            rpc_dict['host'] = db['host']
            rpc_dict['port'] = pdict['port']
            rpc_dict['pg_bin_path'] = db['pg_bin_path']
            rpc_dict['pgdata'] = db['pgdata']
            rpc_dict['db_user'] = pdict['db_user']   # 数据库用户，当db_user禹os_user不相同是，需要在pg_hba.conf中加用户映射，否则本地无法误密码的登录数据库
            rpc_dict['repl_user'] = pdict['repl_user']
            rpc_dict['repl_pass'] = db_encrypt.from_db_text(pdict['repl_pass'])
            rpc_dict['delay'] = 0
            rpc_dict['instance_type'] = 'physical'
            rpc_dict['cpu'] = 1
            rpc_dict['version'] = db['version']
            rpc_dict['repl_app_name'] = db['repl_ip']
            rpc_dict['tblspc_dir'] = []  # 表空间的目录信息:  [{'old_dir': '', 'new_dir': ''}, {'old_dir': '', 'new_dir': ''}]
            rpc_dict['other_param'] = ''  # pg_basebackup的附件参数
            rpc_dict['unix_socket_dir'] = unix_socket_dir

            # 开始搭建备库
            err_code, err_msg = build_pg_standby(task_id, rpc_dict['host'], rpc_dict['db_id'], rpc_dict)
            if err_code != 0:
                dao.update_db_state(db_id, database_state.CREATE_FAILD)
                err_msg = f"{pre_msg}: Failed to create the standby database.: {err_msg}"
                return err_code, err_msg
            else:
                dao.update_db_state(db_id, database_state.RUNNING)

    except Exception:
        err_code = -1
        err_msg = traceback.format_exc()
        dao.set_cluster_state(cluster_id, cluster_state.CREATE_FAILD)
    return err_code, err_msg


def task_create_sr_cluster(task_id, cluster_id, rpc_dict):
    err_code, err_msg = create_sr_cluster(task_id, cluster_id, rpc_dict)
    if err_code == 0:
        task_state = 1
        # leifliu Test
        err_msg = "Success"
    else:
        task_state = -1
        dao.set_cluster_state(cluster_id, cluster_state.CREATE_FAILD)
    general_task_mgr.complete_task(task_id, task_state, err_msg)


def create_polar_db(task_id, host, db_id, rpc_dict):
    """[summary]
    Args:
        task_id ([type]): [description]
        db_dict ([type]): [description]
    """
    err_code = 0
    err_msg = ''
    rpc = None
    try:
        msg_prefix = f"Create pg(db_id={db_id}) on {host}"

        step = f'Connect to host: {host}'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, rpc = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            err_msg = rpc
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Check the parameters for creating the database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        required_params = [
            "host",
            "port",
            "instance_name",
            "os_user",
            "os_uid",
            "pg_bin_path",
            "db_pass",
            "db_user",
            "setting_dict"
        ]

        for para in required_params:
            if para not in rpc_dict:
                err_code = -1
                err_msg = f"{msg_prefix}: {step}step fail: No parameters are provided in the configuration to create the database: {para}, configuration: {rpc_dict}"
                return err_code, err_msg

        pg_bin_path = rpc_dict['pg_bin_path']
        _pg_root_path = os.path.abspath(os.path.join(pg_bin_path, os.pardir))
        db_user = rpc_dict['db_user']
        db_pass = rpc_dict['db_pass']
        pgdata = rpc_dict['pgdata']
        port = rpc_dict['port']
        os_user = rpc_dict["os_user"]
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        kwargs = {
            "os_user": os_user,
            "db_user": db_user,
            "pg_bin_path": pg_bin_path,
            "pgdata": pgdata,
            "port": port,
            "rpc_dict": rpc_dict
        }
        polarCommon = polar_lib.PolarCommon(rpc, **kwargs)

        step = 'create os_user'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.create_os_user()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Add configuration in file(.bashrc)'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.edit_bashrc()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create data directory'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        logging.info("Create_database : create_pg_data_dir")
        err_code, err_msg = rpc.os_makedirs(pgdata, 0o700, exist_ok=True)
        if err_code != 0:
            return err_code, err_msg

        err_code, err_msg = pg_db_lib.set_pg_data_dir_mode(rpc, os_user, rpc_dict['pgdata'])
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Execute the initialization command(initdb)'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.init_db(db_pass)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        # 如果是主库就创建共享文件夹
        is_primary = rpc_dict["is_primary"]
        if is_primary:
            step = 'Create a pfs shared folder'
            general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
            err_code, err_msg = polarCommon.mk_share_dir()
            if err_code != 0:
                return err_code, err_msg
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'start pfs daemon'
        err_code, err_msg = polarCommon.start_pfsdaemon()
        if err_code != 0 and err_code != 1:
            return err_code, "Failed to start pfs daemon"
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        # 如果是主库就执行polar-initdb拷贝文件到poalr_datadir
        if is_primary:
            step = 'Copy files to shared storage'
            general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
            err_code, err_msg = polarCommon.polar_initdb()
            if err_code != 0:
                return err_code, err_msg
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration postgresql.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.edit_postgresql_conf()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration pg_hba.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.edit_hba_conf()
        if err_code != 0:
            return 400, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        # 如果是只读节点，需要配置recovery.conf文件
        if not is_primary:
            step = 'Configure stream replication parameters'
            general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
            err_code, err_msg = polarCommon.edit_repl_conf()
            if err_code != 0:
                return err_code, err_msg
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

            # 将流复制密码写入到.pgpass中
            step = 'Configure the stream replication user password.'
            general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
            repl_user = rpc_dict['repl_user']
            repl_pass = rpc_dict["repl_pass"]
            up_db_port = rpc_dict["up_db_port"]
            up_db_repl_ip = rpc_dict["up_db_repl_ip"]
            err_code, err_msg = pg_db_lib.dot_pgpass_add_item(
                rpc, os_user, up_db_repl_ip, up_db_port, 'replication', repl_user, repl_pass,
            )
            if err_code != 0:
                return err_code, err_msg
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'start database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = pg_db_lib.start(rpc, rpc_dict['pgdata'])
        if err_code < 0:
            err_msg = f'start db error :{err_msg}'
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        if is_primary:
            step = 'Create extension in the database'
            general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
            err_code, err_msg = polarCommon.create_extension()
            if err_code != 0:
                return err_code, err_msg
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

            step = 'Create a replication slot in the primary database'
            general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
            err_code, err_msg = polarCommon.create_repl()
            if err_code != 0:
                return err_code, err_msg
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")
        return 0, ''
    except Exception:
        err_code = -1
        err_msg = f"{msg_prefix}: {step} An unknown error has occurred: {traceback.format_exc()}"
        logging.error(err_msg)
    finally:
        del polarCommon
        if rpc is not None:
            rpc.close()
    return err_code, err_msg


# Leifliu Test 搭建polardb共享存储reader节点
def build_polar_reader(task_id, host, db_id, rpc_dict):
    """[summary]
    Args:
        task_id ([type]): [description]
        db_dict ([type]): [description]
    """
    err_code = 0
    err_msg = ''
    try:
        msg_prefix = f"Create pg(db_id={db_id}) on {host}"

        step = f'Connect to host: {host}'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        rpc = None
        err_code, rpc = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            err_msg = rpc
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Check the parameters for creating the database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")

        pg_bin_path = rpc_dict['pg_bin_path']
        _pg_root_path = os.path.abspath(os.path.join(pg_bin_path, os.pardir))
        db_user = rpc_dict['db_user']
        pgdata = rpc_dict['pgdata']
        port = rpc_dict['port']
        os_user = rpc_dict["os_user"]
        _db_pass = db_encrypt.from_db_text(rpc_dict['db_pass'])
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        kwargs = {
            "os_user": os_user,
            "db_user": db_user,
            "pg_bin_path": pg_bin_path,
            "pgdata": pgdata,
            "port": port,
            "rpc_dict": rpc_dict
        }
        polarCommon = polar_lib.PolarCommon(rpc, **kwargs)

        step = 'create os_user'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.create_os_user()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Add configuration in file(.bashrc)'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.edit_bashrc()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create data directory'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        logging.info("Create_database : create_pg_data_dir")
        err_code, err_msg = rpc.os_makedirs(pgdata, 0o700, exist_ok=True)
        if err_code != 0:
            return err_code, err_msg

        err_code, err_msg = pg_db_lib.set_pg_data_dir_mode(rpc, os_user, rpc_dict['pgdata'])
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'prepare .pgpass for replication user'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        # 连接主库的密码配置到.pgpass中
        repl_user = rpc_dict['repl_user']
        repl_pass = db_encrypt.from_db_text(rpc_dict["repl_pass"])
        up_db_port = rpc_dict["up_db_port"]
        up_db_repl_ip = rpc_dict["up_db_repl_ip"]
        err_code, err_msg = pg_db_lib.dot_pgpass_add_item(
            rpc, os_user, up_db_repl_ip, up_db_port, 'replication', repl_user, repl_pass,
        )
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'make dirs and cp master configure files'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polar_lib.init_polar_reader(rpc, rpc_dict)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'start pfs daemon'
        err_code, err_msg = polarCommon.start_pfsdaemon()
        if err_code != 0 and err_code != 1:
            return err_code, "Failed to start pfs daemon"
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration postgresql.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polar_lib.edit_reader_postgresql_conf(rpc, rpc_dict)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration pg_hba.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.edit_hba_conf()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration recovery.conf'
        repl_app_name = rpc_dict['repl_app_name']
        polar_hostid = rpc_dict["polar_hostid"]
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polar_lib.edit_reader_conf(
            rpc,
            pgdata,
            repl_app_name,
            repl_user,
            up_db_repl_ip,
            up_db_port,
            polar_hostid
        )
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create a stream replication in the primary database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        slot_name = f'replica{rpc_dict["polar_hostid"]}'
        err_code, err_msg = polar_lib.create_replication_slot(rpc_dict, slot_name)
        if err_code != 0 and err_code != 1:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'start database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = pg_db_lib.start(rpc, rpc_dict['pgdata'])
        if err_code < 0:
            err_msg = f'start db error :{err_msg}'
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        del polarCommon
        return 0, ''
    except Exception:
        err_msg = f"{msg_prefix}: {step} An unknown error occurs: {traceback.format_exc()}"
        logging.error(err_msg)
        return -1, err_msg
    finally:
        if rpc:
            rpc.close()


# Leifliu Test
def build_polar_standby(task_id, host, db_id, rpc_dict):
    try:
        msg_prefix = f"Build standby(db_id={db_id}) on {host}"
        step = f'Connect to host: {host}'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, rpc = rpc_utils.get_rpc_connect(host)
        if err_code != 0:
            err_msg = rpc
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Check the parameters for creating a standby database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")

        required_params = [
            'db_id',  # 备库的db_id
            'up_db_port',  # 上级库端口
            'up_db_repl_ip',  # 上级库流复制ip
            'instance_type',
            'os_user',
            'os_uid',
            'pg_bin_path',
            'db_user',
            'repl_user',
            'repl_pass',
            'pgdata',  # 数据库的数据目录
            'repl_app_name',  # 流复制的application_name
            'version',  # 数据库的版本
            # "setting_list'
        ]

        mask_rpc_dict = {}
        mask_rpc_dict.update(rpc_dict)
        if 'repl_pass' in mask_rpc_dict:
            mask_rpc_dict['repl_pass'] = '******'

        for para in required_params:
            if para not in rpc_dict:
                err_code = -1
                err_msg = f"{msg_prefix}: {step}step fail: This parameter is not provided in the create database configuration: {para}, configuration: {mask_rpc_dict}"
                return err_code, err_msg

        up_db_repl_ip = rpc_dict['up_db_repl_ip']
        up_db_port = rpc_dict['up_db_port']
        repl_user = rpc_dict['repl_user']
        repl_pass = db_encrypt.from_db_text(rpc_dict['repl_pass'])
        db_user = rpc_dict['db_user']  # 这是数据库中的超级用户，创建出来的数据库在本地的os_user下，可以不需要密码的就能登录数据库
        os_user = rpc_dict["os_user"]
        _version = rpc_dict['version']
        pgdata = rpc_dict['pgdata']
        port = rpc_dict['port']
        pg_bin_path = rpc_dict['pg_bin_path']
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        kwargs = {
            "os_user": os_user,
            "db_user": db_user,
            "pg_bin_path": pg_bin_path,
            "pgdata": pgdata,
            "port": port,
            "rpc_dict": rpc_dict
        }
        polarCommon = polar_lib.PolarCommon(rpc, **kwargs)

        step = 'create os user'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.create_os_user()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Add configuration in file(.bashrc)'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.edit_bashrc()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create data directory'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        logging.info("Create_database : create_pg_data_dir")
        err_code, err_msg = rpc.os_makedirs(pgdata, 0o700, exist_ok=True)
        if err_code != 0:
            return err_code, err_msg

        err_code, err_msg = pg_db_lib.set_pg_data_dir_mode(rpc, os_user, rpc_dict['pgdata'])
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'prepare .pgpass for pg_basebackup'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        # 连接主库的密码配置到.pgpass中
        err_code, err_msg = pg_db_lib.dot_pgpass_add_item(
            rpc, os_user, up_db_repl_ip, up_db_port, 'replication', repl_user, repl_pass,
        )
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'pg_basebackup'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.polar_basebackup(task_id, msg_prefix)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration postgresql.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polar_lib.edit_standby_postgresql_conf(rpc, rpc_dict)
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'configuration pg_hba.conf'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polarCommon.edit_hba_conf()
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Configure stream replication parameters'
        repl_app_name = rpc_dict['repl_app_name']
        polar_hostid = rpc_dict["polar_hostid"]
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = polar_lib.edit_standby_conf(
            rpc,
            pgdata,
            repl_app_name,
            repl_user,
            up_db_repl_ip,
            up_db_port,
            polar_hostid
        )
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'Create a stream replication in the primary database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        slot_name = f'standby{rpc_dict["polar_hostid"]}'
        err_code, err_msg = polar_lib.create_replication_slot(rpc_dict, slot_name)
        if err_code != 0:
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step}step fail: {err_msg},Please check and repair manually.")
        else:
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")

        step = 'start database'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: start step: {step} ...")
        err_code, err_msg = pg_db_lib.start(rpc, rpc_dict['pgdata'])
        if err_code < 0:
            err_msg = f'start pg db error: {err_msg}'
            return 0, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} step successful.")
        del polarCommon
        return 0, ''
    except Exception:
        err_code = -1
        err_msg = f"{msg_prefix}: {step}An unknown error occurs:  {traceback.format_exc()}"
        logging.error(err_msg)
    finally:
        if rpc is not None:
            rpc.close()
    return err_code, err_msg


# 创建polardb共享存储集群
def create_polar_sd_cluster(task_id, cluster_id, pdict):
    pre_msg = f"Create polardb sd cluster (cluster_id: {cluster_id})"
    try:
        db_list = pdict['db_list']
        # 集群中数据库的application_name
        for db in db_list:
            db['repl_app_name'] = db['host']

        # pri_dict中放主库的参数
        pri_dict = db_list[0]
        # db_dict创建主库和配置主库需要用到的参数
        setting_dict = pg_setting_list_to_dict(pdict['setting_list'])
        setting_dict['port'] = pdict['port']
        pri_dict['port'] = pdict['port']

        # 插入db表
        db_detail = {
            'os_user': pri_dict['os_user'],
            'os_uid': pri_dict['os_uid'],
            'pg_bin_path': pri_dict['pg_bin_path'],
            'db_user': pdict['db_user'],
            'db_pass': pdict['db_pass'],
            'repl_user': pdict['repl_user'],
            'repl_pass': pdict['repl_pass'],
            'version': pri_dict['version'],
            'polar_type': 'master',
            'pfs_disk_name': pdict['pfs_disk_name'],
            'polar_datadir': pdict["polar_datadir"],
            'pfsdaemon_params': pdict['pfsdaemon_params'],
            'reset_cmd': pri_dict['reset_cmd']
        }
        db_detail['os_user'] = pri_dict['os_user']
        db_detail['os_uid'] = pri_dict['os_uid']
        db_detail['pg_bin_path'] = pri_dict['pg_bin_path']

        pri_dict['repl_app_name'] = pri_dict['repl_ip']
        pri_dict['cluster_id'] = cluster_id
        db_detail['polar_hostid'] = 1
        pri_dict['db_detail'] = json.dumps(db_detail)
        pri_dict['state'] = 1
        pri_dict['is_primary'] = 1
        pri_dict['db_state'] = database_state.CREATING
        pri_dict['db_type'] = int(pdict['db_type'])

        sql = "INSERT INTO clup_db (cluster_id, state, pgdata, is_primary, repl_app_name, host," \
              " repl_ip, db_detail, port, db_state, scores, db_type) VALUES " \
              "(%(cluster_id)s, %(state)s, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, " \
              " %(repl_ip)s, %(db_detail)s, %(port)s, %(db_state)s, %(scores)s, %(db_type)s) RETURNING db_id"
        rows = dbapi.query(sql, pri_dict)
        if len(rows) == 0:
            err_msg = 'Failed to insert database information into clup_db.'
            return -1, err_msg
        primary_db_id = rows[0]['db_id']

        # 开始创建主库
        general_task_mgr.log_info(task_id, f'{pre_msg}: create primary db (db_id: {primary_db_id})')
        task_name = f"create_db(db={primary_db_id})"

        rpc_dict = {}
        rpc_dict.update(pri_dict)
        del rpc_dict['db_detail']
        rpc_dict['db_id'] = primary_db_id
        rpc_dict['task_name'] = task_name
        rpc_dict['instance_name'] = rpc_dict['host']
        rpc_dict['db_user'] = pdict['db_user']
        rpc_dict['db_pass'] = db_encrypt.from_db_text(pdict['db_pass'])
        rpc_dict['repl_pass'] = db_encrypt.from_db_text(pdict['repl_pass'])

        rpc_dict['pfsdaemon_params'] = pdict['pfsdaemon_params']
        rpc_dict['pfs_disk_name'] = pdict['pfs_disk_name']
        rpc_dict['polar_datadir'] = pdict['polar_datadir']
        rpc_dict['setting_dict'] = setting_dict
        rpc_dict["polar_hostid"] = 1
        rpc_dict["primary_slot_name"] = []
        for index in range(len(db_list) - 1):
            rpc_dict["primary_slot_name"].append(f"replica{index + 2}")

        err_code, err_msg = create_polar_db(task_id, rpc_dict['host'], primary_db_id, rpc_dict)
        if err_code != 0:
            dao.update_db_state(rpc_dict['db_id'], database_state.FAULT)
            err_msg = f"{pre_msg}: Database creation failure: {err_msg}"
            return err_code, err_msg
        else:
            dao.update_db_state(rpc_dict['db_id'], database_state.RUNNING)

        # 创建复制用户
        general_task_mgr.log_info(task_id, f'{pre_msg}: primary db (db_id: {primary_db_id}) create a replication user')
        err_code, err_msg = modify_db_user_pass(rpc_dict['host'], pdict['port'], rpc_dict['db_user'], rpc_dict['db_pass'])
        if err_code != 0:
            err_msg = f"{err_msg}: Failed to create a stream replication user: (db_id: {primary_db_id})"
            return -1, err_msg

        # ===========搭建备库==========
        cur_index = 0
        for db in db_list:
            cur_index += 1
            if db == pri_dict:
                # 是主库就跳过
                continue
            general_task_mgr.log_info(task_id, f"{pre_msg}: Start building standby on {db['host']}")
            err_code, _rpc = rpc_utils.get_rpc_connect(db['host'])
            if err_code != 0:
                msg = f"Host connection failure({db['host']}), please check service clup-agent is running!"
                return -1, msg

            # db_dict存储搭建备库需要的参数
            db_dict = {}
            db_dict.update(db)

            # 插入db表
            db_detail = {
                'os_user': pri_dict['os_user'],
                'os_uid': pri_dict['os_uid'],
                'pg_bin_path': pri_dict['pg_bin_path'],
                'db_user': pdict['db_user'],
                'db_pass': pdict['db_pass'],
                'repl_user': pdict['repl_user'],
                'repl_pass': pdict['repl_pass'],
                'version': pri_dict['version'],
                'polar_type': 'reader',
                'pfs_disk_name': pdict['pfs_disk_name'],
                'polar_datadir': pdict["polar_datadir"],
                'pfsdaemon_params': pdict['pfsdaemon_params'],
                'reset_cmd': db['reset_cmd']

            }

            db_dict['cluster_id'] = cluster_id
            db_dict['state'] = 1,
            db_dict['is_primary'] = 0
            db_dict['repl_app_name'] = db_dict['repl_ip']
            db_detail['polar_hostid'] = cur_index
            db_dict['db_detail'] = json.dumps(db_detail)
            db_dict['port'] = pdict['port']
            db_dict['db_state'] = database_state.CREATING
            db_dict['up_db_id'] = primary_db_id
            db_dict['db_type'] = pdict['db_type']

            sql = "INSERT INTO clup_db (cluster_id, state, pgdata, is_primary, repl_app_name, host," \
                " repl_ip, db_detail, port, db_state, scores, up_db_id, db_type) VALUES " \
                "(%(cluster_id)s, %(state)s, %(pgdata)s, %(is_primary)s, %(repl_app_name)s, %(host)s, " \
                " %(repl_ip)s, %(db_detail)s, %(port)s, %(db_state)s, %(scores)s, %(up_db_id)s, %(db_type)s) RETURNING db_id "
            rows = dbapi.query(sql, db_dict)
            err_insert_count = 0
            if len(rows) == 0:
                err_insert_count += 1
                err_msg = 'db_id is not returned after database information is inserted into clup_db'
                return -1, err_msg
            else:
                db_id = rows[0]['db_id']

            # 开始搭备库
            # rpc_dict 放下创建备库的调用的参数
            rpc_dict = {}
            rpc_dict['db_id'] = db_id
            rpc_dict['up_db_id'] = primary_db_id
            rpc_dict['up_db_host'] = pri_dict['host']
            rpc_dict['up_db_port'] = pdict['port']
            rpc_dict['up_db_pgdata'] = pri_dict['pgdata']
            rpc_dict['up_db_repl_ip'] = pri_dict['repl_ip']
            rpc_dict['os_user'] = db['os_user']
            rpc_dict['os_uid'] = db['os_uid']
            rpc_dict['host'] = db['host']
            rpc_dict['port'] = pdict['port']
            rpc_dict['pg_bin_path'] = db['pg_bin_path']
            rpc_dict['pgdata'] = db['pgdata']
            rpc_dict['db_user'] = pdict['db_user']   # 数据库用户，当db_user禹os_user不相同是，需要在pg_hba.conf中加用户映射，否则本地无法误密码的登录数据库
            rpc_dict['repl_user'] = pdict['repl_user']
            # rpc_dict['repl_pass'] = db_encrypt.from_db_text(pdict['repl_pass'])
            rpc_dict['repl_pass'] = pdict['repl_pass']
            rpc_dict['delay'] = 0
            rpc_dict['instance_type'] = 'physical'
            rpc_dict['version'] = db['version']
            rpc_dict['repl_app_name'] = db['repl_ip']
            rpc_dict['tblspc_dir'] = []  # 表空间的目录信息:  [{'old_dir': '', 'new_dir': ''}]
            # rpc_dict['other_param'] = ''  # pg_basebackup的附件参数

            rpc_dict['pfsdaemon_params'] = pdict['pfsdaemon_params']
            rpc_dict['pfs_disk_name'] = pdict['pfs_disk_name']
            rpc_dict['polar_datadir'] = pdict['polar_datadir']
            rpc_dict['setting_dict'] = setting_dict
            rpc_dict["primary_slot_name"] = f"replica{ cur_index }"
            rpc_dict["is_primary"] = 0
            rpc_dict["polar_hostid"] = cur_index
            rpc_dict['db_pass'] = pdict['db_pass']
            if "instance_name" not in pdict:
                rpc_dict["instance_name"] = db['host']

            # 开始搭建备库,使用简化流程
            err_code, err_msg = build_polar_reader(task_id, rpc_dict['host'], rpc_dict['db_id'], rpc_dict)
            if err_code != 0:
                dao.update_db_state(rpc_dict['db_id'], database_state.FAULT)
                err_msg = f"{pre_msg}: Database creation failure: {err_msg}"
                return err_code, err_msg
            else:
                dao.update_db_state(rpc_dict['db_id'], database_state.RUNNING)

    except Exception:
        err_code = -1
        err_msg = traceback.format_exc()
    return err_code, err_msg


def task_create_polar_sd_cluster(task_id, cluster_id, rpc_dict):
    err_code, err_msg = create_polar_sd_cluster(task_id, cluster_id, rpc_dict)
    if err_code == 0:
        task_state = 1
        err_msg = "Success"
    else:
        task_state = -1
    general_task_mgr.complete_task(task_id, task_state, err_msg)


def task_build_polar_standby(task_id, host, db_id, rpc_dict):
    err_code, err_msg = build_polar_standby(task_id, host, db_id, rpc_dict)
    # 创建完成后更新clup_db中的状态
    if err_code == 0:
        db_state = database_state.RUNNING
    else:
        db_state = database_state.FAULT
    try:
        dao.update_db_state(db_id, db_state)
    except Exception as e:
        logging.error(f"dao.update_db_state failed: {repr(e)}")

    if err_code == 0:
        task_state = 1
        err_msg = 'Success'
    else:
        task_state = -1
    general_task_mgr.complete_task(task_id, task_state, err_msg)


# leifliu Test 构建polardb 共享存储只读库
def task_build_polar_reader(task_id, host, db_id, rpc_dict):
    err_code, err_msg = build_polar_reader(task_id, host, db_id, rpc_dict)
    # 创建完成后更新clup_db中的状态
    if err_code == 0:
        db_state = database_state.RUNNING
    else:
        db_state = database_state.FAULT
    try:
        dao.update_db_state(db_id, db_state)
    except Exception as e:
        logging.error(f"dao.update_db_state failed: {repr(e)}")

    if err_code == 0:
        task_state = 1
        err_msg = 'Success'
    else:
        task_state = -1
    general_task_mgr.complete_task(task_id, task_state, err_msg)
