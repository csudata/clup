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

import os
import re
import signal
import stat
import time
import traceback

import db_encrypt
import general_task_mgr
import probe_db
import rpc_utils


# 用于修饰函数,函数与第一个参数可以传rpc也可以传host,通过此修饰函数,自动判断第一个参数的类型,如果是字符串,则认为是host
def rpc_or_host(a_func):
    def wrap_func(rpc_host, *arg, **kwargs):
        rpc = None
        need_close_rpc = False
        try:
            if isinstance(rpc_host, str):
                host = rpc_host
                err_code, err_msg = rpc_utils.get_rpc_connect(host, 3)
                if err_code != 0:
                    return -1, f"Host connection failure({host})"
                need_close_rpc = True
                rpc = err_msg
            else:
                rpc = rpc_host
        except Exception:
            return -1, traceback.format_exc()

        try:
            return a_func(rpc, *arg, **kwargs)
        except Exception:
            return -1, traceback.format_exc()
        finally:
            if rpc and need_close_rpc:
                rpc.close()
        return 0, ''
    return wrap_func



@rpc_or_host
def is_running(rpc, pgdata):
    """
    检查数据库是否正在运行
    :param pgdata: 数据库的数据目录
    :return: True表示正在运行,False表示没有运行
    """

    try:
        pg_pid_file = f'{pgdata}/postmaster.pid'
        if not rpc.os_path_exists(pg_pid_file):
            return 0, False

        err_code, err_msg = rpc.file_read(pg_pid_file)
        if err_code != 0:
            return err_code, err_msg
        data = err_msg
        lines = data.split('\n')
        str_pid = lines[0]
        str_pid = str_pid.strip()
        try:
            pid = int(str_pid)
        except ValueError:
            return 0, False
        if not rpc.os_path_exists(f"/proc/{pid}"):
            return 0, False

        try:
            err_code, err_msg = rpc.file_read(f"/proc/{pid}/comm")
            if err_code != 0:
                return err_code, err_msg
            data = err_msg
            data = data.strip()
            # 增加了判断此进程的名称是否为postgres
            if data != 'postgres':
                return 0, False
            else:
                return 0, True
        except Exception:
            return 0, False
    except IOError:
        return 0, False
    except Exception:
        return -1, traceback.format_exc()


@rpc_or_host
def start(rpc, pgdata):
    """
    启动数据库
    :return: 返回0表示成功启动数据库,返回1表示数据库已运行,返回-1表示失败
    """

    # 检查数据库是否已运行
    err_code, err_msg = is_running(rpc, pgdata)
    if err_code != 0:
        return -1, err_msg

    is_run = err_msg
    if is_run:
        return 1, "already running"

    if not rpc.os_path_exists(pgdata):
        return -1, f"directory {pgdata} not exists"

    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg
    except Exception:
        return -1, traceback.format_exc()

    # 把数据库启动
    try:
        cmd = f'''su - {upw_dict["pw_name"]} -c 'pg_ctl start -w -D {pgdata} > /dev/null' '''
        err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
        if err_code != 0:
            if err_code == 1:
                return -1, f"Start the database failed: {err_msg}"\
                    f"Maybe the environment variable is set incorrectly which in {upw_dict['pw_name']}/.bashrc"
            return err_code, err_msg

        # 过3秒后再检查状态
        time.sleep(3)
        err_code, err_msg = is_running(rpc, pgdata)
        if err_code != 0:
            return err_code, "Can not start database"
        return 0, ''
    except Exception:
        return -1, traceback.format_exc()


@rpc_or_host
def stop(rpc, pgdata, wait_time=0):
    """
    停止数据库
    :return: 如果成功,则返回True,如果失败则返回False
    """

    if not rpc.os_path_exists(pgdata):
        return -1, f"directory {pgdata} not exists"

    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg

        # 把数据库停下来
        cmd = f'''su - {upw_dict["pw_name"]} -c 'pg_ctl stop -m fast -w -D {pgdata} > /dev/null' '''
        err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
        if err_code != 0:
            return err_code, err_msg

        err_code, err_msg = is_running(rpc, pgdata)
        if err_code != 0:
            return err_code, err_msg
        if wait_time == 0:
            return err_code, err_msg
        while wait_time > 0:
            err_code, err_msg = is_running(rpc, pgdata)
            if err_code != 0:
                return err_code, err_msg
            elif not err_msg:
                return 0, "Stop success"
            wait_time -= 1
            time.sleep(1)
        if err_code == 0:
            return -1, "Can not stop!"
    except Exception as e:
        return -1, str(e)


@rpc_or_host
def reload(rpc, pgdata):
    # 检查数据库是否已运行
    err_code, err_msg = is_running(rpc, pgdata)
    if err_code != 0:
        return err_code, err_msg

    is_run = err_msg
    if not is_run:  # 如果数据库没有运行,则忽略reload
        return 0, ''

    if not rpc.os_path_exists(pgdata):
        return -1, f"directory {pgdata} not exists"

    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg
    except Exception as e:
        return -1, str(e)

    # 把数据库启动
    cmd = f'''su - {upw_dict["pw_name"]} -c 'pg_ctl reload -D {pgdata}' '''
    rpc.run_cmd_result(cmd)
    return 0, "reload success"


@rpc_or_host
def is_ready(rpc, pgdata, port):
    """
    测试数据库是否可以正常连接
    :param pgdata:
    :param port:
    :return:
    """
    if not rpc.os_path_exists(pgdata):
        return -1, "directory %s not exists" % pgdata

    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg

        cmd = f'''su - {upw_dict["pw_name"]} -c  "pg_isready -p {port} -t 2" '''
        err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
        # 0: is_ready, 1: refuse(in starting), 2: not response(maybe not start), 3: run error(maybe params error)
        if err_code == 0:
            return 0, True
        elif err_code == 1:
            return 1, "during startup, please wait..."
        elif err_code == 2:
            return -1, "no response to the connection attempt, maybe database was not start"
        elif err_code == 3:
            return -1, f"no attempt was made, check the parameters in cmd({cmd})"
        else:
            return -1, f"run cmd({cmd}) with error, {err_msg}"
    except Exception:
        return -1, f"Check pg_isready with unexcept error, {traceback.format_exc()}"



@rpc_or_host
def restart(rpc, pgdata):
    """
    重启数据库
    :param pgdata:
    :return:
    """
    err_code, err_msg = is_running(rpc, pgdata)
    if err_code != 0:
        return err_code, err_msg
    is_run = err_msg
    if is_run:
        err_code, err_msg = stop(rpc, pgdata, wait_time=10)
        if err_code != 0:
            return err_code, err_msg

    err_code, err_msg = start(rpc, pgdata)
    return err_code, err_msg


@rpc_or_host
def is_primary(rpc, pgdata):
    primary_msg = "in production"
    standby_msg = "in archive recovery"

    if not rpc.os_path_exists(pgdata):
        return -1, f"directory {pgdata} not exists"
    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg
    except Exception as e:
        return -1, str(e)
    os_user = upw_dict['pw_name']
    cmd = f"su - {os_user} -c 'pg_controldata -D {pgdata} | grep cluster'"

    err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
    if err_code != 0:
        return err_code, err_msg

    if primary_msg in out_msg:
        is_primary = 1
    elif standby_msg in out_msg:
        is_primary = 0
    return 0, is_primary


@rpc_or_host
def promote(rpc, pgdata):
    """
    把备库提升为主库
    :param pgdata:
    :return: 返回0表示成功,返回-1表示失败
    """

    if not rpc.os_path_exists(pgdata):
        return -1, "directory %s not exists" % pgdata

    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg
    except Exception as e:
        return -1, str(e)

    # 提升数据库
    cmd = f'''su - {upw_dict["pw_name"]} -c 'pg_ctl promote -D {pgdata} > /dev/null' '''
    err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
    if err_code != 0:
        return err_code, err_msg

    # 过1秒后再检查状态
    time.sleep(2)
    recovery_file = os.path.join(pgdata, 'recovery.conf')
    if rpc.os_path_exists(recovery_file):
        return -1, "after 2 seconds, but recovery.conf still exists!"
    else:
        return 0, ''


@rpc_or_host
def pg_init_os_user(rpc, os_user, os_uid):
    # 检查用户是否存在
    os_user_exists = True
    err_code, err_msg = rpc.pwd_getpwnam(os_user)
    if err_code < 0:
        return err_code, err_msg
    if err_code == 1:
        os_user_exists = False

    if not os_user_exists:
        if os_uid == 0:
            cmd = f"useradd  -m -s /bin/bash {os_user}"
        else:
            cmd = f"groupadd -g {os_uid} {os_user} &&  useradd -g {os_uid} -m -s /bin/bash -u {os_uid} {os_user}"
        err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
        if err_code != 0:
            return err_code, err_msg
    return err_code, err_msg


@rpc_or_host
def pg_init_bashrc(rpc, os_user_home, pg_bin_path, pgdata, port, pg_default_host='/tmp'):
    try:
        # 在.bashrc中增加配置
        pg_root_path = os.path.abspath(os.path.join(pg_bin_path, os.pardir))
        bash_rc_file = f'{os_user_home}/.bashrc'
        if pg_default_host:
            # 可能存在获取到的unix_socket_directories参数为'.'此时应将PGHOST设为$PGDATA
            if pg_default_host.strip("'") == '.':
                pg_default_host = '$PGDATA'
            content = (
                f"export PATH={pg_bin_path}:$PATH\n"
                f"export LD_LIBRARY_PATH={pg_root_path}/lib:$LD_LIBRARY_PATH\n"
                f"export PGDATA={pgdata}\n"
                f"export PGHOST={pg_default_host}\n"
                f"export PGPORT={port}"
            )
        else:
            content = (
                f"export PATH={pg_bin_path}:$PATH\n"
                f"export LD_LIBRARY_PATH={pg_root_path}/lib:$LD_LIBRARY_PATH\n"
                f"export PGDATA={pgdata}\n"
                f"export PGPORT={port}"
            )
        tag_line = '# ====== Add by clup init env '
        rpc.config_file_set_tag_in_head(bash_rc_file, tag_line, content)
        return 0, ''
    except Exception as e:
        return -1, repr(e)


def merge_primary_conninfo_str(primary_conninfo, primary_conninfo_dict):
    """
    把原先的primary_info中配置的key=value的对用字典primary_conninfo_dict中的替换,而字典中不存在的key对,仍然保持旧的值
    primary_conninfo = 'application_name=10.197.167.26 user=postgres host=10.197.167.28 port=5432 password=postgres sslmode=disable sslcompression=1'
    """
    try:
        # 先用re_sub把key = value中key到等号以及等号到value中的空格去掉,然后以空格为分隔拆分为一个list
        res_list = re.sub(r'\s*=\s*', '=', primary_conninfo).split()
        d = {}
        for res in res_list:
            temp = res.split('=')
            k = temp[0]
            v = temp[1]
            d[k] = v
        d.update(primary_conninfo_dict)
        primary_conninfo_list = []
        for k, v in d.items():
            item = str(k) + '=' + str(v)
            primary_conninfo_list.append(item)
        primary_conninfo_str = " ".join(primary_conninfo_list)
    except Exception as e:
        return -1, f"get primary_conninfo_str failure: {repr(e)}"

    return 0, "'" + primary_conninfo_str + "'"


@rpc_or_host
def set_sr_config_file(rpc, pg_major_int_version, repl_user, repl_pass, up_db_repl_ip, up_db_port, repl_app_name, pgdata, recovery_min_apply_delay=None):

    """
    设置备库配置文件以满足流复制的需要
    """

    new_primary_conninfo_str = f"'application_name={repl_app_name} user={repl_user} password={repl_pass} host={up_db_repl_ip} port={up_db_port} sslmode=disable sslcompression=1'"

    primary_conninfo_dict = {
        "application_name": repl_app_name,
        "user": repl_user,
        "host": up_db_repl_ip,
        "port": up_db_port,
        "password": repl_pass,
    }

    # 记录需要把文件的属主改成与数据目录相同的文件列表
    need_set_owner_file_list = []

    if not rpc.os_path_exists(pgdata):
        return -1, f"directory {pgdata} not exists!"

    if pg_major_int_version < 12:
        recovery_file = f'{pgdata}/recovery.conf'
        recovery_done_file = f"{pgdata}/recovery.done"
        if not rpc.os_path_exists(recovery_file):
            need_set_owner_file_list.append(recovery_file)
            if rpc.os_path_exists(recovery_done_file):
                cmd = """/bin/cp %s %s""" % (recovery_done_file, recovery_file)
                err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
                if err_code != 0:
                    return err_code, err_msg
            else:
                # 如果recovery.conf不存在,则写一个空文件
                err_code, err_msg = rpc.os_write_file(recovery_file, 0, b"")
                if err_code != 0:
                    return err_code, err_msg
        # 通过前面的操作,recovery.conf文件必定存在
        config_file = recovery_file
        err_code, item_dict = rpc.read_config_file_items(config_file, ['primary_conninfo'])
    else:
        # PostgreSQL 12及之上的版本,需要有standby.signal表示其是备库
        standby_signal_file = f"{pgdata}/standby.signal"
        err_code, err_msg = rpc.os_write_file(standby_signal_file, 0, b"")
        if err_code != 0:
            return err_code, err_msg
        need_set_owner_file_list.append(standby_signal_file)

        # 先读取postgresql.auto.conf,如果其中有配置项primary_conninfo,则使用postgresql.auto.conf
        auto_conf_file = '%s/postgresql.auto.conf' % pgdata
        err_code, item_dict = rpc.read_config_file_items(auto_conf_file, ['primary_conninfo'])
        if err_code == 0 and item_dict.get('primary_conninfo'):
            config_file = auto_conf_file
        else:
            config_file = '%s/postgresql.conf' % pgdata
            err_code, item_dict = rpc.read_config_file_items(config_file, ['primary_conninfo'])
            if err_code != 0:
                return err_code, item_dict

    old_primary_conninfo_str = item_dict.get('primary_conninfo', '')[1:-1]
    if old_primary_conninfo_str:
        err_code, err_msg = merge_primary_conninfo_str(old_primary_conninfo_str, primary_conninfo_dict)
        if err_code != 0:
            return err_code, err_msg
        # 如果没有出错,err_msg中是merge后的primary_conninfo
        new_primary_conninfo_str = err_msg if err_msg else new_primary_conninfo_str

    modify_item_dict = {
        "recovery_target_timeline": "'latest'",
        "primary_conninfo": new_primary_conninfo_str
    }
    if recovery_min_apply_delay:
        modify_item_dict['recovery_min_apply_delay'] = recovery_min_apply_delay

    if pg_major_int_version < 12:
        modify_item_dict['standby_mode'] = "'on'"

    rpc.modify_config_type1(config_file, modify_item_dict, deli_type=1, is_backup=False)

    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg

        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg
    except Exception as e:
        return -1, str(e)

    for var_file in need_set_owner_file_list:
        err_code, err_msg = rpc.os_chown(var_file, upw_dict['pw_uid'], upw_dict['pw_gid'])
        if err_code != 0:
            err_msg = f"can not set {var_file} owner to {upw_dict['pw_uid']}: {err_msg}"
            return err_code, err_msg
    return 0, ''


@rpc_or_host
def dot_pgpass_add_item(rpc, os_user, ip, port, db_name, db_user, db_pass):
    """
    往.pgpass添加一行,如果.pgpass文件不存在,会创建,如果要添加的内容已经存在,则直接返回
    """

    err_code, err_msg = rpc.pwd_getpwnam(os_user)
    if err_code != 0:
        return err_code, err_msg
    pwd_dict = err_msg
    user_home_dir = pwd_dict['pw_dir']
    pgpass_file = f'{user_home_dir}/.pgpass'
    connection_str = f"{ip}:{port}:{db_name}:{db_user}:{db_pass}"
    if rpc.os_path_exists(pgpass_file):
        err_code, err_msg = rpc.file_read(pgpass_file)
        if err_code != 0:
            return err_code, err_msg
        data = err_msg
        pgpass_conn_list = data.split('\n')
    else:
        pgpass_conn_list = []

    # 已经存在,不需要再次添加
    if connection_str in pgpass_conn_list:
        return 0, ''

    # 检查是否已存部分相同信息,判断是不是只是密码不一样,是则注释掉这一行
    i = 0
    for line in pgpass_conn_list:
        if ip in line and not line.startswith("#"):
            # 判断是不是只是密码不一样
            old_info_list = line.split(":")[:-1]
            old_info_str = ":".join(old_info_list)
            if old_info_str in connection_str:
                line = "#" + line
                pgpass_conn_list[i] = line
                break
        i += 1

    pgpass_conn_list.append(connection_str)
    data = '\n'.join(pgpass_conn_list)
    err_code, err_msg = rpc.file_write(pgpass_file, data)
    if err_code != 0:
        return err_code, err_msg
    err_code, err_msg = rpc.os_chown(pgpass_file, pwd_dict['pw_uid'], pwd_dict['pw_gid'])
    if err_code != 0:
        return err_code, err_msg
    err_code, err_msg = rpc.os_chmod(pgpass_file, 0o600)
    return err_code, err_msg


@rpc_or_host
def pg_rewind(rpc, pdict):
    msg_prefix = pdict['msg_prefix']
    msg_prefix = f"{msg_prefix}: pg_rewind"
    task_id = pdict['task_id']
    try:
        step = 'Check all parameters'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: Start {step} ...")
        pgdata = pdict['pgdata']
        version = pdict['version']
        _os_user = pdict['os_user']
        up_db_port = pdict['up_db_port']
        up_db_repl_ip = pdict['up_db_repl_ip']
        repl_user = pdict['repl_user']
        repl_pass = pdict['repl_pass']
        up_db_host = pdict['up_db_host']
        repl_app_name = pdict['repl_ip']
        db_user = pdict['db_user']
        db_pass = pdict['db_pass']

        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} successful.")

        step = 'check db is running'
        general_task_mgr.log_info(task_id, f"{msg_prefix}: Start {step} ...")

        err_code, err_msg = is_running(rpc, pgdata)
        if err_code != 0:
            return err_code, err_msg
        is_run = err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} successful.")
        general_task_mgr.log_info(task_id, f"{msg_prefix}:  database running state is {is_run}")
        if not is_run:
            step = 'start database'
            general_task_mgr.log_info(task_id, f"{msg_prefix}: Start {step} ...")
            err_code, err_msg = start(rpc, pgdata)
            if err_code != 0:
                return err_code, err_msg
            general_task_mgr.log_info(task_id, f"{msg_prefix}: {step} successful.")
        step = 'stop database'
        err_code, err_msg = stop(rpc, pgdata)
        if err_code != 0:
            return err_code, err_msg
        step = 'pg_rewind'
        db_name = pdict.get('db_name', 'template1')
        # 准备运行pg_rewind的命令
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg

        rewind_cmd = f"""pg_rewind --target-pgdata={pgdata} --source-server="host={up_db_repl_ip} """\
            f"""port={up_db_port} user={repl_user} password={repl_pass} dbname={db_name}" -P"""
        cmd = f"""su - {upw_dict['pw_name']} -c'{rewind_cmd}' """

        # 运行pg_rewind的命令
        general_task_mgr.log_info(task_id, f"{msg_prefix}: run {cmd} ...")
        cmd_id = rpc.run_long_term_cmd(cmd, output_qsize=10, output_timeout=600)
        state = 0
        while state == 0:
            state, err_code, err_msg, stdout_lines, stderr_lines = rpc.get_long_term_cmd_state(cmd_id)
            # state = 1 成功结束,state = 0 还在运行,state = -1运行失败
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

        # 修改流复制的配置文件,如recovery.conf等配置文件
        step = 'Configure stream replication parameters'
        repl_app_name = pdict['repl_app_name']
        general_task_mgr.log_info(task_id, f"{msg_prefix}: Start {step} ...")
        delay = pdict.get('delay')
        pg_major_int_version = int(str(version).split('.')[0])
        err_code, err_msg = set_sr_config_file(
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
        # err_code, err_msg = config_standby_sr(
        #     rpc,
        #     version,
        #     os_user,
        #     pgdata,
        #     repl_app_name,
        #     repl_user,
        #     repl_pass,
        #     up_db_repl_ip,
        #     up_db_port
        # )
        if err_code != 0:
            return err_code, err_msg
        general_task_mgr.log_info(task_id, f"{msg_prefix}: {step}成功.")

        err_code, err_msg = start(rpc, pdict['pgdata'])
        if err_code != 0:
            return err_code, err_msg
        # check pg_isready if err_code=1, database is in starting maybe recovering wal
        err_code, err_msg = is_ready(rpc, pdict['pgdata'], pdict['port'])
        while err_code == 1:
            err_code, err_msg = is_ready(rpc, pdict['pgdata'], pdict['port'])
            time.sleep(10)

        step = "Check whether the stream replication of the standby database is normal"
        sql = "SELECT count(*) AS cnt  FROM pg_stat_replication WHERE application_name=%s AND state='streaming'"
        err_code, data = probe_db.run_sql(up_db_host,
                                          up_db_port,
                                          'template1',
                                          db_user,
                                          db_pass,
                                          sql,
                                          (repl_app_name,))
        if err_code != 0:
            return err_code, err_msg

        if data[0]['cnt'] == 1:
            err_code = 0
            err_msg = ''
            return err_code, err_msg
        else:
            err_code = -1
            err_msg = "the stream replication status is abnormal"
            return err_code, err_msg
    except Exception:
        err_code = -1
        err_msg = f"{msg_prefix}: {step} unknown error occurred: {traceback.format_exc()}"
        return err_code, err_msg
    return err_code, err_msg


def lsn_to_int(lsn):
    cells = lsn.split('/')
    return (int(cells[0], 16) << 32) + int(cells[1], 16)


def int_to_lsn(num):
    log_id = num // (2 ** 32)
    log_seg = num % (2 ** 32)
    return "%X/%08X" % (log_id, log_seg)


@rpc_or_host
def pgdata_version(rpc, pgdata):
    """获得数据库的数据目录的版本,注意数据目录的版本只有大版本号,即9.5,9.6,10,11,12,13,14,而不会是9.5.6,9.6.19,10.12,13.6

    Args:
        rpc ([type]): [description]
        pgdata ([type]): [description]

    Returns:
        [type]: [description]
    """

    ver_file = f'{pgdata}/PG_VERSION'
    is_exists = rpc.os_path_exists(ver_file)
    if not is_exists:
        return -1, "%s not exists!" % ver_file

    err_code, err_msg = rpc.file_read(ver_file)
    if err_code != 0:
        return err_code, err_msg
    data = err_msg
    data = data.strip()
    return 0, data


@rpc_or_host
def pg_change_standby_updb(rpc, pgdata, repl_user, repl_pass, repl_app_name, up_db_host, up_db_port):
    err_code, version = pgdata_version(rpc, pgdata)
    if err_code != 0:
        return err_code, version

    pg_major_int_version = int(str(version).split('.')[0])
    err_code, err_msg = set_sr_config_file(rpc, pg_major_int_version, repl_user, repl_pass, up_db_host, up_db_port, repl_app_name, pgdata)
    if err_code != 0:
        return err_code, err_msg
    err_code, err_msg = restart(rpc, pgdata)
    return 0, ''


@rpc_or_host
def modify_pg_conf(rpc, pgdata, setting_dict, is_pg_auto_conf=False):
    try:
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg
        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        _upw_dict = err_msg
        if is_pg_auto_conf:
            conf = f'{pgdata}/postgresql.auto.conf'
        else:
            conf = f'{pgdata}/postgresql.conf'
        rpc.modify_config_type1(conf, setting_dict, is_backup=False)
    except Exception as e:
        return -1, str(e)
    err_code, err_msg = is_running(rpc, pgdata)
    if err_code != 0:
        return err_code, err_msg

    is_run = err_msg
    if not is_run:
        return 0, ''
    # cmd = f"su - {upw_dict['pw_name']} -c'pg_ctl reload -D {pgdata}' "
    # err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
    return err_code, err_msg


def add_sync_standby_name(s, new_name):
    try:
        if re.findall(r'\d+.*?\((.*?)\)', s):
            # "' 2 (stb51, stb52)'"  | "'ANY 2 (stb51, stb52)'"
            res = re.findall(r'\d+.*?\((.*?)\)', s)[0]
            li = [i.strip() for i in res.split(',')]
            if f'"{new_name}"' not in li:
                li.append(f'"{new_name}"')
            new_str = s.replace(res, ','.join(li))
        else:
            # "'stb51, stb52'"
            new_s = s.replace('"', '').replace("'", '').strip()
            li = [i.strip() for i in s.split(',')]
            if len(li) <= 1:
                print(1)
                if new_s != new_name.strip() and new_s:
                    new_str = f'\'"{new_s}", "{new_name}"\''
                else:
                    new_str = f'\'"{new_name}"\''
            else:
                if f'\'"{new_name}"\'' not in li:
                    li.insert(-1, f'\'"{new_name}"\'')
                new_str = ', '.join(li)
    except Exception as e:
        return -1, repr(e)
    return 0, new_str


@rpc_or_host
def change_to_sync(rpc, pgdata, repl_app_name):
    config_file = f"{pgdata}/postgresql.auto.conf"
    err_code, item_dict = rpc.read_config_file_items(config_file, ['synchronous_standby_names'])
    if err_code != 0:
        return -1, item_dict

    if 'synchronous_standby_names' not in item_dict:
        config_file = f"{pgdata}/postgresql.conf"
        err_code, item_dict = rpc.read_config_file_items(config_file, ['synchronous_standby_names'])
        if err_code != 0:
            return -1, item_dict

    val = item_dict.get('synchronous_standby_names')
    if val:
        err_code, val = add_sync_standby_name(val, repl_app_name)
        if err_code != 0:
            return -1, val, ''
    else:
        val = f"'\"{repl_app_name}\"'"
    item_dict = {
        'synchronous_standby_names': val
    }
    rpc.modify_config_type1(config_file, item_dict, is_backup=False)


@rpc_or_host
def change_async_to_sync(rpc, pgdata):
    postgresql_conf = pgdata + '/postgresql.conf'
    try:
        if not rpc.os_path_exists(postgresql_conf):
            return -1, f"file {postgresql_conf} not exists!"
        cmd = f"""sed -i "s/#(clup_change)synchronous_standby_names/synchronous_standby_names/g" {postgresql_conf}"""
        rpc.run_cmd_result(cmd)
        return 0, ''
    except Exception as e:
        return -1, str(e)


@rpc_or_host
def change_sync_to_async(rpc, pgdata):
    postgresql_conf = pgdata + '/postgresql.conf'
    try:
        if not rpc.os_path_exists(postgresql_conf):
            return -1, "file %s not exists!" % postgresql_conf
        cmd = f"""sed -i "s/^synchronous_standby_names/#(clup_change)synchronous_standby_names/g" {postgresql_conf} """
        rpc.run_cmd_result(cmd)
        return 0, ''
    except Exception as e:
        return -1, str(e)


@rpc_or_host
def get_db_port(rpc, pgdata):
    """
    读取postgresql配置文件,获取端口号
    :param pgdata:
    :return:
    """
    try:

        config_file = f"{pgdata}/postgresql.auto.conf"
        err_code, item_dict = rpc.read_config_file_items(config_file, ['port'])
        if err_code != 0:
            return -1, item_dict
        if 'port' not in item_dict:
            config_file = f"{pgdata}/postgresql.conf"
            err_code, item_dict = rpc.read_config_file_items(config_file, ['port'])
            if err_code != 0:
                return -1, item_dict
        port = item_dict.get('port', '').replace('\'', '')
        if port == '':
            port = 5432
        else:
            port = int(port)
        return 0, port
    except Exception as e:
        return -1, str(e)


@rpc_or_host
def extend_db(rpc, pgdata, conn, mem):

    postgresql_conf = '%s/postgresql.auto.conf' % pgdata
    item_dict = {
        'max_connections': conn,
        'shared_buffers': f"{mem}MB"
    }
    rpc.modify_config_type1(postgresql_conf, item_dict, is_backup=False)
    return 0, ''


@rpc_or_host
def delete_db(rpc, pgdata):
    try:
        if pgdata == '/':
            return -1, f"invalid pgdata({pgdata})"
        if not rpc.os_path_exists(pgdata):
            return 0, ''
        err_code, err_msg = rpc.os_stat(pgdata)
        if err_code != 0:
            return err_code, err_msg
        fs = err_msg

        err_code, err_msg = rpc.pwd_getpwuid(fs['st_uid'])
        if err_code != 0:
            return err_code, err_msg
        upw_dict = err_msg

        os_user = upw_dict['pw_name']
        cmd = f""" su - {os_user}  -c 'pg_ctl stop -D {pgdata} -m immediate' """
        err_code, err_msg, _out_msg = rpc.run_cmd_result(cmd)
        tbl_root_path = os.path.join(pgdata, 'pg_tblspc')
        if rpc.os_path_exists(tbl_root_path):
            tbl_oid_list = rpc.os_listdir(tbl_root_path)
            for tbl_oid in tbl_oid_list:
                err_code, err_msg = rpc.os_readlink(os.path.join(tbl_root_path, tbl_oid))
                if err_code != 0:
                    continue
                tbl_real_path = err_msg
                if rpc.os_path_exists(tbl_real_path):
                    rpc.delete_file(tbl_real_path)
        rpc.delete_file(pgdata)
        return 0, 'Delete success'
    except Exception as e:
        return -1, str(e)


# FIXME: 此函数没有使用,后续使用时需要优化内容
@rpc_or_host
def modify_recovery(rpc, pgdata, repl_app_name, repl_user, repl_pass, repl_ip, primary_port):
    recovery_file = '%s/recovery.conf' % pgdata
    recovery_done_file = "%s/recovery.done" % pgdata
    primary_conninfo = "'application_name=%s user=%s host=%s port=%s password=%s sslmode=disable sslcompression=1'" \
                       % (repl_app_name, repl_user, repl_ip, primary_port, repl_pass)
    primary_conninfo_dict = {
        "application_name": repl_app_name,
        "user": repl_user,
        "host": repl_ip,
        "port": primary_port,
        "password": repl_pass,
    }

    # 根据recovery.conf或者recovery.done文件生成recovery.conf
    file_name = ""
    if rpc.os_path_exists(recovery_file):
        file_name = recovery_file
    elif rpc.os_path_exists(recovery_done_file):
        file_name = recovery_done_file

    if file_name == recovery_done_file:
        cmd = """cp %s %s""" % (recovery_done_file, recovery_file)
        err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)

        if err_code == 0:
            file_name = recovery_file
        else:
            file_name = ''

    if file_name:
        err_code, ret_dict = rpc.read_config_file_items(file_name, ['standby_mode', 'primary_conninfo'])
        if err_code == 0:
            old_primary_conninfo_str = ret_dict.get('primary_conninfo', '')[1:-1]
            if old_primary_conninfo_str:
                err_code, err_msg = merge_primary_conninfo_str(old_primary_conninfo_str, primary_conninfo_dict)
                if err_code != 0:
                    return err_code, err_msg
                new_primary_conninfo = err_msg
                primary_conninfo = new_primary_conninfo if new_primary_conninfo else primary_conninfo
                ret_dict['primary_conninfo'] = primary_conninfo
                rpc.modify_config_type1(recovery_file, ret_dict, is_backup=False)
    else:
        content = f"standby_mode = 'on'\nrecovery_target_timeline = 'latest'\nprimary_conninfo={primary_conninfo}"
        try:
            with open(recovery_file, "w") as fp:
                fp.write(content)
        except Exception as e:
            return -1, str(e), ''
    example_file = "%s/postgresql.conf" % pgdata
    cmd = "ls -l %s " % example_file
    err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
    if err_code != 0:
        return err_code, err_msg, out_msg
    if not out_msg:
        return -1, 'Permission modification failed, no result output; cmd: %s' % cmd
    ret_list = out_msg.strip().split(' ')
    cmd = f"touch {recovery_file} && chmod 600 {recovery_file} && chown {ret_list[2]}:{ret_list[3]}  {recovery_file}"
    err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
    return err_code, err_msg


@rpc_or_host
def set_pg_data_dir_mode(rpc, os_user, pgdata):
    """
    设置PG数据目录的属主和权限
    """
    err_code = 0
    err_msg = ''

    err_code, err_msg = rpc.pwd_getpwnam(os_user)
    if err_code != 0:
        return err_code, err_msg
    upw_dict = err_msg
    try:
        err_code, err_msg = rpc.os_chown(pgdata, upw_dict['pw_uid'], upw_dict['pw_gid'])
        if err_code != 0:
            return err_code, err_msg
    except Exception:
        err_code = -1
        return err_code, traceback.format_exc()
    # 把目录的mode改成0700
    try:
        err_code, err_msg = rpc.os_chmod(pgdata, 0o700)
        if err_code != 0:
            return err_code, err_msg
    except Exception:
        err_code = -1
        return err_code, traceback.format_exc()

    # 检查上级目录对用户是否有读和执行的权限,如数据库建立在/data/child1/pgdata下,
    #   即使/data/child1/pgdata中的pgdata目录对用户有权限,但是上级目录/data/child1对数据库没有权限,数据库也是无法启动的。
    #   所以我们需要对上级目录进行赋权,方式如下：
    #     如果目录的属主或组是此用户,则加相应的权限
    #     如果目录的组和属主不是此用户而又没有权限,则直接修改目录的属主为此用户

    # 获得用户的所有的组ID
    err_code, err_msg = rpc.grp_getgrall()
    if err_code != 0:
        return err_code, err_msg

    grp_list = err_msg
    gids = [g['gr_gid'] for g in grp_list if os_user in g['gr_mem']]
    gids.append(upw_dict['pw_gid'])
    par_path = os.path.abspath(os.path.join(pgdata, os.pardir))
    while par_path != '/':
        err_code, err_msg = rpc.os_stat(par_path)
        if err_code != 0:
            return err_code, err_msg
        st = err_msg
        if st['st_uid'] == upw_dict['pw_uid']:  # 如果目录的属主是数据库的OS_USER,则检查目录对此用户是否有可执行权限和只读权限,如果没有则加上
            # 需要有执行和读的权限：
            if not ((st['st_mode'] & stat.S_IXUSR) and (st['st_mode'] & stat.S_IRUSR)):
                # 增加执行和读的权限
                err_code, err_msg = rpc.os_chmod(par_path, st['st_mode'] | stat.S_IXUSR | stat.S_IRUSR)
                if err_code != 0:
                    return err_code, err_msg
        elif st['st_gid'] in gids:  # 如果目录的组是用户的一个组,则看此组是否有执行和只读权限,如果没有则加上
            # 需要有执行和读的权限：
            if not ((st['st_mode'] & stat.S_IXGRP) and (st['st_mode'] & stat.S_IRGRP)):
                # 增加执行和读的权限
                err_code, err_msg = rpc.os_chmod(par_path, st['st_mode'] | stat.S_IXGRP | stat.S_IRGRP)
                if err_code != 0:
                    return err_code, err_msg
        else:  # 如果目录的属主与组与是用户没有关系,则此目录加上其他用户有读和执行的权限
            # 需要有执行和读的权限：
            if not ((st['st_mode'] & stat.S_IXOTH) and (st['st_mode'] & stat.S_IROTH)):
                # 直接修改目录的属主为此用户
                # err_code, err_msg = rpc.os_chown(par_path, upw_dict['pw_uid'], upw_dict['pw_gid'])
                # if err_code != 0:
                #    return err_code, err_msg
                err_code, err_msg = rpc.os_chmod(par_path, st['st_mode'] | stat.S_IXOTH | stat.S_IROTH)
                if err_code != 0:
                    return err_code, err_msg
        par_path = os.path.abspath(os.path.join(par_path, os.pardir))
    return err_code, err_msg



def check_pg_version(pg_version):
    """检查字符串是否是一个PG的版本号
    """
    cells = pg_version.split('.')
    if len(cells) <= 1:  # 版本号至少有两个数字组成
        return False
    for k in cells:
        # 以.分割的各个部分,必须是一个数字
        if not k.isdigit():
            return False
    return True


@rpc_or_host
def get_pg_bin_version(rpc, pg_bin_path):
    """从PG的软件中获得版本号

    Args:
        pg_bin_path ([type]): [description]

    Returns:
        [type]: [description]
    """
    err_msg = 'can not get pg version: '
    pg_version = ''

    # /usr/pgsql-9.6/include/server/pg_config_x86_64.h 中 :#define PG_VERSION "9.6.17"
    pg_config_h_file = f"{pg_bin_path}/../include/pg_config_x86_64.h"
    if not rpc.os_path_exists(pg_config_h_file):
        # FIXME: ARM平台的此文件名可能不正确
        pg_config_h_file = f"{pg_bin_path}/../include/pg_config.h"

    if rpc.os_path_exists(pg_config_h_file):
        err_code, err_msg = rpc.file_read(pg_config_h_file)
        if err_code != 0:
            return err_code, err_msg
        data = err_msg
        lines = data.split('\n')
        for line in lines:
            m = re.match(r'#define\sPG_VERSION\s"([\d.]+)"', line)
            if m:
                pg_version = m.group(1)
                # 如果匹配到的字符串不是PG的版本号,则继续搜索
                if check_pg_version(pg_version):
                    return 0, pg_version
    else:
        err_code = -1
        err_msg = f"{pg_config_h_file} not exist!"

    # 通过执行pg_config获得版本号
    cmd = f'{pg_bin_path}/pg_config --version'
    err_code, err_msg, out_msg = rpc.run_cmd_result(cmd)
    if err_code == 0:
        out_msg = out_msg.strip()
        cells = out_msg.split()
        for pg_version in cells:
            # 如果匹配到的字符串不是PG的版本号,则继续搜索
            if check_pg_version(pg_version):
                return 0, pg_version

    # 如果还没有获得版本号,只能从postgres.bki中获得了,这里面只有主版本号
    # 通过postgres.bki 中的第一行获得版本
    bki_file = f"{pg_bin_path}/../share/postgres.bki"
    if not rpc.os_path_exists(bki_file):
        bki_file = f"{pg_bin_path}/../share/postgresql/postgres.bki"
    if rpc.os_path_exists(bki_file):
        # 只读bki文件的前512字节
        bin_data = rpc.os_read_file(bki_file, 0, 512)
        data = bin_data.decode()
        lines = data.split('\n')
        line = lines[0]
        # postgres.bki第一行的内容类似“# PostgreSQL 9.6”
        # 所以把这一行split后,版本号在最后一列
        cells = line.split()
        pg_major_version = cells[-1]
        try:
            # 这里把版本号转换成浮点数,是为了测试取的版本号是否正确,可以如果不能转换,抛出异常,说明取的有问题,第一行的格式不是我们想象的这样,需要重新修改代码
            _float_version = float(pg_major_version)
            return 0, pg_major_version
        except ValueError:
            err_code = -1
            err_msg = f'can not find major in {bki_file}'
    else:
        err_msg += f' {bki_file} not exists!'
        err_code = -1
    if not pg_version:
        err_code = -1
        if err_msg:
            err_msg = 'can not get version'
    return err_code, err_msg


@rpc_or_host
def pause_standby_walreciver(rpc, db_dict):
    """
    暂停备库的WAL日志接收
    :return: 返回0表示成功暂停备库的WAL日志接收,返回-1表示失败
    """
    try:
        host = db_dict['host']
        pgdata = db_dict['pgdata']
        conf_list = ['wal_retrieve_retry_interval']
        conf_file = pgdata + '/postgresql.conf'
        pg_auto_conf_file = pgdata + '/postgresql.auto.conf'

        err_code, err_msg = rpc.read_config_file_items(conf_file, conf_list)
        if err_code != 0:
            return -1, err_msg
        conf_data = err_msg

        is_exists = rpc.os_path_exists(pg_auto_conf_file)
        auto_conf_data = None
        if is_exists:
            err_code, err_msg = rpc.read_config_file_items(pg_auto_conf_file, conf_list)
            if err_code != 0:
                return -1, err_msg
            auto_conf_data = err_msg

        # 记录下旧的配置, 如果最后old_conf是None,则表示配置文件中没有配置此参数,认为此参数走的时默认值
        old_conf = None
        if auto_conf_data.get('wal_retrieve_retry_interval', None):
            old_conf = auto_conf_data['wal_retrieve_retry_interval']
        elif conf_data.get('wal_retrieve_retry_interval', None):
            old_conf = conf_data['wal_retrieve_retry_interval']

        # 修改参数值并重载
        port = db_dict['port']
        db_user = db_dict['db_user']
        db_pass = db_encrypt.from_db_text(db_dict['db_pass'])

        sql = "ALTER SYSTEM SET wal_retrieve_retry_interval='1d'"
        err_code, err_msg = probe_db.exec_sql(host, port, 'template1', db_user, db_pass, sql, ())
        if err_code != 0:
            return_msg = "alter wal_retrieve_retry_interval failed"
            return -1, return_msg

        # 重新加载配置
        reload_err_code, _reload_err_msg = reload(rpc, pgdata)
        # reload之后还原配置中的wal_retrieve_retry_interval
        if old_conf:
            if "'" not in old_conf:
                old_conf = f"'{old_conf}'"
            reset_sql = f"ALTER SYSTEM SET wal_retrieve_retry_interval={old_conf}"
        else:
            reset_sql = "ALTER SYSTEM RESET wal_retrieve_retry_interval"

        # 还原wal_retrieve_retry_interval的值
        err_code, err_msg = probe_db.exec_sql(host, port, 'template1', db_user, db_pass, reset_sql, ())
        if err_code != 0:
            return_msg = "reset wal_retrieve_retry_interval faild"
            return -1, return_msg

        if reload_err_code != 0:
            return_msg = "reload db failed"
            return -1, return_msg

        # 获取walreceiver的进程号
        query_pid = 'select pid from pg_stat_get_wal_receiver()'
        err_code, rows = probe_db.run_sql(host, port, 'template1', db_user, db_pass, query_pid, ())
        if err_code != 0:
            return_msg = f"run sql({query_pid}) to get walreceiver pid faild"
            return -1, return_msg

        pid = None
        if rows:
            # 如果备库没有接收wal日志则pid为None
            if rows[0]['pid']:
                pid = int(rows[0]['pid'])

        # 如果存在进程,就kill掉
        if pid:
            err_code, err_msg = rpc.os_kill(pid, signal.SIGTERM)
            if err_code != 0:
                return_msg = f"kill walreceiver process pid({pid}) faild"
                return -1, return_msg

        time.sleep(1)
        # 检查进程是否已经kill掉
        err_code, rows = probe_db.run_sql(host, port, 'template1', db_user, db_pass, query_pid, ())
        if err_code != 0:
            return_msg = f"run sql({query_pid}) to check walreceiver pid faild"
            return -1, return_msg

        # 如果PID还存在,等待3s再查一次
        if rows[0]['pid']:
            time.sleep(3)
            err_code, rows = probe_db.run_sql(host, port, 'template1', db_user, db_pass, query_pid, ())
            if rows[0]['pid']:
                return_msg = f"kill walreceiver process pid({pid}) faild"
                return -1, return_msg

        return 0, 'pause standby walreciver success'
    except Exception:
        return_msg = f"Unexpected error occurred: {traceback.format_exc()}"
        return -1, return_msg
