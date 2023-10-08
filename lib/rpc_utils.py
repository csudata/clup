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
@description: rpc工具的库
"""

import logging
import traceback

import config
import csurpc


def get_server_connect(host='127.0.0.1', conn_timeout=5):
    try:
        rpc_pass = config.get('internal_rpc_pass')
        port = config.get('server_rpc_port')
        rpc_address = f"tcp://{host}:{port}"
        c1 = csurpc.Client()
        c1.connect(rpc_address, password=rpc_pass,
        conn_timeout=conn_timeout)
        return 0, c1
    except Exception as e:
        return -1, f"Can not connect {host}: {str(e)}"


def get_rpc_connect(ip, conn_timeout=5, msg_callback=None):
    try:
        rpc_port = config.get('agent_rpc_port')
        rpc_address = f"tcp://{ip}:{rpc_port}"
        c1 = csurpc.Client(msg_callback=msg_callback)
        if msg_callback:
            msg_callback(f"INFO: Connect to {ip}:{rpc_port} ...")
        c1.connect(
            rpc_address, password=config.get('internal_rpc_pass'),
            conn_timeout=conn_timeout)
        if msg_callback:
            msg_callback(f"INFO: Connect to {ip}:{rpc_port} successfully.")
        return 0, c1
    except Exception as e:
        if msg_callback:
            msg_callback(f"ERROR: Can not connect to {ip}:{rpc_port} : err_code=-1, err_msg={str(e)}")
        return -1, f"Can not connect {ip}:{rpc_port} : {str(e)}"


def check_and_add_vip(host, vip):
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not check and add vip({vip}) in host({host}): maybe host is down.")
        return err_code, err_msg
    rpc = err_msg
    try:
        err_code, err_msg = rpc.check_and_add_vip(vip)
        if err_code < 0:
            rpc.close()
            logging.error(f"Can not check add vip({vip}) in host({host}): {err_msg}")
            return err_code, err_msg
    finally:
        rpc.close()
    return 0, ''


def check_and_del_vip(host, vip):
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not check and delete vip({vip}) in host({host}): maybe host is down.")
        return err_code, err_msg
    rpc = err_msg
    try:
        err_code, err_msg = rpc.check_and_del_vip(vip)
        if err_code < 0:
            rpc.close()
            logging.error(f"Can not check and delete vip({vip}) in host({host}): {err_msg}")
            return err_code, err_msg
    finally:
        rpc.close()
    return 0, ''


def pg_cp_delay_wal_from_pri(host, pri_ip, pri_pgdata, stb_pgdata):
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect to {host}: maybe host is down.")
        return err_code, err_msg
    rpc = err_msg

    try:
        # 在rpc.pg_cp_delay_wal_from_pri会把目标数据库stb_pgdata先给停掉
        err_code, err_msg = rpc.pg_cp_delay_wal_from_pri(pri_ip, pri_pgdata, stb_pgdata)
        if err_code != 0:
            rpc.close()
            logging.error(f"Call rpc pg_cp_delay_wal_from_pri failed: {err_msg}")
            return err_code, err_msg
    finally:
        rpc.close()
    return err_code, err_msg


def os_path_exists(host, file_path):
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect to {host} : maybe host is down.")
        return err_code, err_msg

    rpc = err_msg
    try:
        ret = rpc.os_path_exists(file_path)
        return err_code, ret
    finally:
        rpc.close()


def extract_file(host, file_name, tar_path):
    """
    解压tar包
    @param host:
    @param file_name: tar包
    @param tar_path: 解压路径
    @return:
    """
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect to {host}: maybe host is down.")
        return err_code, err_msg
    rpc = err_msg
    try:
        err_code, err_msg = rpc.extract_file(file_name, tar_path)
        if err_code != 0:
            rpc.close()
            logging.error(f"Call rpc extract_file failed: {err_msg}")
            return err_code, err_msg
    finally:
        rpc.close()
    return err_code, err_msg


def read_config_file(host, file_name):
    """
    读取文件内容
    """
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect to {host}: maybe host is down.")
        return err_code, err_msg
    rpc = err_msg
    try:
        file_size = rpc.get_file_size(file_name)
        if file_size < 0:
            return -1, f'Failed to get the file size.(file_name={file_name})'
        try:
            err_code, err_msg = rpc.os_read_file(file_name, 0, file_size)
        except Exception as e:
            err_code = -1
            err_msg = str(e)
        if err_code != 0 and 'function not support' in err_msg:
            err_code, err_msg = rpc.read_file(file_name, 0, file_size)

        if err_code != 0:
            logging.error(f"Call rpc os_read_file failed: {err_msg}")
            return err_code, err_msg
    finally:
        rpc.close()
    return err_code, err_msg


def os_write_file(host, file_name, offset, data):
    """
    读取文件内容
    """
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect to {host}: maybe host is down.")
        return err_code, err_msg

    rpc = err_msg
    try:
        try:
            err_code, err_msg = rpc.os_write_file(file_name, offset, data)
        except Exception as e:
            err_code = -1
            err_msg = str(e)
        if err_code != 0 and 'function not support' in err_msg:
            err_code, err_msg = rpc.write_file(file_name, offset, data)

        if err_code != 0:
            logging.error(f"Call rpc os_write_file failed: {err_msg}")
            return err_code, err_msg
    finally:
        rpc.close()
    return err_code, err_msg


def get_agent_version(host):
    err_code, err_msg = get_rpc_connect(host)
    if err_code != 0:
        logging.error(f"Can not connect to {host}: maybe host is down.")
        return err_code, err_msg
    rpc = err_msg
    try:
        return rpc.get_agent_version()
    finally:
        rpc.close()


def run_rpc_fun(func_name, *args, node_ip=None,
                client=None, **kwargs):
    """
    根据传入的ip(或client)与func_name 调用对应的函数
    node_ip: 连接的rpc主机地址如果为None时, 获取config文件中的server_address
    client: rpc.Client对象, 如果有已经连接的rpc则使用该对象执行func_name
    """
    # 创建rpc连接如果不存在
    if client is None:
        if node_ip is None:
            return -1, "Missing parameter node_ip or client", ""
        err_code, client = get_rpc_connect(node_ip)
        if err_code != 0:
            return err_code, f'Unable to connect to the ip: {node_ip}\n error: {client}', ''

    if not isinstance(client, csurpc.Client):
        return -1, f"client not rpc connection, client: {client}", ''

    func = getattr(client, func_name, None)
    try:
        result = func(*args, **kwargs)
    except Exception:
        err_str = f"call rpc func {func_name} failed with error:\n{traceback.format_exc()}"
        logging.error(err_str)
        return -1, err_str, ''

    # result = run_rpc_fun_by_client(func_name, client, *args, **kwargs)
    # 执行出现错误时 关闭client
    if result[0] == -1 and client is not None:
        client.close()
    # 统一返回3个参数
    if result is None:
        return -1, f"try to run func {func_name} failed without returning", ""
    if len(result) == 2:
        result = (*result, None)
    return result
