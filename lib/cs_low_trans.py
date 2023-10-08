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
@description: RPC调用的底层数据包传输模块
"""

import errno
import hashlib
import random
import select
import socket
import struct
import time
from typing import Tuple

# 在底层的命令中，都发送这个前缀，如果发现收到的包不是这几个字，则表明不是csrpc的的调用
magic = b'UHSGNEHCCPR'
magic_len = len(magic)

CMD_AUTH = 0


def send_data(sock: socket, data: bytes, timeout:int) -> Tuple[int, str]:
    """
    发送数据，会把所有的数据发送完
    :param sock    : socket对象
    :param data    : 要发送的数据
    :param timeout : 超时时间
    :return: 返回值有两个，第一个是错误码，0表示成功, 1表示超时，-1表示出错，第二个是错误信息
    :rtype : int, string
    """

    left_size = len(data)
    read_list = []
    write_list = [sock]
    while left_size > 0:
        try:
            _rs, ws, _es = select.select(read_list, write_list, read_list, timeout)
        except select.error as e:
            if e.args[0] == errno.EINTR:  # 这是收到信号打断了select函数
                continue
            return -1, repr(e)
        if not ws:
            return 1, 'timeout'
        try:
            ret = sock.send(data)
        except socket.error as e:
            return -1, e.strerror
        left_size -= ret
        if left_size > 0:
            data = data[ret:]
    return 0, ''


def recv_data(sock: socket, need_len: int, timeout: int) -> Tuple[int, str, bytes]:
    """
    接收数据，直到接收到指定长度的数据才会返回
    :param sock    : socket对象
    :param need_len: 要接收数据的长度
    :param timeout : 超时时间
    :return: 返回值有三个，第一个是错误码，0表示成功, 1表示超时，-1表示出错，第二个是错误信息，第三个是接收到的数据
    :rtype : (int, string, bytes)
    """

    recv_size = 0
    data = b''
    read_list = [sock]
    write_list = []

    while recv_size < need_len:
        try:
            rs, _ws, _es = select.select(read_list, write_list, read_list, timeout)
        except select.error as e:
            if e.args[0] == errno.EINTR:  # 这是收到信号打断了select函数
                continue
            return -1, repr(e), b''
        if not rs:
            return 1, 'timeout', b''
        try:
            segment = sock.recv(need_len - recv_size)
        except socket.error as e:
            return -1, e.strerror, data
        if not segment:  # 没有接收到任何数据，则表明对方的socket可能关闭了
            return -1, 'socket maybe closed', data
        recv_size += len(segment)
        data += segment

    return 0, '', data


def send_cmd(sock: socket, cmd: int, data: bytes, timeout: int) -> Tuple[int, str, int, bytes]:
    """
    发送命令
    :param sock    : socket对象
    :param cmd     : 命令类型，是一个整数
    :param data    : 要发送的数据
    :param timeout : 超时时间
    :return: (err, msg, ret_code, ret_data) err是错误码，0表示成功, 1表示超时，-1表示出错，msg是错误信息, data是返回的数据
    :rtype : int, string, int, bytes
    """

    global magic
    global magic_len

    len_data = len(data)
    fmt = "!%dsiI%ds" % (magic_len, len_data)
    raw = struct.pack(fmt, magic, cmd, len_data, data)
    err, msg = send_data(sock, raw, timeout)
    if err:
        return err, msg, 0, b''

    err, msg, raw = recv_data(sock, magic_len+8, timeout)
    if err:
        return err, f'after send_cmd, recv reply header failed: {msg}', 0, b''
    fmt = "!%dsiI" % magic_len
    recv_magic, ret_code, data_len, = struct.unpack(fmt, raw)
    if recv_magic != magic:
        return -2, 'Invalid packet format!', -1, ''
    if data_len > 0:
        err, msg, raw = recv_data(sock, data_len, 2)
        if err:
            return err, f'after send_cmd, recv reply body(len={data_len}) failed: {msg}', 0, b''
    else:
        raw = b''
    return 0, '', ret_code, raw


def recv_cmd(sock: socket, timeout: int) -> Tuple[int, str, int, bytes]:
    """
    接收命令
    :param sock    : socket对象
    :param timeout : 超时时间
    :return: (err, msg, cmd, data), err是错误码，0表示成功, 1表示超时，-1表示出错，msg是错误信息, data是返回的数据
    :rtype : int, string, int, bytes
    """
    global magic
    global magic_len

    err, msg, raw = recv_data(sock, magic_len+8, timeout)
    if err:
        return err, msg, -1, b''

    fmt = "!%dsiI" % magic_len
    recv_magic, cmd, len_data, = struct.unpack(fmt, raw)
    if recv_magic != magic:
        return -2, 'Invalid packet format!', ''

    if len_data > 0:
        err, msg, raw = recv_data(sock, len_data, timeout)
        if err:
            return err, msg, -1, b''
        return 0, '', cmd, raw
    else:
        return 0, '', cmd, b''


def reply_cmd(sock: socket, ret_code: int, ret_data: bytes, timeout: int) -> Tuple[int, str]:
    """
    接收到命令之后，返回响应
    :param sock     : socket对象
    :param ret_code : 返回码
    :param ret_data : 返回数据
    :param timeout  : 超时时间
    :return: (err, msg), err是错误码，0表示成功, 1表示超时，-1表示出错，msg是错误信息
    :rtype : int, string
    """

    global magic
    global magic_len

    data_len = len(ret_data)
    if data_len > 0:
        fmt = "!%dsiI%ds" % (magic_len, data_len)
        raw = struct.pack(fmt, magic, ret_code, data_len, ret_data)
    else:
        fmt = "!%dsiI" % magic_len
        raw = struct.pack(fmt, magic, ret_code, 0)
    err, msg = send_data(sock, raw, timeout)
    return err, msg


def connect(ip: str, port: int, password: str, conn_timeout: int, data_timeout: int) -> Tuple[int, str, object]:
    """
    客户端连接服务端进行验证
    :param ip: 服务的ip地址
    :param port: 端口
    :param password: HA服务的密码
    :param timeout:
    :return: (err, msg, sock)
    """

    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # sock.connect((ip, port))
        sock = socket.create_connection((ip, port), conn_timeout)
    except Exception as e:
        return -1, str(e), None
    err, msg, raw = recv_data(sock, magic_len+64, data_timeout)
    if err:
        sock.close()
        return -1, msg, None

    if raw[:magic_len] != magic:
        sock.close()
        return -2, 'Invalid packet format!', None

    # 把收到的字符串(服务端随机生成的)与本地的密码混合，然后做hash运算，把运算后的字符串(hash_str)发给服务端
    random_str = raw[magic_len:]
    mixed_str = password.encode('utf-8')+random_str
    hash_str = hashlib.sha256(mixed_str).hexdigest()

    raw = magic + hash_str.encode('utf-8')
    err, msg, ret_code, ret_data = send_cmd(sock, CMD_AUTH, raw, data_timeout)
    if err:
        sock.close()
        return err, msg, None
    # print "ret_code", ret_code, "ret_data", ret_data, "sock", sock
    if ret_code:
        sock.close()
        return ret_code, ret_data, None
    return 0, '', sock


def auth_connect(sock: socket, ha_pwd: str, timeout: int) -> Tuple[int, str]:
    """
    服务端验证连接，方法为: 当接后客户端的连接后，马上给客户端返回一个随机字段串random_str，客户端收到这个随机字符串后，用自己的密码与这个
    随机字符串混合后做一个hash运算(sha256)，然后把运算后的字符串(client_hash_str)发送给服务端，服务端也把之前生成的随机字符串和自己本
    地的密码混合也做hash运算，得到字符串server_hash_str，然后与客户端发过来的client_encry_str相比较，如果相同，则验证成功，如果不相
    同，则验证失败。
    :param sock: socket连接
    :param ha_pwd: HA服务的密码
    :param timeout: 超时时间
    :return: (err, msg)，如果err<0，说明发生错误，如果err==1，则表示验证失败，如果err==0，
    """

    random_str = ''.join(random.sample('abcdABCDefghEFGHijklIJKLmnMN'+str(time.time())+'opOPqrstQRSTuvwUVWxyXYzZ', 64))
    random_bytes= random_str.encode('utf-8')
    raw = magic+random_bytes
    err, msg = send_data(sock, raw, timeout)
    if err:
        return err, msg

    err, msg, cmd, raw = recv_cmd(sock, timeout)
    if err:
        return err, msg
    if cmd != CMD_AUTH:
        return -1, "Recv a command not AUTH: cmd=%d" % cmd

    if raw[:magic_len] != magic:
        return -2, 'Invalid packet format!'

    client_hash_str = raw[magic_len:]
    mixed_str = ha_pwd.encode("utf-8")+random_bytes
    server_hash_str = hashlib.sha256(mixed_str).hexdigest().encode("utf-8")
    if client_hash_str != server_hash_str:
        err, msg = reply_cmd(sock, -1, b'Authentication failed', timeout)
        sock.close()
        if err == 0:
            err = 1  # 设置成1，表明验证失败
    else:
        err, msg = reply_cmd(sock, 0, b'Authentication success', timeout)
    return err, msg
