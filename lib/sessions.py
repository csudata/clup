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
@description: session模块
"""

import base64
import hashlib
import os
import threading
import time

import config

# 字典的格式为{"xxxxxxxxxx":{""}}
__session_dict = {}
__lock = threading.RLock()


def get_session(user_name):
    """
    :param user_name:
    :return: string, 返回一个session_id
    """

    session_id = ""
    __lock.acquire()
    try:
        now = time.time()
        if len(__session_dict) >= 10000:
            for i in __session_dict:
                if now >= __session_dict[i][0]:
                    __session_dict.pop(i)
        while True:
            session_id = base64.b64encode(os.urandom(32)).decode()
            if session_id in __session_dict.keys():
                continue
            break
        session_expired_secs = config.getint('session_expired_secs')
        __session_dict[session_id] = {"user_name": user_name, "expired_time": now + session_expired_secs, "status": 0}
    except Exception:
        pass
    finally:
        __lock.release()
    return session_id


def login(user_name, session_id, client_hash_value):
    """
    :param user_name:
    :param session_id:
    :param client_hash_value:
    :return:
    """

    conf_user_name = config.get('http_user')
    if conf_user_name != user_name:
        return False, "用户名或密码错误!"
    if not config.getint('http_auth'):
        return True, "OK"
    password = config.get('http_pass')

    hash256 = hashlib.sha256()
    hash_key = user_name + password + session_id
    hash256.update(hash_key.encode())
    server_hash_value = hash256.hexdigest()

    __lock.acquire()
    try:
        now = time.time()
        if session_id not in __session_dict:
            return False, "session expired!"
        else:
            if time.time() > __session_dict[session_id]['expired_time']:
                __session_dict.pop(session_id)
                return False, "session expired!"

        if client_hash_value != server_hash_value:
            return False, "用户名或密码错误!"

        session_expired_secs = config.getint('session_expired_secs')
        __session_dict[session_id]['expired_time'] = now + session_expired_secs
        __session_dict[session_id]['status'] = 1
        return True, "OK"
    finally:
        __lock.release()


def check_session(session_id, _func_name):
    """检查session是否登录以及此session是否有func_name的权限

    Args:
        session_id ([str]): session_id
        func_name ([str]): 功能名称

    Returns:
        [tuple]: (ret_code, msg)，当ret_code=0 表示成功，-1表示没有登录，1表示无此权限
    """

    session_data = {
        "user_name": "admin",
        "expired_time": 9999999999,
        "status": 1,
        "right": {
            "sys_role": {1: '', 2: ''},
            "right_list": {},
            "password": ''
        },
        "right_load_time": 9999999999
    }
    if not config.getint('http_auth'):
        return 0, session_data

    __lock.acquire()
    try:
        now = time.time()
        if session_id not in __session_dict:
            return -1, "session expired!"
        else:
            if time.time() > __session_dict[session_id]['expired_time']:
                __session_dict.pop(session_id)
                return -1, "session expired!"
        if not __session_dict[session_id]['status']:
            return -1, "session not login!"
        session_expired_secs = config.getint('session_expired_secs')
        __session_dict[session_id]['expired_time'] = now + session_expired_secs
        return 0, session_data
    finally:
        __lock.release()


def session_is_valid(session_id):
    if not config.getint('http_auth'):
        return True, "OK"
    __lock.acquire()
    try:
        if session_id not in __session_dict:
            return False, "session expired!"
        else:
            if time.time() > __session_dict[session_id]['expired_time']:
                __session_dict.pop(session_id)
                return False, "session expired!"
        if not __session_dict[session_id]['status']:
            return False, "session not login!"
        return True, "OK"
    finally:
        __lock.release()


def logout(session_id):

    __lock.acquire()
    try:
        if session_id not in __session_dict:
            return False, "session not exists!"
        __session_dict.pop(session_id)
        return True, "OK"
    finally:
        __lock.release()
