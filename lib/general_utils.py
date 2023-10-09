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
@description: agent端工具
"""

import logging
import os
import tarfile
import time
import traceback

import config
import general_task_mgr
import rpc_utils


def os_read_file(file_path, offset, read_len):
    """
    读取除指定的文件
    :return:
    """
    rpc_info = f"os_read_file(file_path={file_path}, offset={offset}, read_len={read_len})"
    logging.info(f"Recv rpc call: {rpc_info}")
    fd = -1
    try:
        fd = os.open(file_path, os.O_RDONLY)
        os.lseek(fd, offset, os.SEEK_SET)
        data = os.read(fd, read_len)
        return 0, data
    except Exception as e:
        logging.error(f"{traceback.format_exc()}")
        return -1, repr(e)
    finally:
        if fd != -1:
            os.close(fd)


def get_file_size(file_path):
    """
    删除指定的文件
    :return:
    """
    rpc_info = f"get_file_size(file_path={file_path})"
    logging.info(f"Recv rpc call: {rpc_info}")
    if not os.path.exists(file_path):
        err_msg = f'file {file_path} is not exists'
        return -1, err_msg
    else:
        file_size = os.path.getsize(file_path)
        return 0, file_size


def upload_file(host, file_name, tar_file_name, block_size=524288):
    """
    agent传输文件,
    file_name: 文件名,绝对路径
    block_size: 单次传输大小,单位字节,默认512KB
    """
    logging.info(f'get file ({file_name}) size')
    err_code, file_size = get_file_size(file_name)
    if err_code != 0:
        return err_code, file_size
    offset = 0
    a = 0
    while True:
        err_code, data = os_read_file(file_name, offset, block_size)
        if err_code != 0:
            return -1, data
        if not data:
            break

        err_code, err_msg = rpc_utils.os_write_file(host, tar_file_name, offset, data)
        if err_code != 0:
            return err_code, err_msg
        offset += block_size
        progress = round(offset / file_size * 100, 4)
        if round(progress, 2) > a:
            print(progress, '%')
            a = round(progress, 2)
    return 0, ''


if __name__ == '__main__':
    pass
    # config.reload()
    # host = '10.197.170.14'
