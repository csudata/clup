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
@description: 日志模块
"""

import logging
import logging.handlers

g_stdout_handle = None
g_file_handle = None


class MultiformatFormatter:
    """实现debug级别下打印文件名和行号,其它级别不打印

    Args:
        logging ([type]): [description]
    """
    def __init__(self):
        # super().__init__(self)
        self.common_fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        self.debug_fmt = logging.Formatter('%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s')

    def format(self, record):
        if record.levelno <= logging.DEBUG and record.name != 'csurpc':
            msg = self.debug_fmt.format(record)
        else:
            msg = self.common_fmt.format(record)
        return msg.replace('\n', '\n    ')


class MyLogHandler(logging.handlers.RotatingFileHandler, object):
    """实现日志中如果有换行，在行前加4个空格

    Args:
        logging ([type]): [description]
        object ([type]): [description]
    """


    def __init__(self, *args, **kwargs):
        logging.handlers.RotatingFileHandler.__init__(self, *args, **kwargs)

    def emit(self, record):

        if isinstance(record.args, tuple):
            new_args = []
            for arg in record.args:
                # 如果参数是字符串，则把其中的新行前加上四个空格
                if isinstance(arg, (str, )):
                    new_args.append(arg.replace('\n', '\n    '))
                else:
                    new_args.append(arg)
            record.args = tuple(new_args)
        logging.handlers.RotatingFileHandler.emit(self, record)


def init(level, log_file: str, max_bytes=10 * 1024 * 1024, backup_count=5):
    """
    初使化日志，让日志即能屏幕输出，也可以输出到日志文件中
    @param level: 日志级别，可以取的值为logging.INFO, logging.DEBUG等等值
    @return:
    """
    global g_stdout_handle
    global g_file_handle


    logger = logging.getLogger()

    # formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    formatter = MultiformatFormatter()

    g_stdout_handle = logging.StreamHandler()
    g_stdout_handle.setFormatter(formatter)
    logger.addHandler(g_stdout_handle)

    # file_handle = logging.handlers.RotatingFileHandler(log_file, encoding='UTF-8', maxBytes=10 * 1024 * 1024, backupCount=5)
    g_file_handle = MyLogHandler(log_file, encoding='UTF-8', maxBytes=max_bytes, backupCount=backup_count)
    g_file_handle.setFormatter(formatter)
    logger.addHandler(g_file_handle)

    logger.setLevel(level)

    csurpc_logger = logging.getLogger('csurpc')
    csurpc_logger.setLevel(logging.WARNING)

    csuhttpd_logger = logging.getLogger('csuhttpd')
    csuhttpd_logger.setLevel(logging.WARNING)

    backup_logger = logging.getLogger('backup')
    backup_logger.setLevel(logging.WARNING)



def reinit(level, log_file: str, max_bytes=10 * 1024 * 1024, backup_count=5):
    """
    重新初使化日志，主要给probe service使用
    @param level: 日志级别，可以取的值为logging.INFO, logging.DEBUG等等值
    @return:
    """
    global g_stdout_handle
    global g_file_handle

    logger = logging.getLogger()

    # formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    formatter = MultiformatFormatter()
    logger.removeHandler(g_file_handle)

    g_file_handle = MyLogHandler(log_file, encoding='UTF-8', maxBytes=max_bytes, backupCount=backup_count)
    g_file_handle.setFormatter(formatter)
    logger.addHandler(g_file_handle)

    logger.setLevel(level)

    csurpc_logger = logging.getLogger('csurpc')
    csurpc_logger.setLevel(logging.WARNING)

    csuhttpd_logger = logging.getLogger('csuhttpd')
    csuhttpd_logger.setLevel(logging.WARNING)

    backup_logger = logging.getLogger('backup')
    backup_logger.setLevel(logging.WARNING)


def get_log_type_list():
    return ["", "csurpc", "csuhttpd"]


def get_log_level_name_dict():
    log_level_name_dict = {
        logging.DEBUG: "debug",
        logging.INFO: "info",
        logging.WARN: "warn",
        logging.ERROR: "error",
        logging.CRITICAL: "critical",
    }
    return log_level_name_dict


def get_log_level_dict():
    log_level_dict = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARN,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    return log_level_dict
