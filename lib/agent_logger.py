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
@description: Agent操作日志模块
"""

import logging
import rpc_utils


def get_log_type_list():
    return ["", "csurpc"]


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


def query_agent_log_level(ip, log_type_list, conn_timeout=1):
    """[summary]

    Args:
        ip ([type]): [description]
    """
    ret_log_level_dict = dict()
    for log_type in log_type_list:
        ret_log_level_dict[log_type] = 'unknown'

    # connect the host
    err_code, err_msg = rpc_utils.get_rpc_connect(ip, conn_timeout)
    if err_code != 0:
        return ret_log_level_dict
    rpc = err_msg
    try:
        for log_type in log_type_list:
            err_code, err_msg = rpc.get_log_level(log_type)
            # if not get the log level,skip
            if err_code != 0:
                continue
            ret_log_level_dict[log_type] = err_msg
        return ret_log_level_dict
    except Exception as e:
        logging.error(repr(e))
        return ret_log_level_dict
    finally:
        if rpc:
            rpc.close()