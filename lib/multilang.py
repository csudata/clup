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
@description: 多语言
"""

import config

dict_zh_cn = {
    "the number of active connections to the database is too high": "数据库活动连接数过高",
    "the current number of active connections": "当前活动连接数",
    "total connections to the database is too high": "数据库总连接数过高",
    "current total connections": "当前总连接数",
    "total connections to the database returns to normal": "数据库总连接数恢复正常",
    "the number of active connections to the database returns to normal": "数据库活动连接数恢复正常",

    "database": "数据库",
    "HA status becomes abnormal": "HA状态异常",
    "HA status returns to normal": "HA状态恢复正常",
    "Stopped": "已经停止",
    "Agent connection timeout for all hosts": "所在主机的agent连接超时",
    "status returns to normal": "状态恢复正常",
    "age of transaction is too old": "最旧事务的年龄过大",
    "current age": "当前年龄",
    "age of transaction returns to normal": "最旧事务的年龄恢复正常",
}


def gettext(en_str):
    if config.get('lang') == 'zh-cn':
        return dict_zh_cn.get(en_str)
    else:
        return en_str
