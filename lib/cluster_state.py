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
@description: 集群状态
"""

OFFLINE = 0
NORMAL = 1
REPAIRING = 2   # 正在修复中
FAILOVER = 3  # 故障自动切换中
CHECKING = 4   # 正在检查中
FAILED = -1  # 故障自动修复失败，需要手工修复
CREATE_FAILD = 5  # 创建集群过程失败


__state_dict = {
    -1: "Failed",
    0: "Offline",
    1: "Online",
    2: "Reparing",
    3: "Failover",
    4: "Checking",
    5: "CREATE_FAILD"
}


def to_str(state):
    global __state_dict
    if state in __state_dict:
        return __state_dict[state]
    else:
        return 'Unknown'


def is_valid(state):
    global __state_dict
    if state in __state_dict:
        return True
    else:
        return False


def get_dict():
    global __state_dict
    ret_dict = {}
    ret_dict.update(__state_dict)
    return ret_dict
