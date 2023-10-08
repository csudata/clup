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
@description: 节点状态
"""

NORMAL = 1
FAULT = 2
FAILOVER = 3  # 节点检测到失败后，正在处于切换的状态。
SWITCHING = 4   # 正在处于手工切换的状态。
REPAIRING = 5   # 正在修复

__state_dict = {
    1: "Normal",
    2: "Fault",
    3: "Failover",
    4: "Switching",
    5: "Repairing"
}


def to_str(state):
    global __state_dict
    if state in __state_dict:
        return __state_dict[state]
    else:
        return 'Unknown'


def get_dict():
    global __state_dict
    ret_dict = {}
    ret_dict.update(__state_dict)
    return ret_dict
