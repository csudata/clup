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
@description: 通用工具
"""


def get_unit_size(unit_size):
    """取带K、M、G、T单位的配置项

    @return 返回数字
    """

    if len(unit_size) <= 0:
        return 0

    unit_char = unit_size[-1].lower()
    if unit_char.isdigit():
        num_size = int(unit_size)
    else:
        if unit_char == 't':
            unit_value = 1024 * 1024 * 1024 * 1024
        elif unit_char == 'g':
            unit_value = 1024 * 1024 * 1024
        elif unit_char == 'm':
            unit_value = 1024 * 1024
        elif unit_char == 'k':
            unit_value = 1024
        else:
            unit_value = 1
        num_size = int(float(unit_size[0:-1]) * unit_value)
    return num_size


def get_cpu_cores(cpu_dict):
    """
    获得CPU的核数
    cpu_dict是cat /proc/cpuinfo看到的cpu信息
    :return: 总的物理核数和逻辑核数（即超线程数）, 如果时arm64,没有超线程,则逻辑核数返回为0
    """

    phy_dict = {}
    cpu_cores = 0

    # 计算x86的cpu的物理核数
    for processor in cpu_dict:
        core_dict = cpu_dict[processor]
        if 'physical id' in core_dict and 'core id' in core_dict:  # 这是x86的cpu
            physical_id = core_dict['physical id']
            core_id = core_dict['core id']
            phy_core_id = f'{physical_id},{core_id}'
            phy_dict[phy_core_id] = 1

    if len(phy_dict) > 0:  # x86的cpu有可能开启了超线程,需要计算实际物理核的数目
        cpu_cores = len(phy_dict)
    else:  # arm64的cpu,没有超线程
        cpu_cores = len(cpu_dict)

    cpu_threads = len(phy_dict)
    return cpu_cores, cpu_threads
