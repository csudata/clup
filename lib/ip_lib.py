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
@description: ip管理模块
"""

import os
import re

import run_lib


def get_nic_ip_dict():
    """ 获得网上的各个ip地址的情况
    :return: dict
    返回一个字典，格式如下:
    {
    'lo': {
        'mac': '00:00:00:00:00:00',
        'ipv6': {'::1': 128},
        'ipv4': {'127.0.0.1': 8},
        'mtu': 65536
    },
    'eth0': {
        'mac': '08:00:27:bc:fb:4d',
        'ipv6': {'fe80::a00:27ff:febc:fb4d': 64},
        'ipv4': {'10.0.2.62': 24},
        'mtu': 1500}
    }
    """
    cmd = '/sbin/ip addr'
    content = run_lib.open_cmd(cmd)
    lines = content.split('\n')
    nic_dict = {}
    nic = {}
    ipv4 = {}
    ipv6 = {}
    for line in lines:
        match = re.search(r"\d+: (.*): .* mtu (\d+)", line)
        if match:
            nic_name = match.group(1)
            nic = {'mtu': int(match.group(2))}
            nic_dict[nic_name] = nic
            continue
        # 可能的数据 link/ether bc:97:e1:b5:99:5d brd ff:ff:ff:ff:ff:ff
        # 可能的数据 link/infiniband 80:00:02:09:fe:80:00:00:00:00:00:00:f4:52:14:03:00:40:9e:d2 brd 00:ff:ff:ff:ff:12:40:1b:ff:ff:00:00:00:00:00:00:ff:ff:ff:ff
        match = re.search(r"\s+link/[^ \t]* ([0-9a-fA-F]{2}(:[0-9a-fA-F]{2})*) brd .*", line)
        if match:
            nic['mac'] = match.group(1)
            continue

        match = re.search(r"\s+inet (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/(\d+) .*", line)
        if match:
            if 'ipv4' not in nic:
                ipv4 = dict()
                nic['ipv4'] = ipv4
            ipv4[match.group(1)] = int(match.group(2))
            continue

        # inet6 fe80::a00:27ff:fe58:41f9/64 scope link
        match = re.search(r"\s+inet6 (.*)/(\d+) .*", line)
        if match:
            if 'ipv6' not in nic:
                ipv6 = dict()
                nic['ipv6'] = ipv6
            ipv6[match.group(1)] = int(match.group(2))
            continue
    return nic_dict


def get_ip_in_network(nic_dict, network):
    """
    找到与在这个网络(network是一个网络地址)中的一个IP地址
    :param network: 网络地址
    :return:
    """
    network_num = ipv4_to_num(network)
    for nic in nic_dict:
        if 'ipv4' not in nic_dict[nic]:
            continue
        ipv4_dict = nic_dict[nic]['ipv4']
        if 'mac' not in nic_dict[nic]:
            continue
        mac = nic_dict[nic]['mac']
        for ip in ipv4_dict:
            netmask_len = ipv4_dict[ip]
            netmask_num = int('1' * netmask_len + '0' * (32 - netmask_len), 2)
            ip_num = ipv4_to_num(ip)
            if ip_num & netmask_num == network_num:
                return ip, mac
    return None, None


def add_vip_to_nic(nic, vip):
    """ 把vip加到指定的网卡上
    """

    # 如果网卡名为“eth0@if2”，在加ip时，只能把eth0加
    cells = nic.split('@')
    vip_nic = cells[0]

    cmd = f'/sbin/ip addr add {vip}/32 dev {vip_nic}'
    run_lib.run_cmd(cmd)
    if os.path.exists('/usr/bin/apt'):  # 这是ubuntu，ubuntu下的arping没有-u选项.
        cmd = f'/usr/sbin/arping -q -c 3 -I {vip_nic} {vip}'
    else:
        cmd = f'/sbin/arping -q -U -c 3 -I {vip_nic} {vip}'
    run_lib.run_cmd(cmd)


def del_vip_from_nic(nic, vip):
    """ 把vip从指定的网卡上删除掉
    """

    # 如果网卡名为“eth0@if2”，在删除vip时，只能用eth0，不能问个问题eth0@if2
    cells = nic.split('@')
    vip_nic = cells[0]
    cmd = f'/sbin/ip addr del {vip}/32 dev {vip_nic}'
    run_lib.run_cmd(cmd)


def vip_exists(vip):
    """
    探测本机是否有vip，
    :return: 如果不存在返回空字符串''，如果存在返回其所在的网卡名称
    """

    nic_dict = get_nic_ip_dict()

    vip_nic = ''
    for nic in nic_dict:
        if 'ipv4' in nic_dict[nic] and vip in nic_dict[nic]['ipv4']:
            vip_nic = nic
            break
    return vip_nic


def check_and_del_vip(vip):
    """
    删除本机上的vip地址
    :return: 如果vip存在，返回vip所在的网卡名，否则返回空字符串''
    """
    vip_nic = vip_exists(vip)
    # 如果vip已存在，则删除掉
    if vip_nic:
        del_vip_from_nic(vip_nic, vip)
    return vip_nic


def ipv4_to_num(ip4):
    cells = ip4.split('.')
    return int(cells[0]) * 256 * 256 * 256 + int(cells[1]) * 256 * 256 + int(cells[2]) * 256 + int(cells[3])


def check_and_add_vip(vip):
    """
    如果vip不存在，则添加，如果存在，则忽略
    :return: 0表示成功，1表示vip已存在，-1表示出错
    """

    nic_dict = get_nic_ip_dict()

    vip_nic = ''
    for nic in nic_dict:
        if 'ipv4' in nic_dict[nic] and vip in nic_dict[nic]['ipv4']:
            vip_nic = nic
            break

    # 如果vip存在，则退出
    if vip_nic:
        return 1

    # vip不存在，需要把vip加上，但加之前，需要知道加到哪个网卡上，而这个网卡就是上面的IP地址与vip是在一个网段内
    vip_nic = ''
    for nic in nic_dict:
        ipv4_dict = nic_dict[nic]['ipv4']
        for ip in ipv4_dict:
            net_mask_len = ipv4_dict[ip]
            net_mask = (2**32 - 1 - (2**(32 - net_mask_len) - 1))
            ip_num = ipv4_to_num(ip)
            vip_num = ipv4_to_num(vip)
            # 与网络掩码做与计算后得到网络地址，如果网络地址相同，说明是在同一个网段内。
            if (ip_num & net_mask) == (vip_num & net_mask):
                vip_nic = nic
                break
        if vip_nic:
            break

    if not vip_nic:
        return -1
    add_vip_to_nic(vip_nic, vip)
    return 0


