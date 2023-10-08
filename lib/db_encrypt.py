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
@description: 密码加解密
"""

import base64

from Crypto.Cipher import AES


_key = '76ca1e79f5ee5ca7'.encode('utf-8')


_map1 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/='
_map2 = 'Rd12KPrTht9LoYg*5knW4pJGvi7eSs-yQNcaEHIuA6fDVB3jMlqU8zwbm+F0xCZXO'

_dict1 = {}
_dict2 = {}
for i in range(65):
    _dict1[_map1[i]] = _map2[i]
    _dict2[_map2[i]] = _map1[i]


def _pad(s):
    BLOCK_SIZE = AES.block_size
    return s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)


def _unpad(s):
    return s[:-ord(s[len(s) - 1:])]


def to_db_text(raw):
    """加密"""
    # python3.6　以上AES的key和加密的密码不支持字符串，需要先encode
    raw = _pad(raw).encode('utf-8')
    cipher = AES.new(_key, AES.MODE_ECB)
    s1 = base64.b64encode(cipher.encrypt(raw)).decode()
    s2 = [_dict1[k] for k in s1]
    return ''.join(s2) + 'A'


def from_db_text(enc):
    """解密"""
    enc = ''.join(_dict2[k] for k in enc[:-1])
    enc = base64.b64decode(enc)
    cipher = AES.new(_key, AES.MODE_ECB)
    return _unpad(cipher.decrypt(enc)).decode('utf8')


def main():
    db_text = 'GDm4NT3k5pC0kyPTe5tDuROOA'
    mystr = 'mypassword'

    print(to_db_text(mystr))
    print(from_db_text(db_text))
    s = 'replica'
    s1 = to_db_text(s)
    s2 = from_db_text(s1)
    print("原始: ", s)
    print("加密: ", s1)
    print("解密: ", s2)


if __name__ == '__main__':
    main()
