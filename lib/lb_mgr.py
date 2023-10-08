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
@description: 负载均衡管理的模块
"""

import urllib.error
import urllib.request

import config


def add_backend(lb_addr, backend_addr):
    token = config.get("cstlb_token")
    url = f"http://{lb_addr}/backend/add?backend={backend_addr}&token={token}"
    try:
        response = urllib.request.urlopen(url)
        http_code = response.getcode()
        data = response.read()
        if http_code != 200:
            return http_code, data
        return 0, data
    except urllib.error.HTTPError as e:
        return e.getcode(), str(e)
    except urllib.error.URLError as e:
        return -1, str(e)
    except Exception as e:
        return -1, str(e)


def delete_backend(lb_addr, backend_addr):
    token = config.get("cstlb_token")
    url = f"http://{lb_addr}/backend/delete?backend={backend_addr}&token={token}"
    try:
        response = urllib.request.urlopen(url)
        http_code = response.getcode()
        data = response.read()
        if http_code != 200:
            return http_code, data
        return 0, data
    except urllib.error.HTTPError as e:
        return e.getcode(), str(e)
    except urllib.error.URLError as e:
        return -1, str(e)
    except Exception as e:
        return -1, str(e)
