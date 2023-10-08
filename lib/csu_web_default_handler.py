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
@description: http操作的基本模块
"""

import json
import logging
import traceback


from http.cookies import SimpleCookie

logger = logging.getLogger('csuhttpd')

def get_session(req):
    if 'user_name' not in req.form:
        return 400, "lost parameter: user_name"
    user_name = req.form['user_name']
    session_id = req.session_handler.get_session(user_name)
    if not session_id:
        return 400, "this user %s session unassigned!"
    return 200, session_id


def login(req):
    ret_dict = {
        "err_code": 0,
        "err_msg": "success"
    }

    try:
        # cookiestring = "\n".join(req.headers.get_all('Cookie', failobj=[]))
        cookiestring = req.headers.get('cookie')
        c = SimpleCookie()
        c.load(cookiestring)
        session_id = c['session_id'].value
        if not session_id:
            ret_dict['err_code'] = -1
            ret_dict['err_msg'] = "no session id in cookie!"
            return 400, json.dumps(ret_dict)
    except KeyError:
        ret_dict['err_code'] = -1
        ret_dict['err_msg'] = "no session id in cookie!"
        return 400, json.dumps(ret_dict)
    except Exception as e:
        logger.error(traceback.format_exc())
        ret_dict['err_code'] = -2
        ret_dict['err_msg'] = "unexpected error: %s" % str(e)
        return 500, json.dumps(ret_dict)

    if 'user_name' not in req.form:
        ret_dict['err_code'] = -3
        ret_dict['err_msg'] = "lost parameter: user_name"
        return 400, json.dumps(ret_dict)

    if 'hash_value' not in req.form:
        ret_dict['err_code'] = -4
        ret_dict['err_msg'] = "lost parameter: hash_value"
        return 400, json.dumps(ret_dict)

    user_name = req.form['user_name']
    client_hash_value = req.form['hash_value']
    ok, msg = req.session_handler.login(user_name, session_id, client_hash_value)
    if ok:
        user_data = {"id": 1,
                     "username": user_name,
                     "nickname": user_name,
                     "name": user_name,
                     "email": "admin@csudata.com",
                     "err_code": 0,
                     "err_msg": "success"
                     }
        return 200, json.dumps(user_data)
    else:
        return 400, msg


def logout(req):
    try:
        # cookiestring = "\n".join(req.headers.get_all('Cookie', failobj=[]))
        cookiestring = req.headers.get('cookie')
        c = SimpleCookie()
        c.load(cookiestring)
        session_id = c['session_id'].value
        if not session_id:
            return 400, "no session id in cookie!"
    except KeyError:
        return 400, "no session id in cookie!"
    except Exception as e:
        logger.error(traceback.format_exc())
        return 500, "unexpected error: %s" % str(e)
    ok, msg = req.session_handler.logout(session_id)
    if ok:
        return 200, "OK"
    else:
        return 200, msg
