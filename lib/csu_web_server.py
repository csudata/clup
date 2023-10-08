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
@description: 对外提供web服务
"""

import json
import logging
import os
import socket
import threading
import traceback
import urllib.parse
from http.cookies import SimpleCookie

import csu_http
import csu_web_default_handler

logger = logging.getLogger('csuhttpd')

__v1_ui_handler_attr_list = dir(csu_web_default_handler)
default_handler_func_dict = {}
for __attr in __v1_ui_handler_attr_list:
    __obj = getattr(csu_web_default_handler, __attr)
    if callable(__obj):
        default_handler_func_dict[__attr] = __obj


def http_do_head(req):

    ctype = csu_http.guess_type(req.path)
    real_path = os.path.join(req.web_root, req.path[1:])
    try:
        f = open(real_path, 'rb')
    except OSError:
        csu_http.reply_http(req.conn, 404, "File not found")
        return

    fs = os.fstat(f.fileno())
    f.close()
    hdr = {"Content-Type": ctype, "Last-Modified": csu_http.date_time_string(fs.st_mtime)}
    csu_http.reply_http(req.conn, 200, '', hdr)


def http_do_get(req):
    url_path = req.path.split('?')[0]
    if url_path == '/':
        url_path = '/index.html'
    hdr = {}
    accept_encoding = req.headers.get('accept-encoding')
    ctype = csu_http.guess_type(url_path)
    hdr["content-type"] = ctype
    url_path = urllib.parse.unquote(url_path)
    ori_real_path = os.path.join(req.web_root, url_path[1:])
    real_path = ori_real_path
    if 'gzip' in accept_encoding:
        if ori_real_path[-3:] == '.js' or ori_real_path[-4:] == '.css':
            gz_real_path = '%s.gz' % ori_real_path
            if os.path.exists(gz_real_path):
                hdr['Content-Encoding'] = 'gzip'
                real_path = gz_real_path
    try:
        f = open(real_path, 'rb')
    except OSError:
        csu_http.reply_http(req.conn, 404, "File not found")
        return
    body_data = f.read()
    fs = os.fstat(f.fileno())
    f.close()
    hdr["Last-Modified"] = csu_http.date_time_string(fs.st_mtime)
    csu_http.reply_http(req.conn, 200, body_data, hdr)


def http_do_post(req):
    req.session_id = None
    req.session_data = None
    try:
        http_handler = req.http_handler
        session_handler = req.session_handler
        url_path = req.path
        hdr = req.headers
        if url_path.startswith('/api/v1'):
            prefix = '/api/v1/'
        else:
            csu_http.reply_http(req.conn, 400, "无效的URL!")
            return
        if 'content-length' not in hdr:
            csu_http.reply_http(req.conn, 400, "Http请求头中没有 content-length")
            return

        if 'content-type' in hdr:
            content_type = hdr['content-type']
        else:
            content_type = ''
        length = int(hdr['content-length'])
        req_params = {}
        if length > 0:
            try:
                partial_body = req.partial_body
                body_left_size = length - len(partial_body)
                if body_left_size > 0:
                    err_code, left_data = csu_http.recv_len_data(req.conn, body_left_size)
                    if err_code != 0:
                        return
                    data = partial_body + left_data
                else:
                    data = partial_body

                if content_type.startswith('multipart/form-data;'):
                    req.form = data
                else:
                    req.form = json.loads(data)
                    req_params = req.form
            except Exception as e:
                csu_http.reply_http(req.conn, 400, "读取请求时发生了未知的错误：%s" % str(e))
                return
        else:
            req.form = {}

        func_name = url_path[len(prefix):]

        if func_name not in http_handler:
            csu_http.reply_http(req.conn, 404, "无此API: %s" % func_name)
            return
        if func_name not in ['get_session', 'login', 'logout']:
            try:
                c = SimpleCookie()
                c.load(hdr['cookie'])
                session_id = c['session_id'].value
                if not session_id:
                    csu_http.reply_http(req.conn, 401, "no login")
                    # reply_nologin_error(req.conn)
                    return
            except Exception as e:
                csu_http.reply_http(req.conn, 401, "no login: %s" % repr(e))
                # reply_nologin_error(req.conn)
                return

            check_res, msg = session_handler.check_session(session_id, func_name)
            if check_res < 0:
                csu_http.reply_http(req.conn, 401, "no login: %s" % msg)
                return
            elif check_res > 0:
                csu_http.reply_http(req.conn, 403, "no right: %s" % msg)
                return
            req.session_id = session_id
            req.session_data = msg

            user_name = req.session_data['user_name']
            logger.debug(f"Recv http request [{user_name}] {func_name}({repr(req_params)})")
            func_obj = http_handler[func_name]
            http_code, body_data = func_obj(req)
            logger.debug(f"Reply request [{user_name}] {func_name}({repr(req_params)}): {http_code}, {body_data}")
            csu_http.reply_http(req.conn, http_code, body_data)
        else:
            logger.debug(f"Recv http request {func_name}({repr(req_params)})")
            func_obj = http_handler[func_name]
            http_code, body_data = func_obj(req)
            logger.debug(f"Reply request {func_name}({repr(req_params)}): {http_code}, {body_data}")
            csu_http.reply_http(req.conn, http_code, body_data)
        return
    except BrokenPipeError:
        logger.debug(f"Recv http request BrokenPipeError: {traceback.format_exc()}.")
        return
    except Exception as e:
        logger.warning(f"Recv http request unknown error: {traceback.format_exc()}.")
        csu_http.reply_http(req.conn, 500, "Unexpected error: %s" % repr(e))
        return
    finally:
        req.session_id = None
        req.session_data = None


class HttpReq:
    def __init__(self, web_root, conn, origin_source, http_method, hdr, url_path, partial_body, exit_func,
                 http_handler, session_handler):
        self.web_root = web_root
        self.conn = conn
        self.origin_source = origin_source
        self.method = http_method
        self.headers = hdr
        self.path = url_path
        self.partial_body = partial_body
        self.form = None
        self.session_id = ''
        self.exit_flag = False
        self.g_exit_func = exit_func
        self.http_handler = http_handler
        self.session_handler = session_handler


class ProcessThread(threading.Thread):
    def __init__(self, client_socket, client_addr, web_root, http_handler, session_handler, g_exit_func):
        threading.Thread.__init__(self, name="csu-web-server-process")
        self.socket = client_socket
        self.addr = client_addr
        self.web_root = web_root
        self.http_handler = http_handler
        self.session_handler = session_handler
        self.g_exit_func = g_exit_func

    def run(self):
        origin_source = "%s:%s" % (self.addr[0], self.addr[1])
        pre_msg = "http(%s)" % origin_source
        logger.debug(f"Recv http {pre_msg} request.")
        csu_http.set_keepalive_linux(self.socket)
        while not self.g_exit_func():
            err_code, http_dict = csu_http.recv_headers(self.socket)
            if err_code != 0:
                logger.debug(f"Invalid http request(close connection): {http_dict}.")
                self.socket.close()
                break

            http_method = http_dict['method']
            hdr = http_dict['hdr']

            http_req = HttpReq(self.web_root,
                               self.socket,
                               origin_source,
                               http_method,
                               hdr,
                               http_dict['path'],
                               http_dict['partial_body'],
                               self.g_exit_func,
                               self.http_handler,
                               self.session_handler
                               )

            if 'upgrade' in hdr and 'sec-websocket-version' in hdr:
                csu_http.reply_http(self.socket, 400, "not support websocket: %s" % http_method)
            elif http_method == 'HEAD':
                http_do_head(http_req)
            elif http_method == 'GET':
                http_do_get(http_req)
            elif http_method == 'POST':
                http_do_post(http_req)
            else:
                csu_http.reply_http(self.socket, 400, "invalid method: %s" % http_method)

        logger.debug(f"{pre_msg}: closed.")


class WebServerMainThread(threading.Thread):
    def __init__(self, web_root, listen_addr, http_handler, session_handler, g_exit_func):
        global default_handler_func_dict
        threading.Thread.__init__(self, name="csu_main_web_server_thread")
        self.web_root = web_root
        self.listen_addr = listen_addr
        self.http_handler = http_handler
        self.session_handler = session_handler
        self.exit_func = g_exit_func
        self.http_handler = {}
        self.http_handler.update(default_handler_func_dict)
        self.http_handler.update(http_handler)

    def run(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        csu_http.set_keepalive_linux(server_socket)
        server_socket.bind(self.listen_addr)
        server_socket.listen(5)
        logging.info(f"csu web server listen at {repr(self.listen_addr)} ...")
        while not self.exit_func():
            try:
                client_socket, client_addr = server_socket.accept()
            except socket.timeout:
                continue
            thd = ProcessThread(client_socket,
                                client_addr,
                                self.web_root,
                                self.http_handler,
                                self.session_handler,
                                self.exit_func)
            thd.start()


def start(web_root, listen_addr, http_handler, session_handler, exit_func):
    """

    :param web_root:  web服务的根目录，此目录下的是静态html文件
    :param listen_addr: 监控端口
    :param http_handler: 处理post请求的处理函数字典
    :param session_handler: session的处理对象，至少要实现函数check_session和session_is_valid
    :param exit_func:
    :return:
    """
    logging.info("Starting web server...")
    web_thread = WebServerMainThread(web_root, listen_addr, http_handler, session_handler, exit_func)
    web_thread.start()
