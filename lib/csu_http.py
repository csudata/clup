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


import mimetypes
import posixpath
import socket
import struct
import time

import email.utils

if not mimetypes.inited:
    mimetypes.init()  # try to read system mime.types
__extensions_map = mimetypes.types_map.copy()
__extensions_map.update({
    '': 'application/octet-stream',  # Default
    '.py': 'text/plain',
    '.c': 'text/plain',
    '.h': 'text/plain',
    })

__http_code_dict = {
100: 'Continue',
101: 'Switching Protocols',
102: 'Processing',
200: 'OK',
201: 'Created',
202: 'Accepted',
203: 'Non-Authoritative Information',
204: 'No Content',
205: 'Reset Content',
206: 'Partial Content',
207: 'Multi-Status',
208: 'Already Reported',
226: 'IM Used',
300: 'Multiple Choices',
301: 'Moved Permanently',
302: 'Found',
303: 'See Other',
304: 'Not Modified',
305: 'Use Proxy',
307: 'Temporary Redirect',
308: 'Permanent Redirect',
400: 'Bad Request',
401: 'Unauthorized',
402: 'Payment Required',
403: 'Forbidden',
404: 'Not Found',
405: 'Method Not Allowed',
406: 'Not Acceptable',
407: 'Proxy Authentication Required',
408: 'Request Timeout',
409: 'Conflict',
410: 'Gone',
411: 'Length Required',
412: 'Precondition Failed',
413: 'Request Entity Too Large',
414: 'Request-URI Too Long',
415: 'Unsupported Media Type',
416: 'Requested Range Not Satisfiable',
417: 'Expectation Failed',
421: 'Misdirected Request',
422: 'Unprocessable Entity',
423: 'Locked',
424: 'Failed Dependency',
426: 'Upgrade Required',
428: 'Precondition Required',
429: 'Too Many Requests',
431: 'Request Header Fields Too Large',
500: 'Internal Server Error',
501: 'Not Implemented',
502: 'Bad Gateway',
503: 'Service Unavailable',
504: 'Gateway Timeout',
505: 'HTTP Version Not Supported',
506: 'Variant Also Negotiates',
507: 'Insufficient Storage',
508: 'Loop Detected',
510: 'Not Extended',
511: 'Network Authentication Required'
}


def code_to_phrase(code: int) -> str:
    global __http_code_dict
    if code not in __http_code_dict:
        return ''
    return __http_code_dict[code]


def guess_type(path):
    """Guess the type of a file.
    Argument is a PATH (a filename).
    Return value is a string of the form type/subtype,
    usable for a MIME Content-type header.
    The default implementation looks the file's extension
    up in the table self.extensions_map, using application/octet-stream
    as a default; however it would be permissible (if
    slow) to look inside the data to make a better guess.
    """

    _base, ext = posixpath.splitext(path)
    if ext in __extensions_map:
        return __extensions_map[ext]
    ext = ext.lower()
    if ext in __extensions_map:
        return __extensions_map[ext]
    else:
        return __extensions_map['']


def date_time_string(timestamp=None):
    """Return the current date and time formatted for a message header."""
    if timestamp is None:
        timestamp = time.time()
    return email.utils.formatdate(timestamp, usegmt=True)


def set_keepalive_linux(sock, after_idle_sec=1, interval_sec=2, max_fails=3):
    """
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


def recv_len_data(conn, data_len):
    ret_data = b''
    received_len = 0
    while received_len < data_len:
        recv_data = conn.recv(data_len - received_len)
        if len(recv_data) == 0:
            return -1, "connection is closed"
        ret_data += recv_data
        received_len += len(recv_data)
    return 0, ret_data


MANDATORY = 1
INT = 2


def parse_parms(params, req):
    """
    :param params:
    :param req:
    :return: 成功处理返回一个字典, 否则返回None
    """
    # params = {'page_num': MANDATORY|INT, 'page_size': MANDATORY|INT, 'pm_hostname':0, 'ip':0}
    mandatory_params = []
    int_params = []

    for param in params:
        if params[param] & MANDATORY == MANDATORY:
            mandatory_params.append(param)

        if params[param] & INT == INT:
            int_params.append(param)

    # 检查参数的合法性
    pdict = {}
    for param in params:
        if param in mandatory_params and param not in req.form:
            # return_param_lost(req, param)
            return -1, "parameter lost: %s" % param
        if param not in req.form:
            continue
        if param in req.form:
            value = req.form[param]
        else:
            value = ''
        if param in int_params:
            if isinstance(value, str):
                if not value.isdigit():
                    return -1, "parameter must be integer: %s = %s" % (param, value)
                value = int(value)
            elif isinstance(value, int):
                pass
            else:
                return -1, "parameter must be integer: %s = %s" % (param, value)
        pdict[param] = value
    return 0, pdict


def reply_http(conn, http_code, body_data, hdr=None):
    if hdr is None:
        hdr = {}
    reason_phrase = code_to_phrase(http_code)

    if isinstance(body_data, str):
        body_data = body_data.encode()

    hdr_msg = "HTTP/1.1 %s %s\r\nContent-Length: %d" % (http_code, reason_phrase, len(body_data))
    if 'content-type' not in hdr:
        hdr_msg += "\r\nContent-Type: text/html; charset=UTF-8"
    for key in hdr:
        hdr_msg += "\r\n%s: %s" % (key, hdr[key])
    http_msg = hdr_msg.encode() + b"\r\n\r\n" + body_data
    conn.sendall(http_msg)


def recv_headers(conn):
    """
    接受http header的数据
    :param conn:
    :return:
    """
    max_hdr_len = 8192
    recv_data = conn.recv(max_hdr_len)
    recv_len = len(recv_data)
    if recv_len == 0:
        return -1, "socket maybe closed"
    req_data = recv_data
    while b'\r\n\r\n' not in req_data:
        if recv_len >= max_hdr_len:
            return -1, "header data can not more than %d." % max_hdr_len
        recv_data = conn.recv(max_hdr_len - recv_len)
        if not recv_data:
            return -1, "socket maybe closed"
        req_data += recv_data
        recv_len += len(recv_data)

    pos = req_data.find(b'\r\n\r\n')
    if pos == -1:
        return -1, "invalid http request: %s" % str(req_data)
    hdr_data = req_data[:pos]
    partial_body = req_data[pos+4:]

    hdr_items = hdr_data.split(b'\r\n')
    cells = hdr_items[0].split()
    if len(cells) != 3:
        return -1, "invalid http request"
    http_method = cells[0].decode()
    url_path = cells[1].decode()
    hdr_items = hdr_items[1:]
    hdr_dict = {}
    for item in hdr_items:
        pos = item.find(b':')
        if pos >= 0:
            key = item[:pos].strip()
            val = item[pos + 1:].strip()
            hdr_dict[key.decode().lower()] = val.decode()
        else:
            hdr_dict[item.strip()] = ''
    http_dict = {"method": http_method, "path": url_path, "hdr": hdr_dict, "partial_body": partial_body}
    return 0, http_dict


def ws_recv_frame_hdr(conn):
    """
    :param conn:
    :return: err_code, {"op_code": op_code, "mask": mask_flag, "mask_key": mask_key, "hdr_data": hdr_data,
                        "data_len": data_len}
    如果err_code为-1表示错误，而data中存的是错误信息。如果err_code为0，则表示为成功，其中remain是当数据包比较大的时候，还没有读完的数据。
    """

    err_code, hdr_data = recv_len_data(conn, 2)
    if err_code != 0:
        return err_code, hdr_data
    op_code = hdr_data[0] & 15
    mask_flag = hdr_data[1] & 128
    if mask_flag:
        mask_key_len = 4
    else:
        mask_key_len = 0
    payload_len = hdr_data[1] & 127
    if payload_len == 126:
        err_code, next_bytes = recv_len_data(conn, 2 + mask_key_len)
        if err_code != 0:
            return err_code, next_bytes
        data_len = next_bytes[0] * 256 + next_bytes[1]
        mask_key = next_bytes[2:]
    elif payload_len == 127:
        err_code, next_bytes = recv_len_data(conn, 8 + mask_key_len)
        if err_code != 0:
            return err_code, next_bytes
        data_len, = struct.unpack('>Q', next_bytes[:8])
        mask_key = next_bytes[8:]
    else:
        data_len = payload_len
        next_bytes = b''
        if mask_flag:
            err_code, next_bytes = recv_len_data(conn, mask_key_len)
            if err_code != 0:
                return err_code, next_bytes
            mask_key = next_bytes
        else:
            mask_key = b''

    hdr_data += next_bytes
    hdr_dict = {"op_code": op_code, "mask": mask_flag, "mask_key": mask_key, "hdr_data": hdr_data, "data_len": data_len}
    return 0, hdr_dict


def gen_http_handler(obj):
    obj_attr_list = dir(obj)
    handler_func_dict = {}
    for attr_name in obj_attr_list:
        if attr_name.startswith("__"):
            continue
        attr_val = getattr(obj, attr_name)
        if not callable(attr_val):
            continue
        handler_func_dict[attr_name] = attr_val
    return handler_func_dict
