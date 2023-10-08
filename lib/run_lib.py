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
@description: OS命令运行模块
"""

import os
import sys
import select
import logging
import subprocess



def run_cmd(cmd):
    logging.debug(f"Run: {cmd}")
    cp = subprocess.run(cmd, shell=True)
    return cp.returncode


def daemon_execv(path, args):

    pid = os.fork()
    if pid > 0:  # 父进程返回
        return
    pid = os.getpid()

    sys.stdout.flush()
    sys.stderr.flush()
    si = open('/dev/null', 'r')
    so = open('/dev/null', 'a+')
    se = open('/dev/null', 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # 把打开的文件句柄关闭，防止影响子进程
    fds = os.listdir(f'/proc/{pid}/fd')
    for fd in fds:
        int_fd = int(fd)
        if int_fd > 2:
            try:
                os.close(int_fd)
            except Exception:
                pass
    os.execv(path, args)


def __exec_cmd(cmd):
    p = subprocess.Popen(cmd, shell=True, close_fds=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_msg = b''
    err_msg = b''
    p_stdout_fd = p.stdout.fileno()
    p_stderr_fd = p.stderr.fileno()
    os.set_blocking(p.stdout.fileno(), False)
    os.set_blocking(p.stderr.fileno(), False)
    rlist = [p_stdout_fd, p_stderr_fd]
    elist = [p_stdout_fd, p_stderr_fd]
    out_data = b''
    err_data = b''
    need_exists = False
    while True:
        rs, _ws, es = select.select(rlist, [], elist)
        for r in rs:
            if r == p_stdout_fd:
                out_data = p.stdout.read()
                out_msg += out_data
            if r == p_stderr_fd:
                err_data = p.stderr.read()
                if err_data == b'':
                    need_exists = True
                err_msg += err_data
        for r in es:
            rlist.remove(r)
        if len(rlist) == 0:
            break
        if (not out_data) and (not err_data):
            break
        if need_exists:
            break

    ret_out_msg = out_msg.decode('utf-8')
    ret_err_msg = err_msg.decode('utf-8')
    err_no = p.wait()
    return err_no, ret_err_msg, ret_out_msg


def open_cmd(cmd):
    try:
        err_no, err_msg, out_msg = __exec_cmd(cmd)
        logging.debug(f"Run: {cmd}")
    except Exception as e:
        raise e

    if err_no:
        raise OSError(err_no, f"Run cmd {cmd} failed: \n {err_msg}")
    return out_msg


def test_cmd(cmd):
    try:
        p = subprocess.Popen(cmd, shell=True, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _out_msg = p.stdout.read()
        _err_msg = p.stderr.read()
        err_code = p.wait()
        # logger.debug(f"Run: {cmd}, return {out_msg}")
        logging.debug(f"Run: {cmd}")
        return err_code
    except Exception:
        return -1


def run_cmd_result(cmd):
    out_msg = ''
    try:
        err_no, err_msg, out_msg = __exec_cmd(cmd)
        logging.debug(f"Run: {cmd}")
    except Exception as e:
        err_no = -1
        err_msg = str(e)
    return err_no, err_msg, out_msg
