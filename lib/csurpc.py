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
@description: rpc调用的模块
"""

import os
import sys
import queue
import errno
import fcntl
import pickle
import select
import socket
import struct
import logging
import threading
import traceback

import cs_low_trans

logger = logging.getLogger('csurpc')


# 列出服务端的各个服务名
CMD_FUNC_LIST = 100

# 调用服务端的某个服务
CMD_CALL_FUNC = 200

#
DEBUG_LOG_MAX_LEN = 8192


# 线程池中的线程
class _WorkThread(threading.Thread):
    """
    线程池中的工作线程
    """
    def __init__(self, thread_pool, thread_name):
        """
        :param thread_pool: 线程池对象
        """
        threading.Thread.__init__(self, name=thread_name)
        self.thread_pool = thread_pool
        # is_exit: 是一个函数，在每次循环时，调用此函数，如果此函数返回为趁，则此线程结束
        self.is_exit = thread_pool.is_exit
        self.setDaemon(True)
        self.work_queue = thread_pool.work_queue
        self.start()

    def run(self):
        while not self.is_exit():
            try:
                work_func, args, kwargs = self.work_queue.get(timeout=0.1)
            except queue.Empty:  # 任务队列为空，继续循环
                continue
            try:
                # 执行任务
                self.thread_pool.inc_busy()
                work_func(*args, **kwargs)
                self.thread_pool.dec_busy()
            except Exception:
                self.thread_pool.dec_busy()
                logger.error(f"RPC ERROR: {traceback.format_exc()}.")


class _ThreadPool:
    """
    线程池对象
    """
    def __init__(self, name, is_exit_func, pool_size=10):
        """
        :param pool_size: 线程池启动的线程数
        """
        self.name = name
        self.mutex = threading.Lock()
        self.is_exit = is_exit_func
        self.busy_count = 0   # 繁忙的线程数
        self.work_queue = queue.Queue()
        self.threads = []
        self.__create_thread(pool_size)

    def inc_busy(self):
        self.mutex.acquire()
        self.busy_count += 1
        self.mutex.release()

    def dec_busy(self):
        self.mutex.acquire()
        self.busy_count -= 1
        self.mutex.release()

    def get_busy_threads_count(self):
        self.mutex.acquire()
        busy_count = self.busy_count
        self.mutex.release()
        return busy_count

    def __create_thread(self, num_of_threads):
        for i in range(num_of_threads):
            thread = _WorkThread(self, "%s-%d" % (self.name, i))
            self.threads.append(thread)

    def wait_for_complete(self):
        """
        等待所有线程完成。
        """
        for thread in self.threads:
            # 等待线程结束
            if thread.isAlive():  # 判断线程是否还存活来决定是否调用join
                thread.join()

    def add_job(self, work_func, *args, **kwargs):
        """
        把工作任务加到任务队列中
        """
        self.work_queue.put((work_func, args, kwargs))


def _get_member_func(obj):

    func_list = []
    attr_name_list = dir(obj)
    for attr_name in attr_name_list:
        if attr_name[0] == '_':
            continue
        attr = getattr(obj, attr_name)
        if callable(attr):
            func_list.append(attr_name)
    return func_list


def _handler_connect(sock, srv_obj):
    """
    :param sock:    新连接的socket句柄
    :param srv_obj: 服务类的一个实例
    :return: 无返回值
    """
    global DEBUG_LOG_MAX_LEN

    try:
        err, _msg = cs_low_trans.auth_connect(sock, srv_obj.password, srv_obj.timeout)
        if err:
            return  # 验证失败或socket错误，直接返回
    except Exception:
        traceback.print_exc()
        return

    while True:
        try:
            err, _msg, cmd, data = cs_low_trans.recv_cmd(sock, srv_obj.timeout)
            if err:
                break
            if cmd == CMD_FUNC_LIST:  # 客户端请求handler中有哪些函数可以调用
                ret_data = pickle.dumps(srv_obj.srv_func_list)
                err, _msg = cs_low_trans.reply_cmd(sock, 0, ret_data, srv_obj.timeout)
                if err:
                    break
            elif cmd == CMD_CALL_FUNC:
                try:
                    func_name, func_args, func_kwargs = pickle.loads(data)
                    if logger.level <= logging.DEBUG:
                        str_args = repr(func_args)
                        if len(str_args) > DEBUG_LOG_MAX_LEN:
                            str_args = str_args[:DEBUG_LOG_MAX_LEN] + " ... "
                        str_kwargs = repr(func_kwargs)
                        if len(str_kwargs) > DEBUG_LOG_MAX_LEN:
                            str_kwargs = str_kwargs[:DEBUG_LOG_MAX_LEN] + " ... "
                        rpc_info = f"RECV RPC: {func_name} :\nargs={str_args}"
                        if func_kwargs:
                            rpc_info += f"\nkwargs={str_kwargs}"
                        logger.debug(rpc_info)
                except Exception as e:
                    err, _msg = cs_low_trans.reply_cmd(sock, 1,
                                                      f"decode func args failed: {str(e)}".encode('utf-8'),
                                                      srv_obj.timeout)
                    if err:
                        break
                    continue

                if func_name not in srv_obj.srv_func_list:
                    err, _msg = cs_low_trans.reply_cmd(sock, 1,
                                                      ("Function(%s) does not exist" % func_name).encode('utf-8'),
                                                      srv_obj.timeout)
                    if err:
                        break
                    continue

                call_func = getattr(srv_obj.handler, func_name)
                try:
                    ret = call_func(*func_args, **func_kwargs)
                    ret_data = pickle.dumps(ret)
                    ret_code = 0
                    if logger.level <= logging.DEBUG:
                        str_ret = repr(ret)
                        if len(str_ret) > DEBUG_LOG_MAX_LEN:
                            str_ret = str_ret[:DEBUG_LOG_MAX_LEN]
                        rpc_info = f"RETURN RECV RPC: {func_name} :\nreturn={str_ret}"
                        logger.debug(rpc_info)
                except Exception as e:
                    if logger.level <= logging.DEBUG:
                        logger.debug(f"call {func_name} failed:\n{traceback.format_exc()}")

                    exc_type, _, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    err_msg = '%s: %s in %s:%d' % (exc_type.__name__, str(e), fname, exc_tb.tb_lineno)
                    ret_code = 1
                    ret_data = err_msg.encode()
                err, _msg = cs_low_trans.reply_cmd(sock, ret_code, ret_data, srv_obj.timeout)
                if err:
                    break
        except Exception:
            traceback.print_exc()
            break

    try:
        sock.close()
    except Exception:
        pass


def parse_connect_url(conn_url):
    """
    解析连接字符串
    :param conn_url:
    :return: (protocol, ip, port)
    """

    cells = conn_url.split('://')
    if len(cells) != 2:
        raise Exception(f"Invalid connect url: {conn_url}.")
    protocol = cells[0].lower()
    if protocol != 'tcp':  # 注意目前只支持tcp协议
        raise Exception("Unsupported protocol: %s" % cells[0])
    ipport = cells[1]
    cells = ipport.split(':')
    if len(cells) != 2:
        raise Exception("Invalid connect url: %s" % conn_url)

    try:
        ip = socket.gethostbyname(cells[0])
    except socket.gaierror:
        raise Exception(f"Invalid server or ip addr in url: {conn_url}.")

    if not cells[1].isdigit():
        raise Exception("Invalid port in connect url:%s" % cells[1])
    port = int(cells[1])

    return protocol, ip, port


class Server:
    """
        使用方法:
        s = Server(MyHandle):
        s.bind("tcp://0.0.0.0:4342")
        s.run()
    """

    def __init__(self, name, handler, is_exit_func, password='cstechRpc', thread_count=30, timeout=300, debug=0):
        self.name = name
        self.handler = handler
        self.thread_count = thread_count
        self.timeout = timeout
        self.ss = None
        self.is_exit = is_exit_func
        self.thread_pool = None
        self.srv_func_list = _get_member_func(handler)
        self.password = password
        self.debug = debug

    def get_busy_threads_count(self):
        """
        获得正在执行任务的线程数
        :return: int, 返回正在执行任务的线程数
        """

        if not self.thread_pool:
            return 0
        return self.thread_pool.get_busy_threads_count()

    def bind(self, conn_url):
        """
        :param conn_url: 连接url,格式为: 'protocol://ip:port',目前protocol只支持tcp。
        :return: 无返回值
        使用例子: s.bind('tcp://0.0.0.0:4342')
        """

        protocol, ip, port = parse_connect_url(conn_url)
        if protocol != 'tcp':
            raise Exception("Unsupported protocol:%s" % protocol)

        self.ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        fcntl.fcntl(self.ss.fileno(), fcntl.F_SETFD, fcntl.FD_CLOEXEC)

        self.ss.settimeout(self.timeout)
        self.ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ss.bind((ip, port))

    def run(self):
        """
        运行服务
        :return: 无返回值
        """

        self.thread_pool = _ThreadPool(self.name, self.is_exit, self.thread_count)
        self.ss.listen(10)
        while not self.is_exit():

            # if self.debug:
            #    print "Busy threads count: %s" % self.get_busy_threads_count()

            try:
                readable, _writeable, _exceptional = select.select([self.ss], [], [], 1)
                if not readable:
                    # logger.debug("select timeout.")
                    continue
            except select.error as e:
                if e.args[0] == errno.EINTR:  # 这是收到信号打断了select函数
                    continue
                raise Exception("call select() failed: %s" % repr(e))
            (client, _address) = self.ss.accept()
            # if self.debug:
            #    print("Accept connection from %s" % str(address))
            linger = struct.pack('ii', 1, 1)
            r = client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
            if r:
                raise Exception("Set socket SO_LINGER failed!")
            client.settimeout(self.timeout)
            self.thread_pool.add_job(_handler_connect, client, self)
        self.ss.close()


class CsuTimeoutError(Exception):
    """
    定义一个超时错误，当在异步调用时，获得结果超时会抛出此错误
    """
    def __init__(self, msg):
        Exception.__init__(self, msg)


class RunError(Exception):
    """
    定义一个超时错误，当在异步调用时，获得结果超时会抛出此错误
    """
    def __init__(self, msg):
        Exception.__init__(self, msg)


class AsyncError(Exception):
    """
    当在同一个socket连接中正在执行一个异步请求还没有结束，又发起了一个请求时，就会抛出此错误
    """
    def __init__(self, msg):
        Exception.__init__(self, msg)


class _Trans:
    """
    传输对象，把socket对象封装进来
    """

    def __init__(self, sock, call_timeout, ip, port, msg_callback=None):
        self.sock = sock
        self.call_timeout = call_timeout
        self.ip = ip
        self.port = port
        self.pending = False  # pending==True时，表示在异步模式下正在处理异步任务，这时不能再发起异步请求
        self.msg_callback = msg_callback


# 客户端异步调用时使用的线程
class _AsyncCallTask(threading.Thread):
    """
    异步调用的任务类
    """
    def __init__(self, trans, func_name, *args, **kwargs):
        """
        :param trans: 传输对象
        :param func_name: 要调用的远程函数名称
        """
        threading.Thread.__init__(self)
        self.mutex = threading.Lock()
        self.result_queue = queue.Queue(maxsize=1)
        self.trans = trans
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs
        self.setDaemon(True)

        if trans.pending:
            raise AsyncError("Previous async task is running, please get result of previous async task, then retry!")
        trans.pending = True
        self.start()

    def run(self):
        data = pickle.dumps([self.func_name, self.args, self.kwargs])
        err, msg, ret_code, ret_data = cs_low_trans.send_cmd(self.trans.sock, CMD_CALL_FUNC, data, self.trans.call_timeout)
        if err:
            self.result_queue.put((err, msg))
            return
        if ret_code:
            self.result_queue.put((ret_code, ret_data))
            return
        func_ret = pickle.loads(ret_data)
        self.result_queue.put((0, func_ret))

    def get(self, timeout=None):
        """
        获得调用的结果
        :param timeout: 如果不设置，则一直等到用户端返回
        :return: result, result是远程函数的返回值
        """

        if not self.trans.pending:
            raise AsyncError("Async task not running!")

        try:
            err, result = self.result_queue.get(timeout=timeout)
            self.trans.pending = False
            if err:
                raise RunError(result)
            return result
        except queue.Empty:
            raise CsuTimeoutError("Timeout: unable to obtain results within %s seconds" % timeout)


def call_remote_func(trans, func_name, *args, **kwargs):
    """
    调用远程的函数
    :param trans:
    :param func_name: 远程的函数名
    :param args: 传给远程函数的列表形式的参数
    :param kwargs: 传给远程函数的字典形式的参数
    :return: 如果是同步模式，则返回远程函数的返回值，如果是异步模式返回异步对象async_task，通过async_task.get()可以获得执行结果
    """
    global DEBUG_LOG_MAX_LEN

    call_timeout = trans.call_timeout
    sock = trans.sock
    if logger.level <= logging.DEBUG:
        str_args = repr(args)
        if len(str_args) > DEBUG_LOG_MAX_LEN:
            str_args = str_args[:DEBUG_LOG_MAX_LEN] + " ... "
        str_kwargs = repr(kwargs)
        if len(str_kwargs) > DEBUG_LOG_MAX_LEN:
            str_kwargs = str_kwargs[:DEBUG_LOG_MAX_LEN] + " ... "

        rpc_info = f"CALL RPC({trans.ip}:{trans.port}): {func_name} :\n    args={str_args}"
        if kwargs:
            rpc_info += f"\n    kwargs={str_kwargs}"
        logger.debug(rpc_info)
        if trans.msg_callback:
            trans.msg_callback(rpc_info)

    async_mode = False
    if 'async_mode' in kwargs:
        if kwargs['async_mode']:
            async_mode = True
        del kwargs['async_mode']

    if async_mode:  # 异步模式
        async_task = _AsyncCallTask(trans, func_name, *args, **kwargs)
        return async_task
    else:  # 同步模式
        try:
            data = pickle.dumps([func_name, args, kwargs])
        except Exception as e:
            raise UserWarning(f"unsupport args type: {repr(e)}")
        err, msg, ret_code, ret_data = cs_low_trans.send_cmd(sock, CMD_CALL_FUNC, data, call_timeout)
        if err:
            raise UserWarning(f"socket error: {msg}")
        if ret_code:
            raise UserWarning(ret_data.decode())
        func_ret = pickle.loads(ret_data)
        if logger.level <= logging.DEBUG:
            str_ret = repr(func_ret)
            if len(str_ret) > DEBUG_LOG_MAX_LEN:
                str_ret = str_ret[:DEBUG_LOG_MAX_LEN]
            rpc_info = f"RETURN CALL RPC({trans.ip}:{trans.port}) : {func_name} :\n    return={str_ret}"
            logger.debug(rpc_info)
            if trans.msg_callback:
                trans.msg_callback(rpc_info)

        return func_ret


class Client:
    """
    使用方法:
    c = Client():
    c.connect("tcp://127.0.0.1:4342")
    c.my_func()
    异步模式:
    rs = c.hello('hello', 'world', async_mode=True)
    ret = rs.get(10)
    注意即使在异步模式下，对同一个连接也只能同时发一个请求，不能同时发多个请求，如果需要同时发多个请求，请建多个连接
    """

    def __init__(self, call_timeout=300, msg_callback=None):
        self.trans = _Trans(sock=None, call_timeout=call_timeout, ip=None, port=None, msg_callback=msg_callback)
        self.mutex = threading.Lock()
        self.conn_timeout = 300
        self.msg_callback = msg_callback

    def connect(self, conn_url, password='cstechRpc', conn_timeout=10, data_timeout=300):
        """
        s.bind('tcp://127.0.0.1:4342')
        :param conn_url:
        :param password:
        :param timeout: 注意是连接超时，不是调用超时
        :return:
        """
        self.conn_timeout = conn_timeout
        self.data_timeout = data_timeout

        protocol, ip, port = parse_connect_url(conn_url)
        if protocol != 'tcp':
            raise Exception("Unsupported protocol:%s" % protocol)
        self.trans.ip = ip
        self.trans.port = port
        err, msg, self.trans.sock = cs_low_trans.connect(ip, port, password, conn_timeout, data_timeout)
        if err:
            raise Exception(msg)

        err, msg, ret_code, ret_data = cs_low_trans.send_cmd(self.trans.sock, CMD_FUNC_LIST, b'', data_timeout)
        if err:
            raise Exception("socket error: %s" % msg)
        if ret_code:
            raise Exception("rpc error: %s" % ret_data)

        # 把远程服务中存在的函数名加到本地的类上，这样调用本地类上的函数，相当于调用了远程的函数
        self.func_list = pickle.loads(ret_data)

    def close(self):
        if self.trans.sock is not None:
            self.trans.sock.close()
            self.trans.sock = None

    def __del__(self):
        if self.trans.sock is not None:
            self.trans.sock.close()
            self.sock = None

    def __nonzero__(self):
        return True

    def __call__(self, method, *args, **kwargs):
        return call_remote_func(self.trans, method, *args, **kwargs)

    def __getattr__(self, method):
        if method not in self.func_list:
            raise Exception("function not support %s" % method)
        return lambda *args, **kargs: self(method, *args, **kargs)

    def __str__(self):
        return f"csurpc({self.trans.ip}:{self.trans.port})"



#############################################################################
# 下面为测试代码                                                              #
#############################################################################

import time


class _TestHandle:
    def __init__(self):
        pass

    def func1(self, *args):
        time.sleep(0.01)
        return '.'.join(args)

    def func2(self, *args):
        time.sleep(0.01)
        return '#'.join(args)


def main():
    """
    此函数用做测试
    :return: 无
    """

    exit_flag = 0

    def is_exit():
        return exit_flag

    def _usage():
        print("In Server: %s -s " % sys.argv[0])
        print("In Client: %s -c <anystr1> [anystr2] [anystr3] ..." % sys.argv[0])
        print("Usage: %s -s" % (sys.argv[0]))
    if len(sys.argv) < 2:
        _usage()
        return

    if sys.argv[1] == '-h' or sys.argv[1] == '--help':
        _usage()
        return
    if sys.argv[1] not in ['-s', '-c']:
        _usage()
        return

    if sys.argv[1] == '-s':
        handle = _TestHandle()
        srv = Server('ha-service', handle, is_exit, password='cstechRpc', thread_count=10, debug=1)
        srv.bind("tcp://0.0.0.0:4342")
        srv.run()
    elif sys.argv[1] == '-c':
        for _i in range(30):
            proxy = Client()
            proxy.connect("tcp://127.0.0.1:4342")
            # 测试同步调用
            ret1 = proxy.func1(*sys.argv[2:])
            print("Sync call func1(), return: %s " % ret1)

        ret2 = proxy.func2(*sys.argv[2:])
        print("Sync call func1(), return: %s " % ret2)

        # 测试异步调用
        c2 = Client()
        c2.connect("tcp://127.0.0.1:4342")
        print("ASync call func1() ... ")
        rs1 = proxy.func1(*sys.argv[2:], async_mode=True)
        print("ASync call func2() ... ")
        rs2 = c2.func2(*sys.argv[2:], async_mode=True)
        ret1 = rs1.get(10)
        print("ASync call func1() return : %s" % ret1)
        ret2 = rs2.get(10)
        print("ASync call func2() return : %s" % ret2)

if __name__ == '__main__':
    main()
