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
@description: 为应用提供daemon运行的模块
"""

import os
import sys
import time
import inspect
import logging.handlers
import traceback
import logging
import signal
import threading

# 应用程序名
__app_name = ''
# 退出标志
_exit_flag = False
# 退出时的清理
_cleanup_handles = []


def prepare_run(app_name: str, foreground=False):
    """
    运行前准备，如果传入的foreground不是真，即会进入后台执行，同时会生成pidfile
    :param app_name: 会在/var/run目录下生成<app_name>.pid的文件，把当前进程的PID写入此文件
    :param foreground: 是否是前台运行
    :return: 无
    """
    global __app_name

    __app_name = app_name

    if os.path.isdir('/run'):
        __run_path = '/run'
    else:
        __run_path = '/var/run'

    pidfile = os.path.join(__run_path, "%s.pid" % app_name)
    if os.path.exists(pidfile):
        f = open(pidfile, "r")
        pidstr = f.readline()
        pidstr = pidstr.strip()
        if pidstr.isdigit():
            pid = int(pidstr)
            f.close()
            try:
                os.kill(pid, 0)
                print("ERROR : %s(pid=%d) already running !!!" % (app_name, pid))
                sys.exit(1)
            except Exception:
                pass
        else:
            print("WARNING : can not read pid from %s !!!" % pidfile)

    if foreground:
        # 当是后台运行时，daemon()函数会自动写pid文件，前台运行时需要自己生成pid文件
        with open(pidfile, "w") as fp:
            fp.write(str(os.getpid()))
        return

    try:
        sys.stdout.flush()
        sys.stderr.flush()
        si = open('/dev/null', 'r')
        so = open('/dev/null', 'a+')
        se = open('/dev/null', 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        pid = os.fork()
        if pid > 0:
            f = open(pidfile, "w")
            # 需要在父进程中写入pid文件，否则在systemctl中会报如下错误
            # PID file XXXX.pid not readable (yet?) after start
            f.write(str(pid))
            f.close()
            sys.exit(0)

        os.chdir('/')
        os.setsid()
        # os.umask(0)
    except OSError as e:
        print(repr(e))
        sys.exit(1)

    __set_signal()


def stop(app_name: str, retry_cnt=5, retry_sleep_seconds=4):
    """
    停止应用的运行
    :param app_name:
    :param retry_cnt:
    :param retry_sleep_seconds:
    :return:
    """

    if os.path.isdir('/run'):
        __run_path = '/run'
    else:
        __run_path = '/var/run'

    pidfile = os.path.join(__run_path, "%s.pid" % app_name)

    if not os.path.exists(pidfile):
        print("%s not running" % app_name)
        sys.exit(0)

    f = open(pidfile, "r")
    pidstr = f.readline()
    pidstr = pidstr.strip()
    if not pidstr.isdigit():
        print("ERROR : invalid content in %s !!!" % pidfile)
        sys.exit(1)

    pid = int(pidstr)
    f.close()
    try:
        os.kill(pid, 0)
    except Exception:
        print("%s already stopped !" % app_name)
        return

    i = 0
    while i < retry_cnt:
        try:
            os.kill(pid, signal.SIGTERM)
            print("Wait %d seconds for program stopped..." % retry_sleep_seconds)
            time.sleep(retry_sleep_seconds)
            i += 1
        except Exception:
            break

    if i >= retry_cnt:
        try:
            os.kill(pid, signal.SIGKILL)
            print("%s force stopped" % app_name)
        except Exception:
            print("%s stopped." % app_name)
    else:
        print("%s stopped." % app_name)


def status(app_name: str):
    """
    返回程序的运行状态
    :param app_name: 应用名称
    :return: 返回(err_code, status_msg)，当err_code为0表示程序未运行，err_code为1,表示程序正在运行，err_code为-1，出现错误
    """

    if os.path.isdir('/run'):
        __run_path = '/run'
    else:
        __run_path = '/var/run'

    pidfile = os.path.join(__run_path, "%s.pid" % app_name)

    if not os.path.exists(pidfile):
        return 0, "%s not running." % app_name

    f = open(pidfile, "r")
    pidstr = f.readline()
    pidstr = pidstr.strip()
    if not pidstr.isdigit():
        return -1, "ERROR : invalid content in %s !!!" % pidfile

    pid = int(pidstr)
    f.close()
    try:
        os.kill(pid, 0)
        return 1, "%s(pid=%d) is running." % (app_name, pid)
    except Exception:
        return 0, "%s not running,but %s exists!" % (app_name, pidfile)


def reg_service(service_name: str, after_service_list: list, service_script=None):
    """
    注册一个系统服务
    :param service_name: 服务名称
    :param after_service_list: 开机自启动时，在哪些服务启动之后之后再启动
    :param service_script: 服务的脚本，此脚本要求是可以加命令行参数start、stop、status的脚本，
                           如果不提供此参数，则默认脚本为/opt/<service_name>/bin/service_name
    :return: 返回err_code, err_msg, err_code为0表示成功无错误，err_code为1表示服务已存在，err_code为-1表示发生错误
    """
    if os.path.isdir('/run'):
        __run_path = '/run'
    else:
        __run_path = '/var/run'

    pid_file = os.path.join(__run_path, "%s.pid" % service_name)
    if not service_script:
        service_script = "/opt/{service_name}/bin/{service_name}".format(service_name=service_name)

    srv_path = '/etc/systemd/system'
    if not os.path.isdir(srv_path):
        err_msg = "this linux not support systemd service, can not register system service!"
        return -1, err_msg
    srv_file = os.path.join(srv_path, "%s.service" % service_name)
    if os.path.exists(srv_file):
        return 1, "already register systemd service!"

    str_service_list = ' '.join(after_service_list)

    srv_content = \
        """[Unit]
Description={service_name}
After={service_list}
[Service]
Type=forking
User=root

ExecStartPre=-{service_script} stop
ExecStart={service_script} start
PIDFile={pid_file}
ExecStop=-{service_script} stop

[Install]
WantedBy=multi-user.target
""".format(service_name=service_name, pid_file=pid_file, service_script=service_script, service_list=str_service_list)

    try:
        with open(srv_file, "w") as fp:
            fp.write(srv_content)
    except Exception as e:
        err_msg = "Write %s error: %s\n" % (srv_file, str(e))
        return -1, err_msg
    cmd = "systemctl enable %s" % service_name
    ret = os.system(cmd)
    if ret != 0:
        err_msg = "run %s failed" % cmd
        return -1, err_msg
    print("register service sucessfully.")
    print("""please use: "systemctl start {service_name}" to start {service_name}."""
          .format(service_name=service_name))
    return 0, ''


class _LogFuncObject:
    """
    生成一系列的函数，替换logging原先的一些debug、info、warn、error等函数
    """

    def __init__(self, old_func):
        """
        @param old_func: 原先的函数对象
        """
        self.old_func = old_func

    def __call__(self, *args):
        """
        调用本对象时，会对参数做一个处理，然后再调回原先的函数。
        @param args: 调用函数的多个参数
        @return:
        """

        new_args = []
        for arg in args:
            # 如果参数就字符串，则把其中的新行前加上四个空格
            if isinstance(arg, (str, )):
                new_args.append(arg.replace('\n', '\n    '))
            else:
                new_args.append(arg)

        # 如果调用的是debug方法，则在输出信息中加上文件名及行号
        if self.old_func.__name__ == 'debug':
            # print inspect.stack()
            lib_path = os.path.dirname(__file__)
            call_fn = inspect.stack()[2][1]
            call_ln = inspect.stack()[2][2]
            call_func = inspect.stack()[2][3]
            if call_fn.startswith(lib_path):
                call_fn = call_fn[len(lib_path):]
            if call_fn[0] == '/' or call_fn[0] == '\\':
                call_fn = call_fn[1:]
            new_args[0] = "%s:%s:%s %s" % (call_fn, call_ln, call_func, new_args[0])
        self.old_func(*new_args)
        return 0


def init_log(level, log_file: str, max_bytes=10 * 1024 * 1024, backup_count=5):
    """
    初使化日志，让日志即能屏幕输出，也可以输出到日志文件中
    :param level: 日志级别,可以取的值为logging.INFO, logging.DEBUG等等值
    :param log_file: 输入的log文件
    :param max_bytes: 每个日志文件的最大大小
    :param backup_count: 保留多少个日志文件
    :return:
    """

    logger = logging.getLogger()

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    stdout_handle = logging.StreamHandler()
    stdout_handle.setFormatter(formatter)
    logger.addHandler(stdout_handle)

    file_handle = logging.handlers.RotatingFileHandler(log_file, encoding='UTF-8',
                                                       maxBytes=max_bytes, backupCount=backup_count)
    file_handle.setFormatter(formatter)
    logger.addHandler(file_handle)

    logger.setLevel(level)

    # 把原先的logger对象的函数info、debug等替换掉，当传进入的错误信息中有回车，会在每个回车前多加一些空格，以便打印的好看一些
    func_name_list = ['info', 'debug', 'warn', 'error', 'critical']
    for funcName in func_name_list:
        func = getattr(logger, funcName)
        # 生成一个新的函数对象，替换原先的函数对象
        new_func = _LogFuncObject(func)
        setattr(logger, funcName, new_func)


def is_exit():
    """是否置了退出标志
    :return:bool
    """
    return _exit_flag


def set_exit():
    """
    设置退出状态为真
    :return:无
    """
    global _exit_flag
    _exit_flag = True


def cleanup() -> int:
    """
    优雅的清理应用，把线程优雅的停下来，把pid文件删除掉
    先前已把全局变量_exit_flag设置为True，然后等待各个线程退出
    :return: 返回一个错误码err_code， 0表示成功，其它值看bit位,
    bit位 1 表示调用cleanup_handle错误，
    bit位 2表示线程没有清理干净
    bit位 3表示删除pid文件出错
    """
    global __app_name
    global _cleanup_handles

    err_code = 0
    # 调用退出处理函数
    for handle in _cleanup_handles:
        try:
            handle()
        except Exception:
            err_code = 1
            logging.error(f"cleanup error: {traceback.format_exc()}.")

    # 试图让线程自己中止，如果各个线程检测到g_exit_flag为1了，则会退出
    # 如果线程在9秒后没有停止，则最后会调用exit()强制停止进程

    all_threads = sorted(threading.enumerate(), key=lambda d: d.name)
    logging.debug(f"all_threads: {repr(all_threads)}.")

    thread_cnt = len(all_threads)
    stopped_thread_cnt = 0
    i = 0
    retry_cnt = 30
    for t in all_threads:
        if t.name == 'MainThread':  # 不需要等主线程退出，因为此函数就是在主线程中
            continue
        while True:
            is_alive = False
            try:
                is_alive = t.isAlive()
            except Exception:
                pass
            if is_alive:
                if i > retry_cnt:
                    logging.info(f"Not waiting for the thread({t.name}) to stop!")
                    break
                time.sleep(0.3)
                i += 1
                continue
            else:
                stopped_thread_cnt += 1
                logging.info(f"Thread({t.name}) is stopped.")
                break

    if stopped_thread_cnt != thread_cnt - 1:
        err_code |= 2

    # 删除pid文件
    if os.path.isdir('/run'):
        __run_path = '/run'
    else:
        __run_path = '/var/run'
    pidfile = os.path.join(__run_path, "%s.pid" % __app_name)
    try:
        os.unlink(pidfile)
    except FileNotFoundError:
        pass
    except Exception:
        err_code |= 4
        logging.error(f"cleanup(delete pid file) error: {traceback.format_exc()}.")
    return err_code


def __sig_handle(signum, _frame):
    """
    :param signum:
    :param frame:
    :return:
    """
    global __app_name
    global _exit_flag

    logging.info(f"========== Recv signal {signum},{__app_name} will stop... ==========")
    _exit_flag = 1


def __set_signal():
    """
    设置合适的signal,以便让程序友好退出
    """
    signal.signal(signal.SIGINT, __sig_handle)
    signal.signal(signal.SIGTERM, __sig_handle)
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
    # signal.signal(signal.SIGCHLD, signal.SIG_IGN)


def register_cleanup_handle(handle):
    global _cleanup_handles
    _cleanup_handles.append(handle)
