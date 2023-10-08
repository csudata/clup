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
@description: 操作数据库
"""

import traceback

import config
import psycopg2
import psycopg2.extras
import psycopg2.pool


def connect_db(host=None):
    if host:
        db_host = host
    else:
        db_host = config.get('db_host')

    db_name = config.get('db_name')
    db_port = config.get('db_port')
    db_user = config.get('db_user')
    db_pass = config.get("db_pass")
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    return conn



# 当同时需要执行有多个SQL时，可以放到with DBProcess() as dbp这个with块中，这样只连接数据库一次
class DBProcess:
    """
    可以与with语句联合使用:
        with dbapi.DBProcess() as dbp:
            dbp.execute("DELETE TABLE mytest where id=10")
            rows = dbp.query("SELECT * FROM mytest"
    当然也可以单独使用:
        dbp = DBProcess()
        rows = dbp.query("SELECT * FROM mytest"
        dbp.commit()
        dbp.close()  # 注意不要忘了调用dbp.close()关闭连接
    """

    def __init__(self, db_host=None):
        self.err_msg = ''
        self.conn = connect_db(host=db_host)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # DBProcess 支持 with 语句
    def __enter__(self):
        return self

    # DBProcess 支持 with 语句
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:  # 有异常回滚
            self.err_msg = traceback.format_exception(exc_type, exc_val, exc_tb)
            self.conn.rollback()
            self.close()
            return False
        # 无异常 则 commit 提交
        self.conn.commit()
        self.close()

    # 如果execute 失败 raise InternalError
    def query(self, sql, args=()):
        self.cur.execute(sql, args)
        return self.cur.fetchall()

    def execute(self, sql, args=()):
        self.cur.execute(sql, args)

    def rollback(self):
        self.conn.rollback()

    def commit(self):
        self.conn.commit()

    def close(self):
        if self.cur:
            self.cur.close()
            self.cur = None
        if self.conn:
            self.conn.close()
            self.conn = None


def query(sql, args=()):
    """
    使用连接池的方式执行一条SQL语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param args: tuple数组类型，代表传进来的各个绑定变量
    :return: 返回查询到的结果集，是一个数组，数组中每个元素是一个真正的字典
    """
    rows = []
    with DBProcess() as dbp:
        # logging.info(f"run: {sql}")
        rows = dbp.query(sql, args)
    return rows


def execute(sql, args=()):
    """
    使用连接池的方式执行一条无需要结果的SQL语句，通常是DML语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param args: tuple数组类型，代表传进来的各个绑定变量
    :return: 无
    """

    with DBProcess() as dbp:
        dbp.execute(sql, args)


def get_db_conn(db_host, db_port, db_user, db_pass, db_name='template1'):
    conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    return conn


def conn_query(conn, sql, args=()):
    """
    执行一条SQL语句
    :param sql: 字符串类型, 有绑定变量的SQL语句，如select * from table tablename where col1=%s and col2=%s;
    :param args: tuple数组类型，代表传进来的各个绑定变量
    :return: 返回查询到的结果集，是一个数组，数组中每个元素可以看做是一个数组，也可以看做是一个字典
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, args)
    msg = cur.fetchall()
    conn.commit()
    cur.close()
    return msg


def conn_execute(conn, sql, args=()):
    """
    执行一条无需要结果的SQL语句，通常是DML语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param args: tuple数组类型，代表传进来的各个绑定变量
    :return: 无
    """

    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql, args)
    cur.close()
    conn.close()
