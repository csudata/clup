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
@Author: liulei
@description: 不带连接池的数据库连接
"""

import logging

import psycopg2
import psycopg2.extras

import config


def connect_db():
    try:
        db_name = config.get('collect_db_name')
        db_host = config.get('collect_db_host')
        db_port = config.get('collect_db_port')
        db_user = config.get('collect_db_user')
        db_pass = config.get("collect_db_pass")
        if not db_host:
            return None
        conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    except Exception:
        return None
    return conn


def __prepare_run_sql(sql):
    conn = connect_db()
    debug_sql = config.get("debug_sql")
    if debug_sql:
        logging.debug(f"Run SQL: {sql}.")
    return conn


def query(sql, args=()):
    """
    执行一条SQL语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param args: tuple数组类型，代表传进来的各个绑定变量
    :return: 返回查询到的结果集，是一个数组，数组中每个元素是一个真正的字典
    """

    conn = __prepare_run_sql(sql)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, args)
    msg = cur.fetchall()
    cur.close()
    conn.close()
    return msg


def execute(sql, args=()):
    """
    执行一条无需要结果的SQL语句，通常是DML语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param args: tuple数组类型，代表传进来的各个绑定变量
    :return: 无
    """

    conn = __prepare_run_sql(sql)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql, args)
    cur.close()
    conn.close()


# 当同时需要执行有多个SQL时，可以放到with DBProcess() as dbp这个with块中，这样只连接数据库一次
class DBProcess:
    """
    可以与with语句联合使用:
        with dbapi.DBProcess() as dbp:
            dbp.execute("DELETE TABLE mytest where id=10")
            rows = dbp.query("SELECT * FROM mytest"
    当然也可以单独使用：
        dbp = DBProcess()
        rows = dbp.query("SELECT * FROM mytest"
        dbp.commit()
        dbp.close()  # 注意不要忘了调用dbp.close()关闭连接
    """

    def __init__(self, _is_dict=True):
        self.conn = connect_db()
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # DBProcess 支持 with 语句
    def __enter__(self):
        return self

    # DBProcess 支持 with 语句
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:  # 有异常回滚
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
        self.cur.close()
        self.conn.close()


# 测试数据库连接
def test_collect_db():
    if not config.get("collect_db_host"):
        return False

    conn = connect_db()
    if conn:
        conn.close()
        return True
    return False
