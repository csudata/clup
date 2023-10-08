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
@description: PostgerSQL数据库工具模块
"""

import psycopg2
import psycopg2.extras


def get_stream_status(pri_db_host, pri_db_port, db_user, db_pass, standby_name):
    """
    获得流复制的状态
    :return: 返回一个字符串
    """

    # 先判断是否是10以上的版本：
    sql = """select count(*) as cnt from pg_proc p, pg_namespace n where p.pronamespace=n.oid and n.nspname='pg_catalog' and p.proname = 'pg_current_wal_lsn'"""
    conn = psycopg2.connect(database='template1', user=db_user, password=db_pass, host=pri_db_host, port=pri_db_port)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    if rows[0]['cnt'] > 0:
        sql = "SELECT sync_state||'(delay:'|| pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)||')' " \
              f"  FROM pg_stat_replication where application_name='{standby_name}'"
    else:
        sql = "SELECT sync_state||'(delay:'|| pg_xlog_location_diff(pg_current_xlog_location(), flush_location)||')' " \
              f"  FROM pg_stat_replication where application_name='{standby_name}'"

    cur.execute(sql)
    msg = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return msg[0][0]


def get_repl_delay(pri_db_host, pri_db_port, db_user, db_pass, cur_wal):
    # 先判断是否是10以上的版本：
    sql = """select count(*) as cnt from pg_proc p, pg_namespace n where p.pronamespace=n.oid and n.nspname='pg_catalog' and p.proname = 'pg_current_wal_lsn'"""
    conn = psycopg2.connect(database='template1', user=db_user, password=db_pass, host=pri_db_host, port=pri_db_port)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    if rows[0]['cnt'] > 0:
        sql = "select application_name as repl_name, \n"\
              f"    '{cur_wal}' as current_lsn,\n"\
              f"    pg_wal_lsn_diff('{cur_wal}', sent_lsn) as sent_delay,\n"\
              f"    pg_wal_lsn_diff('{cur_wal}', write_lsn) as write_delay,\n"\
              f"    pg_wal_lsn_diff('{cur_wal}', flush_lsn) as flush_delay,\n"\
              f"    pg_wal_lsn_diff('{cur_wal}', replay_lsn) as replay_delay,\n" \
              "    state,\n" \
              "    sync_state as is_sync\n"\
              " from pg_stat_replication;"
    else:
        sql = "select application_name as repl_name, \n"\
              f"    '{cur_wal}' as current_lsn,\n"\
              f"    pg_xlog_location_diff('{cur_wal}', sent_location) as sent_delay,\n"\
              f"    pg_xlog_location_diff('{cur_wal}', write_location) as write_delay,\n"\
              f"    pg_xlog_location_diff('{cur_wal}', flush_location) as flush_delay,\n"\
              f"    pg_xlog_location_diff('{cur_wal}', replay_location) as replay_delay,\n" \
              "    state,\n" \
              "    sync_state as is_sync\n"\
              " from pg_stat_replication;"

    cur.execute(sql)
    msg = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return msg


def get_last_lsn(host, port, user, password):
    """
    获得数据库的最后LSN(log sequence number)
    :param host:
    :param port:
    :param user:
    :param password:
    :return: 返回三个值，第一值是错误码，如果错误码为0，第二值是LSN，如果错误码不为0，第二个值是错误信息，第三个值是timeline
    """
    try:
        conn = psycopg2.connect(f"dbname='template1'  host='{host}' user='{user}' password='{password}'"
                                f" port={port} replication=database"
                                )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute('IDENTIFY_SYSTEM')
        db_data = cur.fetchall()
        cur.close()
        conn.close()
        return 0, db_data[0][2], db_data[0][1]
    except Exception as e:
        return -1, str(e), -1


def lsn_to_xlog_file_name(timeline, lsn, wal_segment_size):

    cells = lsn.split('/')
    xlog_id = int(cells[0], 16)
    xlog_seg = (int(cells[1], 16) // (wal_segment_size))
    return f"{timeline:08X}{xlog_id:08X}{xlog_seg:08X}"


def sql_query_dict(host, port, db, user, password, sql, binds=()):
    """
    执行一条SQL语句
    :param sql: 字符串类型, 有绑定变量的SQL语句，如select * from table tablename where col1=%s and col2=%s;
    :param binds: tuple数组类型，代表传进来的各个绑定变量
    :return: 返回查询到的结果集，是一个数组，数组中每个元素可以看做是一个数组，也可以看做是一个字典
    """
    try:
        conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, binds)
        msg = cur.fetchall()
        cur.close()
        conn.close()
        return 0, msg
    except Exception as e:
        return -1, str(e)


def sql_exec(host, port, db, user, password, sql, binds=()):
    """
    执行一条无需要结果的SQL语句，通常是DML语句
    :param sql: 字符串类型, 有绑定变量的SQL语句
    :param binds: tuple数组类型，代表传进来的各个绑定变量
    :return: 无
    """
    try:
        conn = psycopg2.connect(database=db, user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql, binds)
        cur.close()
        conn.close()
        return 0, ''
    except Exception as e:
        return -1, str(e)


def get_val_with_unit(val, unit):
    if unit == '':
        return val
    # unit 可能会出现8kB这样的单位：
    if not unit.isalpha():
        i = 0
        num_str = ''
        cnt = len(unit)
        while i < cnt:
            if unit[i] >= '0' and unit[i] <= '9':
                num_str += unit[i]
            else:
                break
            i += 1
        if len(num_str) == 0:
            raise Exception(f"Unknown unit: {unit}")
        num_in_unit = int(num_str)
        unit = unit[i:]
        val = int(val) * num_in_unit

    if unit == 'B':
        return int(val)
    elif unit == 'kB':
        return 1024 * int(val)
    elif unit == 'MB':
        return 1024 * 1024 * int(val)
    elif unit == 'GB':
        return 1024 * 1024 * 1024 * int(val)
    elif unit == 'TB':
        return 1024 * 1024 * 1024 * 1024 * int(val)
    else:
        raise Exception(f"Unknown unit: {unit}")


def get_time_with_unit(val, unit):
    """
    获取时间的ms单位数值
    """
    if unit == '' or unit == 'ms':
        return int(val)
    elif unit == 's':
        return int(val) * 1000
    elif unit == 'min':
        return int(val) * 1000 * 60
    elif unit == 'h':
        return int(val) * 1000 * 60 * 60
    elif unit == 'd':
        return int(val) * 1000 * 60 * 60 * 24
    else:
        raise Exception(f"Unknown unit: {unit}")


def fomart_by_unit(byte_value, unit):
    """
    按数据库设置单位，byte_value值四舍五入转换为该单位数据
    """
    if unit == 'B':
        return int(byte_value)
    elif unit == 'kB':
        return round(int(byte_value) / 1024)
    elif unit == '8kB':
        return round(int(byte_value) / (1024 * 8))
    elif unit == 'MB':
        return round(int(byte_value) / (1024 * 1024))
    elif unit == 'GB':
        return round(int(byte_value) / (1024 * 1024 * 1024))
    elif unit == 'TB':
        return round(int(byte_value) / (1024 * 1024 * 1024 * 1024))
    else:
        raise Exception(f"Unknown unit: {unit}")
