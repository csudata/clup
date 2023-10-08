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
@description: 集群操作
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor

import database_state
import db_encrypt
import dbapi
import psycopg2
import rpc_utils
from rpc_utils import get_rpc_connect


def get_version():
    """
    获取clup版本
    @return:
    """
    try:
        sql = "SELECT content FROM clup_settings WHERE key='db_version' "
        rows = dbapi.query(sql)
        return rows
    except Exception as e:
        logging.error(repr(e))
        return []


def recover_pending():
    # 状态2: "Reparing", 3: "Failover" 都是中间状态，如果clup都重启了，这些状态都改成-1，即"Failed"失败状态
    sql = 'UPDATE clup_cluster SET state = -1 WHERE state in (2,3,4)'
    dbapi.execute(sql)

    # 这是任务状态0表示正在运行，也是一个临时状态，当clup重启后，需要把状态都改成-1
    dbapi.execute("UPDATE clup_general_task SET state = -1, last_msg = '执行被中断!' WHERE state = 0")

    # db_state = 2 表示还在创建中的状态，重启以后改为-1
    dbapi.execute("UPDATE clup_db SET db_state = -1 WHERE db_state = 2 OR db_state = 3")
    dbapi.execute("UPDATE clup_db SET state = 2 WHERE state != 1")


def get_cluster_count():
    sql = "SELECT count(*) as cnt FROM clup_cluster"
    rows = dbapi.query(sql)
    cnt = rows[0]['cnt']
    return cnt


def get_cluster(cluster_id):
    sql = "SELECT * FROM clup_cluster WHERE cluster_id=%s"
    rows = dbapi.query(sql, (cluster_id,))
    if not rows:
        return None
    row = rows[0]
    # 此处提取字典内部的值到外部
    row.update(row.pop('cluster_data'))
    return row


def get_cluster_with_db_list(cluster_id):
    with dbapi.DBProcess() as dbp:
        sql = "SELECT * FROM clup_cluster WHERE cluster_id=%s"
        rows = dbp.query(sql, (cluster_id,))
        if not rows:
            return None
        row = rows[0]
        cluster_dict = row['cluster_data']
        del row['cluster_data']
        cluster_dict.update(row)
        sql = "SELECT db_id, cluster_id, state, pgdata, is_primary, " \
              "repl_app_name,host,repl_ip" \
              "  FROM clup_db WHERE cluster_id=%s"
        db_rows = dbp.query(sql, (cluster_id,))
        if db_rows:
            cluster_dict['db_list'] = db_rows
        return cluster_dict


def get_cluster_state(cluster_id):
    """
    :param cluster_id:
    :return:
    """
    sql = "SELECT state FROM clup_cluster WHERE cluster_id=%s"
    rows = dbapi.query(sql, (cluster_id,))
    if not rows:
        return None
    return rows[0]['state']


def get_cluster_type(cluster_id):
    sql = "SELECT cluster_type FROM clup_cluster WHERE cluster_id=%s"
    rows = dbapi.query(sql, (cluster_id,))
    if not rows:
        return None
    return rows[0]['cluster_type']


def get_cluster_db_list(cluster_id):
    """
    :param cluster_id:
    :return:
    """

    sql = ("SELECT db_id, up_db_id, cluster_id,scores, state, pgdata, is_primary, repl_app_name, host, repl_ip, port, "
           " db_detail->>'instance_type' AS instance_type, "
           " db_detail->>'os_user' as os_user, db_detail->>'os_uid' as os_uid, "
           " db_detail->>'pg_bin_path' as pg_bin_path, db_detail->>'version' as version, "
           " db_detail->>'db_user' as db_user, db_detail->>'db_pass' as db_pass, "
           " db_detail->>'repl_user' as repl_user, db_detail->>'repl_pass' as repl_pass, "
           " db_detail->'room_id' as room_id, "
           " db_detail->'polar_type' as polar_type, "
           " db_detail->'reset_cmd' as reset_cmd, "
           " db_type "
           " FROM clup_db WHERE cluster_id=%s ORDER BY db_id")
    rows = dbapi.query(sql, (cluster_id,))
    if not rows:
        return []
    for row in rows:
        if row['room_id'] is None:
            row['room_id'] = '0'
    return list(rows)


def get_all_cluster():
    with dbapi.DBProcess() as dbp:
        sql = (
            "SELECT cluster_id, cluster_type, cluster_data,state, lock_time "
            "  FROM clup_cluster")
        rows = dbp.query(sql)
        if not rows:
            return []

        ret_rows = []
        for row in rows:
            cluster_dict = row['cluster_data']
            if 'cstlb_list' in cluster_dict:
                cluster_dict['cstlb_list'] = cluster_dict['cstlb_list'].split(',')

            ret_row = {'cluster_id': row['cluster_id'],
                       'cluster_type': row['cluster_type'],
                       'state': row['state'],
                       'lock_time': row['lock_time']
                       }
            ret_row.update(cluster_dict)
            ret_rows.append(ret_row)

        # sql = (
        #     "SELECT db_id, cluster_id, state, pgdata, is_primary, repl_app_name, "
        #     "host, repl_ip FROM clup_db")
        sql = """
        SELECT db_id,cluster_id,state pgdata, is_primary, repl_app_name, host, repl_ip FROM clup_db INNER JOIN
        (SELECT cluster_id, cluster_type FROM clup_cluster) as cluster
        USING (cluster_id) WHERE cluster.cluster_type = 1;
        """
        rows = dbp.query(sql)
        all_db_dict = {}
        print(type(all_db_dict))
        for row in rows:
            cluster_id = row['cluster_id']
            all_db_dict.setdefault(cluster_id, [])
            all_db_dict[cluster_id].append(row)

        for row in ret_rows:
            cluster_id = row['cluster_id']
            if cluster_id in all_db_dict:
                row['db_list'] = all_db_dict[cluster_id]
        return ret_rows


def get_cluster_id_list():
    sql = "SELECT cluster_id FROM clup_cluster"
    rows = dbapi.query(sql)
    cluster_id_list = []
    for row in rows:
        cluster_id_list.append(row['cluster_id'])
    return cluster_id_list


def get_cluster_db_ip_list(cluster_id):
    rows = dbapi.query(
        "SELECT host FROM clup_db where cluster_id=%s and is_primary=0",
        (cluster_id,))
    cluster_db_host_list = [row['host'] for row in rows]
    return cluster_db_host_list


def add_cluster(cluster_dict):
    """
    :param cluster_dict:
    :return: 返回cluster_id
    """

    cluster_type = cluster_dict['cluster_type']
    if 'db_list' in cluster_dict:
        db_list = cluster_dict.pop('db_list')
    else:
        db_list = []

    col_list = ['cluster_type', 'state', 'lock_time']
    cluster_row = {}
    for col_name in col_list:
        cluster_row[col_name] = cluster_dict.pop(col_name)
    cluster_data = json.dumps(cluster_dict)

    with dbapi.DBProcess() as dbp:
        rows = dbp.query(
            "INSERT INTO clup_cluster(cluster_type ,cluster_data, state, lock_time)"
            " VALUES(%s,%s,%s,%s) RETURNING cluster_id",
            (cluster_row['cluster_type'], cluster_data,
             cluster_row['state'], cluster_row['lock_time'])
        )
        cluster_id = rows[0]['cluster_id']

        if cluster_type == 1:
            for i in db_list:
                dbp.execute(
                    "INSERT INTO clup_db(cluster_id, state, pgdata, "
                    "is_primary, repl_app_name, host, repl_ip) "
                    "values(%s,%s,%s,%s,%s,%s,%s)",
                    (cluster_id, i['state'], i['pgdata'], i['is_primary'],
                     i['repl_app_name'], i['host'], i['repl_ip']))
        return cluster_id


def set_cluster_state(cluster_id, state):
    """
    :param cluster_id:
    :param state:
    :return:
    """
    dbapi.execute(
        "UPDATE clup_cluster SET state = %s WHERE cluster_id=%s",
        (state, cluster_id))


def test_and_set_cluster_state(cluster_id, test_state_list, set_state):
    """
    :param cluster_id:
    :param state:
    :return: 返回None表示，没有找到状态的值，否则返回之前的状态
    """
    str_in_cond = ', '.join([str(k) for k in test_state_list])

    rows = dbapi.query(
        f"UPDATE clup_cluster a SET state = %s FROM clup_cluster b WHERE a.cluster_id=b.cluster_id and a.cluster_id=%s AND a.state in ({str_in_cond}) RETURNING b.state",
        (set_state, cluster_id))
    if len(rows) < 1:
        return None
    else:
        return rows[0]['state']


def set_cluster_data_attr(cluster_id, attr, value):
    """
    :param cluster_id:
    :param attr:
    :param value:
    :return:
    """
    set_dict = {attr: value}
    dbapi.execute(
        "UPDATE clup_cluster SET cluster_data = cluster_data || %s WHERE cluster_id=%s",
        (json.dumps(set_dict), cluster_id))


def set_cluster_data_by_dict(cluster_id, set_dict):
    """
    :param cluster_id:
    :param set_dict:
    :return:
    """

    dbapi.execute(
        "UPDATE clup_cluster SET cluster_data = cluster_data || %s WHERE cluster_id=%s",
        (json.dumps(set_dict), cluster_id))


def set_cluster_db_state(cluster_id, db_id, state):
    """
    :param cluster_id:
    :param db_id:
    :param state:
    :return:
    """
    rows = dbapi.query(
        "UPDATE clup_db SET state = %s WHERE cluster_id=%s and db_id = %s RETURNING state",
        (state, cluster_id, db_id))
    if len(rows) < 1:
        return False
    else:
        return True


def set_cluster_db_attr(cluster_id, db_id, attr, value):
    rows = dbapi.query(
        "UPDATE clup_db SET {col_name}=%s WHERE cluster_id=%s and db_id = %s RETURNING db_id".format(col_name=attr),
        (value, cluster_id, db_id))
    if len(rows) < 1:
        return False
    else:
        return True


def set_host_attr(ip, attr, value):
    with dbapi.DBProcess() as dbp:
        sql = "SELECT * FROM clup_host WHERE ip =%s"
        rows = dbp.execute(sql, (ip,))
        if not rows:
            return False
        attr_dict = rows[0]['data']
        attr_dict[attr] = value
        rows = dbp.query("UPDATE clup_host SET data = %s WHERE ip=%s RETURNING ip", (value, ip))
        if len(rows) < 1:
            return False
        else:
            return True


def register_host(ip, hostname, mem_size, cpu_cores, cpu_threads, os_type):
    with dbapi.DBProcess() as dbp:
        sql = "SELECT * FROM clup_host WHERE ip =%s"
        rows = dbp.query(sql, (ip,))
        if not rows:
            attr_dict = {"hostname": hostname, "os": os_type, "cpu_cores": cpu_cores, "mem_size": mem_size}
            if cpu_threads:
                attr_dict['cpu_threads'] = cpu_threads

            rows = dbp.query("INSERT INTO clup_host(ip, data) values(%s,%s) returning hid",
                    (ip, json.dumps(attr_dict)))

        else:
            attr_dict = rows[0]['data']
            attr_dict['hostname'] = hostname
            attr_dict['os'] = os_type
            attr_dict['cpu_cores'] = cpu_cores
            # change unit to MB
            attr_dict['mem_size'] = int(mem_size / 1024 / 1024)
            if cpu_threads:
                attr_dict['cpu_threads'] = cpu_threads

            rows = dbp.query("UPDATE clup_host SET data = %s WHERE ip=%s returning hid",
                    (json.dumps(attr_dict), ip))
        return rows[0]['hid']


def get_hid(ip):
    """
    获取hid
    :param db_conn: 数据库连接
    :param ip: agent的ip地址
    :return: hid
    """
    sql = "select hid from clup_host where ip='%s'"
    rows = dbapi.query(sql, (ip, ))
    if rows:
        return rows[0]['hid']


def get_is_primary(ip):
    """
    查看是否是主库
    :param ip:
    :return:返回1或0,1是主库
    """
    sql = "select is_primary from clup_db where host=%s"
    rows = dbapi.query(sql, (ip, ))
    if rows:
        return rows[0]['is_primary']


def get_database_data():
    db_state_list = [database_state.CREATING, database_state.REPAIRING]
    str_db_state_list = str(db_state_list)[1:-1]
    sql = "SELECT pgdata,db_id, up_db_id, is_primary,host,port,instance_name, " \
          "db_detail->>'db_user' as db_user," \
          "db_detail->>'db_pass' as db_pass," \
          "db_detail->>'instance_type' as instance_type, db_detail->'version' as version " \
          f" FROM clup_db WHERE cluster_id is NULL AND db_state not in ({str_db_state_list})"
    rows = dbapi.query(sql)
    return rows


def get_all_db_info():
    """
    获取所有数据库的信息
    :param state:
    :return: db_rows
    """

    db_state_list = [database_state.CREATING, database_state.REPAIRING]
    str_db_state_list = str(db_state_list)[1:-1]
    sql = " select cluster_id, pgdata, up_db_id, db_state, db_id,is_primary,host,cluster_data->>'port' as port, cluster_type, " \
          "db_detail->>'db_user' as db_user," \
          "cluster_data->>'probe_db_name' as probe_db_name," \
          "db_detail->>'db_pass' as db_pass," \
          "db_detail->>'instance_type' as instance_type, db_detail->'version' as version, " \
          "(db_detail->>'topsql')::int as topsql " \
          " from clup_db inner join clup_cluster using(cluster_id) " \
          f" WHERE db_state not in ({str_db_state_list}) or db_state is null"
    cluster_rows = dbapi.query(sql)

    db_state_list = [database_state.CREATING, database_state.REPAIRING]
    str_db_state_list = str(db_state_list)[1:-1]
    sql = "SELECT pgdata,db_id, up_db_id, db_state, is_primary,host,port,instance_name, " \
          "db_detail->>'db_user' as db_user," \
          "db_detail->>'db_pass' as db_pass," \
          "db_detail->>'instance_type' as instance_type, db_detail->'version' as version, " \
          "(db_detail->>'topsql')::int as topsql " \
          f" FROM clup_db WHERE cluster_id is NULL AND db_state not in ({str_db_state_list})"
    db_rows = dbapi.query(sql)
    db_rows.extend(cluster_rows)
    return db_rows


def get_db_info(db_id):
    """
    :param db_id:
    :return: 根据cluster_id返回集群和相关数据库所有信息
    """
    # 要优先使用clup_cluster中的复制用户和复制密码
    sql = "select db_id,cluster_id,up_db_id, state as state, is_primary, host,port,pgdata,db_state,port," \
          " repl_ip, repl_app_name, db_detail->'instance_type' as instance_type, " \
          "db_detail->'db_user' as db_user, db_detail->'db_pass' as db_pass, " \
          "db_detail->'repl_user' as repl_user, db_detail->'repl_pass' as repl_pass, " \
          "db_detail->'tblspc_dir' as tblspc_dir, " \
          "db_detail->'version' as version, db_detail->>'cpu' as cpu, " \
          "db_detail->'pg_bin_path' as pg_bin_path, " \
          "db_detail->'os_user' as os_user, db_detail->'os_uid' as os_uid," \
          "db_detail->'polar_type' as polar_type, "\
          "db_detail->'cpu_list' as cpu_list, " \
          "db_detail->'memory_size' as memory_size, " \
          "db_detail->'is_exclusive' as is_exclusive, " \
          "db_detail->'reset_cmd' as reset_cmd, " \
          "db_type" \
          " from clup_db" \
          " where db_id=%s"

    rows = dbapi.query(sql, (db_id,))
    return rows


# 获取db_id,数据库类型
def get_db_type(db_id):
    """
    :param db_id:
    :return: 根据db_id返回数据库类型;(sql v4.0-v5.0)1-pg，2-mysql, 3-orale, 11-polardb';
    """
    sql = "SELECT db_type FROM clup_db WHERE db_id=%s"
    rows = dbapi.query(sql, (db_id,))
    if not rows:
        return None
    return rows[0]['db_type']


# 根据host和pgdata获取db_type
def search_db_type(rpc, pgdata):
    host = rpc.trans.ip
    sql = "SELECT host, db_id, db_type," \
        " db_detail->'polar_type' as polar_type" \
        " FROM clup_db WHERE host=%s AND pgdata=%s"
    rows = dbapi.query(sql, (host, pgdata))
    if not rows:
        return None
    return rows[0]


def get_host_info(object_id):
    """
    :param object_id:
    :return: 根据cluster_id返回集群和相关数据库所有信息
    """
    sql = "select hid as object_id, ip as host from clup_host where hid=%s"
    rows = dbapi.query(sql, (object_id, ))
    return rows


def get_db_conn(db_dict):
    try:
        db_name = db_dict.get('db_name', 'template1')
        db_host = db_dict['host']
        db_port = db_dict['port']
        db_user = db_dict['db_user']
        # real_pass 用来区分真实密码和解密后的密码
        if db_dict.get('real_pass'):
            db_pass = db_dict['db_pass']
        else:
            db_pass = db_encrypt.from_db_text(db_dict['db_pass'])
        conn = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
        return conn
    except Exception as e:
        return str(e)


def get_db_conn_info(db_id):
    sql = "SELECT cluster_id,host, port, db_detail->'db_user' as db_user, db_detail->'db_pass' as db_pass " \
          " FROM clup_db WHERE db_id = %s"
    rows = dbapi.query(sql, (db_id, ))
    if len(rows) == 0:
        return {}
    return rows[0]


def sql_query(conn, sql, args=()):
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


def execute(conn, sql, args=()):
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


# 获取集群中数据库数量
def get_db_count(cluster_id):
    sql = "select count(*) from clup_db where cluster_id= %s "
    rows = dbapi.query(sql, (cluster_id,))
    db_count = rows[0]['count']
    return db_count


def get_cluster_db_host_list(cluster_id):
    sql = "SELECT host,is_primary,db_detail->'room_id' as room_id FROM clup_db WHERE cluster_id=%s"
    rows = dbapi.query(sql, (cluster_id,))
    return rows


def set_new_read_vip_host(cluster_id, host):
    sql = f"""UPDATE clup_cluster SET cluster_data=jsonb_set(cluster_data, '{{read_vip_host}}', '"{host}"') WHERE cluster_id = {cluster_id}"""
    dbapi.execute(sql)


def get_cluster_vip(cluster_id):
    sql = "SELECT cluster_data->'read_vip' AS read_vip, cluster_data->'vip' as vip, " \
          "cluster_data->'read_vip_host' AS read_vip_host FROM clup_cluster WHERE cluster_id =%s"
    rows = dbapi.query(sql, (cluster_id, ))
    return rows[0]


def get_primary_host(cluster_id):
    sql = "SELECT host FROM clup_db WHERE is_primary =1 AND cluster_id = %s"
    rows = dbapi.query(sql, (cluster_id,))
    ret = rows[0] if rows else {}
    return ret


def get_cluster_db(cluster_id):
    sql = """
    SELECT db_id, port,db_state, host,is_primary,pgdata,db_detail->>'instance_type' as instance_type
     FROM clup_db WHERE cluster_id=%s
    """
    rows = dbapi.query(sql, (cluster_id,))
    return rows


def get_primary_info(cluster_id):
    sql = """
        SELECT  db_id, cluster_id,scores, clup_db.state as state, pgdata, is_primary, repl_app_name, host, repl_ip,
        instance_name,port,
         db_detail->>'instance_type' as instance_type, db_detail->>'db_user' as db_user
          FROM clup_db INNER JOIN clup_cluster USING (cluster_id) WHERE cluster_id=%s AND is_primary=1;
        """
    rows = dbapi.query(sql, (cluster_id,))
    if not rows:
        return {}
    return rows[0]



def get_db_state(db_id):
    # leifliu Test: sql-add db_state
    sql = """
    SELECT db_state, state, is_primary FROM clup_db WHERE db_id= %s
    """
    rows = dbapi.query(sql, (db_id,))
    return rows[0]


def get_cluster_primary(cluster_id):
    sql = """
    SELECT db_id, pgdata, port, host, db_detail->>'instance_type' as instance_type
     FROM clup_db WHERE is_primary=1 AND cluster_id=%s
    """
    rows = dbapi.query(sql, (cluster_id,))
    return rows


def use_db_id_get_cluster_type(db_id):
    sql = """
    SELECT cluster_type FROM clup_cluster FULL JOIN clup_db USING(cluster_id) WHERE clup_db.db_id=%s
    """
    rows = dbapi.query(sql, (db_id,))
    return rows


def update_db_state(db_id, db_state):
    sql = """ UPDATE clup_db SET db_state= %s WHERE db_id = %s"""
    dbapi.execute(sql, (db_state, db_id))


def extend_database(db_id, params_dict):
    for k, v in params_dict.items():
        key = '{' + k + '}'
        sql = """UPDATE clup_db SET db_detail=jsonb_set(db_detail, %s , '%s') WHERE db_id =%s"""
        dbapi.execute(sql, (key, int(v), db_id))


def get_cluster_min_scores_db(cluster_id, db_id):
    sql = "SELECT db_id, scores FROM clup_Db WHERE cluster_id = %s AND up_db_id =%s ORDER BY scores"
    rows = dbapi.query(sql, (cluster_id, db_id))
    return rows


def get_all_child_db(db_id):
    sql = """with RECURSIVE cte as
                   (
                       SELECT a.db_id, a.up_db_id, false as is_cycle,ARRAY[a.db_id] as path_array FROM clup_db a WHERE db_id = %s
                       union all
                       SELECT b.db_id, b.up_db_id, b.db_id=ANY(path_array), path_array || b.db_id  FROM clup_db b inner join cte c on c.db_id=b.up_db_id
                       WHERE NOT is_cycle
                   ) SELECT db_id, up_db_id FROM cte """
    rows = dbapi.query(sql, (db_id, ))
    return rows


def get_all_cascaded_db(db_id):
    sql = """ with RECURSIVE cte as
                       (
                           SELECT a.db_id, a.up_db_id, a.host, a.port, a.db_state FROM clup_db a WHERE db_id = %s
                           union
                           SELECT b.db_id, b.up_db_id, b.host, b.port, b.db_state FROM clup_db b
                           inner join cte c on c.up_db_id=b.db_id or c.db_id = b.up_db_id
                       ) SELECT db_id, up_db_id, host, port, db_state FROM cte  ORDER BY db_id; """
    rows = dbapi.query(sql, (db_id, ))
    return rows


def get_primary_db(db_id):
    cluster_id_sql = "SELECT cluster_id FROM clup_db WHERE db_id = %s"
    rows = dbapi.query(cluster_id_sql, (db_id, ))
    if not rows:
        return []
    cluster_id = rows[0]['cluster_id']
    if cluster_id is not None:
        sql = f"SELECT db_id, up_db_id, host, port, pgdata FROM clup_db WHERE cluster_id={cluster_id} AND is_primary=1"
    else:
        # 递归SQL找到主库
        sql = """with RECURSIVE cte as
                    (
                        SELECT a.db_id, a.up_db_id,a.host,a.port,a.pgdata,false as is_cycle,ARRAY[a.db_id] as path_array  FROM clup_db a WHERE db_id = %s
                        union all
                        SELECT b.db_id, b.up_db_id,b.host,b.port, b.pgdata, b.db_id=ANY(path_array), path_array || b.db_id FROM clup_db b inner join cte c on c.up_db_id=b.db_id
                        WHERE NOT is_cycle
                    ) SELECT db_id, up_db_id,host, port,pgdata FROM cte WHERE up_db_id IS NULL ;
                    """
    rows = dbapi.query(sql, (db_id, ))
    if len(rows) == 0:
        return ''
    return rows


def get_lower_db(db_id):
    """
    查询数据库id为db_id是否有备库
    """
    sql = "SELECT db_id, repl_app_name, pgdata, host, port, instance_name, scores, state, " \
        "db_detail->>'os_user' as os_user, db_detail->'db_user' as db_user, " \
        "db_detail->'db_pass' as db_pass " \
        " FROM clup_db WHERE up_db_id= %s "
    rows = dbapi.query(sql, (db_id, ))
    if not rows or len(rows) == 0:
        return []
    return rows


def get_db_and_lower_db(db_id):
    sql = "SELECT db_id, cluster_id, scores, pgdata, is_primary, " \
        "repl_app_name, host, repl_ip, port, state, db_state, " \
        "db_detail->>'pg_bin_path' as pg_bin_path,  " \
        "db_detail->>'os_user' as os_user, db_detail->>'os_uid' as os_uid, " \
        "db_detail->'instance_type' as instance_type, db_detail->>'version' as version, " \
        "db_detail->'db_user' as db_user, db_detail->'db_pass' as db_pass, " \
        "db_detail->'repl_user' as repl_user, db_detail->'repl_pass' as repl_pass, " \
        "db_detail->'room_id' as room_id  FROM clup_db WHERE up_db_id= %s OR db_id = %s"
    rows = dbapi.query(sql, (db_id, db_id))
    if not rows:
        return []
    return rows


def get_up_db(db_id):
    sql = "SELECT up_db_id FROM clup_db WHERE db_id = %s "
    rows = dbapi.query(sql, (db_id,))
    return rows


def update_up_db_id(up_db_id, db_id, is_primary):
    sql = f"UPDATE clup_db SET up_db_id = {up_db_id}, is_primary = {is_primary} WHERE db_id = %s"
    dbapi.execute(sql, (db_id,))


def get_current_wal_lsn(conn):
    try:
        # 10版本及以上用wal,9版本用xlog
        sql = "select count(*) as cnt from pg_proc p, pg_namespace n " \
              " where p.pronamespace=n.oid and n.nspname='pg_catalog' and p.proname = 'pg_current_wal_lsn'"
        rows = sql_query(conn, sql)
        sql = "SELECT pg_current_wal_lsn() AS cur_wal;"
        if rows[0]['cnt'] == 0:
            sql = "SELECT pg_current_xlog_location() AS cur_wal;"
        rows = sql_query(conn, sql)

    except Exception:
        rows = []
    return rows


def get_db_host(db_id):
    sql = "SELECT host FROM clup_db WHERE db_id=%s"
    rows = dbapi.query(sql, (db_id, ))
    if rows:
        return rows[0]['host']
    else:
        return None


def get_db_cluster_type(db_id):
    sql = "SELECT cluster_id,cluster_type FROM clup_db FULL JOIN clup_cluster " \
          "using (cluster_id) WHERE db_id = %s"
    rows = dbapi.query(sql, (db_id,))
    if rows:
        return rows[0]
    else:
        return {'cluster_id': None, 'cluster_type': None}


def get_db_version(db_id):
    sql = "SELECT db_detail->'version' as version FROM clup_db WHERE db_id = %s"
    rows = dbapi.query(sql, (db_id, ))
    if len(rows) == 0:
        return None
    return rows[0]['version']


def get_cluster_name(cluster_id):
    sql = "select cluster_data->'cluster_name' as cluster_name from clup_cluster where cluster_id = %s"
    rows = dbapi.query(sql, (cluster_id, ))
    if len(rows) == 0:
        return dict()
    return rows[0]


def set_node_state(db_id, state):
    sql = "UPDATE clup_db SET state=%s WHERE db_id=%s"
    dbapi.execute(sql, (state, db_id))


def update_ha_state(db_id, state):
    sql = "SELECT count(*) as cnt FROM clup_db WHERE db_id = %s"
    rows = dbapi.query(sql, (db_id,))
    if rows[0]['cnt'] == 0:
        return -1, f'Database not found: db_id={db_id}'
    try:
        sql = "UPDATE clup_db SET state = %s WHERE db_id = %s"
        dbapi.execute(sql, (state, db_id))
    except Exception as e:
        return -1, f'Failed to update HA status: {repr(e)}'
    return 0, ''


def get_primary_db_count():
    sql = """SELECT count(*) as cnt FROM clup_db WHERE is_primary =1"""
    rows = dbapi.query(sql)
    return rows[0]['cnt']


def get_standby_db_count():
    sql = """SELECT count(*) as cnt FROM clup_db WHERE is_primary !=1"""
    rows = dbapi.query(sql)
    return rows[0]['cnt']


def get_cpu_cores():
    sql = """select coalesce(sum((data->>'cpu_cores')::int),0) as cpu_cores from clup_host"""
    rows = dbapi.query(sql)
    return rows[0]['cpu_cores']


def get_bit_or_db_type():
    sql = """select coalesce(bit_or((2^(db_type-1))::bigint),1) as bit_or_db_type from clup_db"""
    rows = dbapi.query(sql)
    return rows[0]['bit_or_db_type']


def get_up_db_and_standby(db_id):
    """
    获取目标库的备库或者主库
    """
    relevance_db_list = []
    search_sql = "select db_id from clup_db where up_db_id = %s"
    rows = dbapi.query(search_sql, (db_id,))
    if len(rows) > 0:
        relevance_db_list += [row['db_id'] for row in rows]
    search_sql = "select up_db_id from clup_db where db_id = %s and up_db_id is not null"
    rows = dbapi.query(search_sql, (db_id,))
    if len(rows) > 0:
        relevance_db_list += [row['up_db_id'] for row in rows]
    return relevance_db_list


def create_replication_user(db_host, db_port, db_user, db_pass, repl_user, repl_pass):
    """
    检查流复制用户，如果不存在则创建
    """
    try:
        db_dict = {
            "db_host": db_host,
            "db_port": db_port,
            "db_user": db_user,
            "db_pass": db_pass,
            "repl_user": repl_user,
            "repl_pass": repl_pass
        }

        conn = dbapi.get_db_conn(db_host, db_port, db_user, db_pass)
        # 检查该用户是否存在
        sql = "select userepl from pg_user where usename=%(repl_user)s"
        rows = dbapi.conn_query(conn, sql, db_dict)
        if len(rows) == 0:
            # 没有该复制用户，就创建一个
            sql = f"CREATE USER {repl_user} replication LOGIN  ENCRYPTED PASSWORD %(repl_pass)s"
            dbapi.conn_execute(conn, sql, db_dict)
            conn.close()
            return 0, ''
        if not rows[0]:
            # 用户存在但是没有复制权限
            sql = f"alter user {repl_user}  replication login ENCRYPTED PASSWORD %(repl_pass)s"
            dbapi.conn_execute(conn, sql, db_dict)
            conn.close()
            return 0, ''
    except Exception as e:
        return -1, f'create user error: {repr(e)}'
    return 0, ''



def format_byte(num):
    """
    传入单位为字节数，返回适当的数据大小写法
    """
    try:
        for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if num < 1024.0:
                return "%3.1f %s" % (num, x)
            num /= 1024.0
        return num
    except Exception:
        return -1


def convert_bytes(num):
    """
    传入带单位的文件大小数，转换为字节单位
    """
    # 根据传入参数的单位的KB,MB,GB转换为字节
    try:
        if 'KB' in num:
            return float(num.replace('KB', '')) * 1024
        if 'K' in num:
            return float(num.replace('K', '')) * 1024
        elif 'MB' in num:
            return float(num.replace('MB', '')) * 1024 ** 2
        elif 'M' in num:
            return float(num.replace('M', '')) * 1024 ** 2
        elif 'GB' in num:
            return float(num.replace('GB', '')) * 1024 ** 3
        elif 'G' in num:
            return float(num.replace('G', '')) * 1024 ** 3
        else:
            return float(num)
    except Exception:
        return -1


def get_clients_list_by_thread(nodes_ip_list: list, max_thread_num: int = 10, conn_timeout: int = 10):
    """
    :return [(node_ip, err_code, client or err_msg), (...), ...]
    """
    pool = ThreadPoolExecutor(max_thread_num)
    get_client_threads_list = [(node_ip, pool.submit(get_rpc_connect, node_ip, conn_timeout)) for node_ip in nodes_ip_list]

    return [(node_ip, *thread.result())  # 利用* 返回(ip, is_success, client)格式
            for node_ip, thread in get_client_threads_list]


def get_clients_dict_by_thread(nodes_ip_list: list, max_thread_num: int = 10, conn_timeout: int = 10):
    """
    :return dict type {node_ip: (err_code, client or err_msg)}
    """
    pool = ThreadPoolExecutor(max_thread_num)
    get_client_threads_list = [(node_ip, pool.submit(get_rpc_connect, node_ip, conn_timeout)) for node_ip in nodes_ip_list]
    return {node_ip: thread.result() for node_ip, thread in get_client_threads_list}
