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
@description: 自动升级
"""

import os

import config
import dbapi
import run_lib


def get_dbapi_obj():
    upgrade_dbapi = dbapi
    return upgrade_dbapi


def psql_run(sql_fn):
    db_dict = {
        "db_name": config.get('db_name'),
        "db_host": config.get('db_host'),
        "db_port": config.get('db_port'),
        "db_user": config.get('db_user'),
        "db_pass": config.get('db_pass'),
        "psql_cmd": config.get('psql_cmd')
    }

    cmd = f"PGPASSWORD={db_dict['db_pass']} {db_dict['psql_cmd']} -h {db_dict['db_host']} " \
          f"-p {db_dict['db_port']} -U{db_dict['db_user']} -d {db_dict['db_name']} -f {sql_fn}"

    #  当前默认使用的数据库软件是clup机器上的
    err_code, err_msg, out_msg = run_lib.run_cmd_result(cmd)
    if "ERROR:" in err_msg:
        err_code = -1
    return err_code, err_msg, out_msg


def upgrade_common(v1, v2):
    sql_path = config.get_sql_path()
    sql_scripts = os.path.join(sql_path, f'v{v1}_v{v2}.sql')
    try:
        err_code, err_msg, _out_msg = psql_run(sql_scripts)
        if err_code != 0:
            return err_code, err_msg
    except Exception as e:
        return -1, f"run sql file error: \n{sql_scripts}\n{str(e)}\n"
    return 0, ''


upgrade_func_list = [
    ["5.0.0", upgrade_common]
]


def cmp_version(ver1, ver2):
    items1 = ver1.split('.')
    items2 = ver2.split('.')
    cnt = len(items1)
    for i in range(cnt):
        if i >= len(items2):
            return 1
        if int(items1[i]) > int(items2[i]):
            return 1
        elif int(items1[i]) < int(items2[i]):
            return -1
    if len(items1) == len(items2):
        return 0
    else:
        return -1


def check_and_upgrade():
    try:
        upgrade_dbapi = get_dbapi_obj()

        test_conn = upgrade_dbapi.connect_db()
        if not test_conn:
            return -1, "connect clupcdb faild, check clup.conf and start clupcdb"
        test_conn.close()

        # 找到当前版本，放到db_version
        sql = "select count(*) as cnt from pg_class where relname = 'clup_settings' and relkind = 'r'"
        rows = upgrade_dbapi.query(sql)
        if rows[0]['cnt'] <= 0:
            db_version = '0.0'
        else:
            sql = "SELECT content FROM clup_settings where key='db_version'"
            rows = upgrade_dbapi.query(sql)
            if len(rows) <= 0:
                db_version = '0.0'
            else:
                db_version = rows[0]['content']
        pre_version = '1.0'
        for item in upgrade_func_list:
            version = item[0]
            if cmp_version(item[0], db_version) > 0:
                err_code, err_msg = item[1](pre_version, version)
                if err_code != 0:
                    return err_code, err_msg
                try:
                    sql = """UPDATE clup_settings SET content = %s WHERE key='db_version' """
                    upgrade_dbapi.execute(sql, (version,))
                except Exception as e:
                    return -1, f"run sql error: \n{sql}\n{str(e)}\n"
            pre_version = version
    except Exception as e:
        return -1, str(e)
    return 0, ''
