#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@File        : __init__.py
@Project     : jbt-miner-handler-platform
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""

__author__ = 'Eagle (liuzh@igoldenbeta.com)'

import os
import sys
import json
import datetime
import pandas as pd

from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import connect

# sys.path.append(os.getcwd())
# sys.path.append(os.path.join(os.getcwd(), ".."))

from util.logs_utils import logger


class MysqlClient(object):

    def __init__(self, host='127.0.0.1', port=3306, schema='test', username='root', password='123456', pool_size=5):
        """
        :param host:数据库ip地址
        :param port:数据库端口
        :param schema:库名
        :param username:用户名
        :param password:密码
        """
        self.host = host
        self.port = port
        self.schema = schema
        self.username = username
        self.password = str(password)
        self.pool_size = pool_size
        self.charset = 'utf8'
        self._base_dbconfig = {
            'host': self.host,
            'port': self.port,
            'database': 'information_schema',
            'user': self.username,
            'password': self.password,
            'charset': self.charset
        }
        self._biz_dbconfig = {
            'host': self.host,
            'port': self.port,
            'database': self.schema,
            'user': self.username,
            'password': self.password,
            'charset': self.charset
        }
        # 连接池对象
        self._pool = None
        self._conn = self.get_cnx()
        self._cursor = self._conn.cursor()

    def __create_database(self):
        conn = connect(**self.base_dbconfig)
        cur = conn.cursor()
        create_sql = f'create database if not exists {self.schema} default charset utf8mb4 collate utf8mb4_general_ci'
        cur.execute(create_sql)
        cur.close()
        conn.commit()
        conn.close()

    def get_cnx(self):
        """
            @summary: 静态方法,从连接池中取出连接
            @return MySQLdb.connection
            @这里具体连接数根据机器配置来自定义
        """
        try:
            if not self._pool:
                self._pool = MySQLConnectionPool(pool_name="pool1", pool_size=self.pool_size, pool_reset_session=True, **self._biz_dbconfig)
            return self._pool.get_connection()
        except Exception as e:
            logger.error(f'create_cnxpool Exception： {e}', exc_info=True)
            str_e = str(e)
            if '1049' in str_e and "Unknown database '{}'".format(self.database_name) in str_e:
                self.__create_database()
                self.create_cnxpool()

    def close(self):
        try:
            self._cursor.close()
            self._conn.close()
        except Exception as e:
            logger.error(e)

    @staticmethod
    def __dict_datetime_obj_to_str(result_dict):
        """把字典里面的datatime对象转成字符串，使json转换不出错"""
        if result_dict:
            result_replace = {k: v.__str__() for k, v in result_dict.items() if isinstance(v, datetime.datetime)}
            result_dict.update(result_replace)
        return result_dict

    def execute(self, sql, values=None, exe_type=None):
        cnx = None
        cur = None
        effect_rows = None
        except_redo_times = 3
        while except_redo_times:
            try:
                cnx = self.get_cnx()
                cur = cnx.cursor()
                if values:
                    effect_rows = cur.executemany(sql, values)
                else:
                    effect_rows = cur.execute(sql)
                if exe_type in ['select', 'show']:
                    effect_rows = cur.fetchall()
                    columns = []
                    for field in cur.description:
                        columns.append(field[0])
                    effect_rows = pd.DataFrame(columns=columns, data=effect_rows)
                cur.close()
                if exe_type not in ['select', 'show']:
                    cnx.commit()
                break
            except Exception as e:
                logger.error(f'execute sql:{sql} Exception： {e}', exc_info=True)
                if cnx and exe_type not in ['select', 'show']:
                    cnx.rollback()
                except_redo_times -= 1
                raise Exception(f"sql执行异常,sql={sql}, e={e}")
            finally:
                if cur:
                    cur.close()
                if cnx:
                    cnx.close()
        return effect_rows

    def execute_uncommit(self, cnx, sql, values=None, exe_type=None):
        if cnx and sql:
            cur = None
            effect_rows = None
            except_redo_times = 3
            while except_redo_times:
                try:
                    cur = cnx.cursor()
                    if values:
                        effect_rows = cur.executemany(sql, values)
                    else:
                        effect_rows = cur.execute(sql)
                    if exe_type in ['select', 'show']:
                        effect_rows = cur.fetchall()
                    cur.close()
                    break
                except Exception as e:
                    logger.error(f'execute sql:{sql} Exception： {e}', exc_info=True)
                    if cnx and exe_type not in ['select', 'show']:
                        raise e
                    except_redo_times -= 1
                finally:
                    if cur:
                        cur.close()
            return effect_rows

    def select(self, sql, values=None):
        return self.execute(sql, values, exe_type='select')

    def insert(self, sql, values=None):
        # logger.info(f"insert sql = {sql} \n values = {values}")
        return self.execute(sql, values)

    def update(self, sql, values=None):
        # logger.info(f"update sql = {sql} \n values = {values}")
        return self.execute(sql, values)

    def delete(self, sql, values=None):
        return self.execute(sql, values)


if __name__ == "__main__":
    mc = MysqlClient()
    sql1 = 'SELECT * from (SELECT 2 as A) t where 1=%s'
    result1 = mc.query(sql1, (1,))
    print(json.dumps(result1[1], ensure_ascii=False))
    result1 = mc.query(sql1, (2,))
    print(json.dumps(result1[1], ensure_ascii=False))

    sql2 = 'SELECT * from (SELECT 1, 2, 3 ) t where (1,2,3) in ((%s, %s, %s))'
    param = (1, 2, 3)
    print(json.dumps(mc.query(sql2, param)[1], ensure_ascii=False))
    param = (1, 2, 4)
    print(json.dumps(mc.query(sql2, param)[1], ensure_ascii=False))