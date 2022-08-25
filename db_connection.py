#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 2022/6/23 13:55
# pip3 install DBUtils==1.3
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from utils.logs_utils import logger


class Mysqldb(object):
    # 初始化
    def __init__(self, config):
        # 初始化方法中调用连接数据库的方法
        self.config = config
        self.conn = self.get_conn()
        # 调用获取游标的方法
        self.cursor = self.get_cursor()

    # 连接数据库
    def get_conn(self):
        # **config代表不定长参数
        conn = pymysql.connect(**self.config)
        return conn

    # 获取游标
    def get_cursor(self):
        cursor = self.conn.cursor()
        return cursor

    # 查询sql语句返回所有数据
    def select_all(self, sql):
        self.cursor.execute(sql)
        logger.info(f'查询sql为:{sql}')
        return self.cursor.fetchall()

    # 查询sql语句返回一条数据
    def select_one(self, sql):
        self.cursor.execute(sql)
        logger.info(f'查询sql为:{sql}')
        return self.cursor.fetchone()

    # 查询sql语句返回指定条数数据
    def select_many(self, sql, num):
        self.cursor.execute(sql)
        logger.info(f'查询sql为:{sql}')
        return self.cursor.fetchmany(num)

    # 增删改除了sql语句不一样其他都一样，都需要提交
    def commit_data(self, sql, data_list=None):
        try:
            # 执行语句
            if data_list:
                self.cursor.executemany(sql, data_list)
                self.conn.commit()
                logger.info(f'提交成功，sql:{sql}')
            else:
                self.cursor.execute(sql)
                # 提交
                self.conn.commit()
                logger.info(f'提交成功，sql:{sql}')
        except Exception as e:
            logger.info(f'提交失败:{e}')
            # 提交失败需要回滚
            self.conn.rollback()

    # 获取dataframe结构的查询
    def select_data_by_dataframe(self, sql):
        username = self.config['user']
        pwd = self.config['passwd']
        ip = self.config['host']
        port = self.config['port']
        database = self.config['database']
        charset = self.config['charset']

        engine = create_engine(f'mysql+pymysql://{username}:{pwd}@{ip}:{port}/{database}?charset={charset}')
        df = pd.read_sql(sql=sql, con=engine)
        if df is not None:
            return df
        else:
            return None

    # 当对象被销毁时，游标要关闭,连接也要关闭
    # 创建时是先创建连接后创建游标，关闭时是先关闭游标后关闭连接

    def __del__(self):
        self.cursor.close()
        self.conn.close()
