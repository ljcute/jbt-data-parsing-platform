#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/28 15:13
# @Site    : 
# @File    : raw_data_deal.py
# @Software: PyCharm
import datetime
from configparser import ConfigParser
import os

from db_connection import Mysqldb

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')

cf = ConfigParser()
cf.read(full_path, encoding='utf-8')

config = {
    "host": cf.get('mysql-raw', 'host'),
    "port": cf.getint('mysql-raw', 'port'),
    "database": cf.get('mysql-raw', 'schema'),
    "charset": cf.get('mysql-raw', 'charset'),
    "user": cf.get('mysql-raw', 'username'),
    "passwd": cf.get('mysql-raw', 'password')
}

# 实例化
db = Mysqldb(config)


def select_sh_mt_trading_total_data():
    sql = f'select log_id, biz_dt, data_type, data_source, data_text from t_ndc_data_collect_log ' \
          f'where data_type = 0 and data_source = "上海交易所" and data_status = 1 order by update_dt desc'
    result = db.select_one(sql)
    # result = db.select_data_by_dataframe(sql)
    return result


def select_collected_data(biz_dt, data_type, data_source):
    sql = f'select log_id, biz_dt, data_type, data_source, data_text from t_ndc_data_collect_log ' \
          f'where data_type = {data_type} and data_source = "{data_source}" and biz_dt = {biz_dt} ' \
          f'and data_status = 1 order by update_dt desc'
    result = db.select_all(sql)
    # result = db.select_data_by_dataframe(sql)
    return result


def select_sh_mt_trading_items_data():
    sql = f'select log_id, biz_dt, data_type,data_source, data_text from t_ndc_data_collect_log ' \
          f'where data_type = 1 and data_source = "上海交易所" and data_status = 1 order by update_dt desc'
    result = db.select_one(sql)
    # result = db.select_data_by_dataframe(sql)
    return result


if __name__ == '__main__':
    pass
