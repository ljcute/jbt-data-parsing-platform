#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/28 15:13
# @Site    : 
# @File    : biz_data_deal.py
# @Software: PyCharm
import datetime
import time
from configparser import ConfigParser
from utils.snowflake_utils import get_id
import os

from db_connection import Mysqldb

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')

cf = ConfigParser()
cf.read(full_path, encoding='utf-8')

config = {
    "host": cf.get('mysql-biz', 'host'),
    "port": cf.getint('mysql-biz', 'port'),
    "database": cf.get('mysql-biz', 'schema'),
    "charset": cf.get('mysql-biz', 'charset'),
    "user": cf.get('mysql-biz', 'username'),
    "passwd": cf.get('mysql-biz', 'password')
}

# 实例化
db = Mysqldb(config)


def insert_data(a, b, c, d, e, f):
    value = (a, b, c, d, e, f, str(datetime.datetime.now()), str(datetime.datetime.now()))
    sql = f'insert into t_security_broker values {value}'
    db.commit_data(sql)
    # sql = 'select * from t_security_broker'
    # pd = db.select_data_by_dataframe(sql)
    # print(pd)


def insert_data_process_controler(biz_id, data_processor, data_type, data_source, data_status, last_process_bizdt,
                                  last_process_status, last_process_result):
    ctl_id = get_id()
    value = (ctl_id, biz_id, data_processor, data_type, data_source, str(datetime.datetime.now()),
             str(datetime.datetime.now()), data_status, str(last_process_bizdt), last_process_status,
             last_process_result, str(datetime.datetime.now()), str(datetime.datetime.now()))

    sql = f'insert into t_data_process_controller values {value}'
    db.commit_data(sql)


def insert_data_process_log(biz_id, data_processor, data_type, data_source, record_num, data_status, last_process_bizdt,
                            last_process_status, last_process_result):
    ctl_id = get_id()
    value = (ctl_id, biz_id, data_processor, data_type, data_source, str(datetime.datetime.now()),
             str(datetime.datetime.now()), 0, record_num, data_status, str(last_process_bizdt), last_process_status,
             last_process_result, str(datetime.datetime.now()), str(datetime.datetime.now()))
    sql = f'insert into t_data_process_log values {value}'
    db.commit_data(sql)


def insert_exchange_mt_transactions_total(biz_dt, exchange_market, financing_balance, financing_purchase_amount,
                                          lending_securities_volume,
                                          lending_securities_amount, lending_securities_sales_volume,
                                          margin_trading_balance, data_status,
                                          creator_id, updater_id):
    row_id = get_id()
    value = (
        row_id, str(biz_dt), exchange_market, financing_balance, financing_purchase_amount, lending_securities_volume,
        lending_securities_amount, lending_securities_sales_volume, margin_trading_balance, data_status,
        creator_id, str(datetime.datetime.now()), updater_id, str(datetime.datetime.now()))
    sql = f'insert into t_exchange_mt_transactions_total values {value}'
    db.commit_data(sql)


def insert_exchange_mt_transactions_items(data_list):
    data_list_ = []
    for i in range(0, len(data_list)):
        row_id = get_id()
        data_list[i].insert(0, row_id + 1)
        data_list[i].insert(-1, str(datetime.datetime.now()))
        data_list[i].append(str(datetime.datetime.now()))
        data_list_.append(data_list[i])
        time.sleep(0.001)

    if data_list_:
        print(f'data_list_:{data_list_}')

    sql = "insert into t_exchange_mt_transactions_items(row_id,secu_id,secu_code,secu_name,biz_dt,financing_balance," \
          "financing_purchase_amount,lending_securities_volume,lending_securities_amount,lending_securities_sales_volume" \
          ",margin_trading_balance,data_status,creator_id,create_dt,updater_id,update_dt)" \
          " values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    db.commit_data(sql, data_list)


# def insert_exchange_mt_transactions_items(secu_id, secu_code, secu_name, biz_dt, financing_balance,
#                                           financing_purchase_amount, lending_securities_volume,
#                                           lending_securities_amount, lending_securities_sales_volume,
#                                           margin_trading_balance, data_status, creator_id, updater_id):
#     row_id = get_id()
#     value = (row_id, secu_id, secu_code, secu_name, str(biz_dt), financing_balance, financing_purchase_amount,
#              lending_securities_volume, lending_securities_amount, lending_securities_sales_volume,
#              margin_trading_balance, data_status, creator_id, str(datetime.datetime.now())
#              , updater_id, str(datetime.datetime.now()))
#     sql = f'insert into t_exchange_mt_transactions_items values {value}'
#     db.commit_data(sql)


if __name__ == '__main__':
    a = 6
    b = 'ame'
    c = 'ame'
    d = 1
    e = 'ame'
    f = '1'
    insert_data(a, b, c, d, e, f)
