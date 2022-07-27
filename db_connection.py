#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 2022/6/23 13:55
# pip3 install DBUtils==1.3

import pymysql
from dbutils.pooled_db import PooledDB
from configparser import ConfigParser
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, 'config/config.ini')

cf = ConfigParser()
cf.read(full_path, encoding='utf-8')

host_biz = cf.get('mysql-biz', 'host')
port_biz = cf.getint('mysql-biz', 'port')
username_biz = cf.get('mysql-biz', 'username')
password_biz = cf.get('mysql-biz', 'password')
schema_biz = cf.get('mysql-biz', 'schema')
charset_biz = cf.get('mysql-biz', 'charset')

global_pool_biz = PooledDB(
    creator=pymysql,
    maxconnections=200,
    mincached=4,
    maxcached=20,
    maxshared=0,
    blocking=True,
    setsession=[],
    ping=5,
    host=host_biz,
    port=port_biz,
    user=username_biz,
    password=password_biz,
    database=schema_biz,
    charset=charset_biz)

host_raw = cf.get('mysql-raw', 'host')
port_raw = cf.getint('mysql-raw', 'port')
username_raw = cf.get('mysql-raw', 'username')
password_raw = cf.get('mysql-raw', 'password')
schema_raw = cf.get('mysql-raw', 'schema')
charset_raw = cf.get('mysql-raw', 'charset')

global_pool_raw = PooledDB(
    creator=pymysql,
    maxconnections=200,
    mincached=4,
    maxcached=20,
    maxshared=0,
    blocking=True,
    setsession=[],
    ping=5,
    host=host_raw,
    port=port_raw,
    user=username_raw,
    password=password_raw,
    database=schema_raw,
    charset=charset_raw)

if __name__ == '__main__':
    conn = global_pool_raw.connection()
    cursor = conn.cursor()
    print(cursor)
