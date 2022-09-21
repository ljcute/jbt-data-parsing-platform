#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/21 14:59
# @Site    : 
# @File    : temp_handler.py
# @Software: PyCharm
import os
import sys
import time
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler


class TempHandler(BaseHandler):
    pass


if __name__ == '__main__':
    data0 = {'user_id': 960529, 'biz_dt': '2022-09-21', 'data_type': '2', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data1 = {'user_id': 960529, 'biz_dt': '2022-09-21', 'data_type': '4', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data2 = {'user_id': 960529, 'biz_dt': '2022-09-21', 'data_type': '5', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data3 = {'user_id': 960529, 'biz_dt': '2022-09-21', 'data_type': '2', 'data_source': '深圳交易所',
             'message': 'sz_exchange_mt_guaranty_security_collect'}
    data4 = {'user_id': 960529, 'biz_dt': '2022-09-21', 'data_type': '3', 'data_source': '深圳交易所',
             'message': 'sz_exchange_mt_guaranty_security_collect'}
    list = [data0, data1, data2, data3, data4]
    for i in list:
        TempHandler.parsing_data_job(i)
        time.sleep(10)
