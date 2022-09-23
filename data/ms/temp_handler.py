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
    data0 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '99', 'data_source': '中信建投',
             'message': 'zxjt_securities_collect'}
    data1 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '中信证券',
             'message': 'zx_securities_collect'}

    data2 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '4', 'data_source': '国泰君安',
             'message': 'gtja_securities_collect'}
    data3 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '5', 'data_source': '国泰君安',
             'message': 'gtja_securities_collect'}
    data4 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '国泰君安',
             'message': 'gtja_securities_collect'}

    data5 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '华泰证券',
             'message': 'ht_securities_collect'}
    data6 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '3', 'data_source': '华泰证券',
             'message': 'ht_securities_collect'}

    data7 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '招商证券',
             'message': 'zs_securities_collect'}
    data8 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '4', 'data_source': '招商证券',
             'message': 'zs_securities_collect'}
    data9 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '5', 'data_source': '招商证券',
             'message': 'zs_securities_collect'}

    data10 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '兴业证券',
              'message': 'xy_securities_collect'}
    data11 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '3', 'data_source': '兴业证券',
              'message': 'xy_securities_collect'}

    data12 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '5', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}
    data13 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}
    data14 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '4', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}

    data15 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '5', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}
    data16 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}
    data17 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '4', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}

    data18 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '申万宏源',
              'message': 'sw_securities_collect'}
    data19 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '3', 'data_source': '申万宏源',
              'message': 'sw_securities_collect'}

    data20 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '中金财富',
              'message': 'zjcf_securities_collect'}
    data21 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '3', 'data_source': '中金财富',
              'message': 'zjcf_securities_collect'}

    data22 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    # data23 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '4', 'data_source': '上海交易所',
    #          'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    # data24 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '5', 'data_source': '上海交易所',
    #          'message': 'sh_exchange_mt_underlying_and_guaranty_security'}

    data25 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '2', 'data_source': '深圳交易所',
             'message': 'sz_exchange_mt_guaranty_security_collect'}
    # data26 = {'user_id': 1, 'biz_dt': '2022-09-23', 'data_type': '3', 'data_source': '深圳交易所',
    #          'message': 'sz_exchange_mt_guaranty_security_collect'}

    list = [data0, data1, data2, data3, data4, data5, data6, data7, data8, data9, data10, data11, data12, data13,
            data14, data15, data16, data17, data18, data19, data20, data21, data22, data25]
    for i in list:
        TempHandler.parsing_data_job(i)
        time.sleep(5)
