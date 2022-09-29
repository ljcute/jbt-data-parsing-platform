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

    data1 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data2 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data3 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}

    data4 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '深圳交易所',
             'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data5 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '深圳交易所',
             'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data6 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '99', 'data_source': '中信建投',
             'message': 'zxjt_securities_collect'}
    data7 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '中信证券',
             'message': 'zx_securities_collect'}
    data8 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '中信证券',
             'message': 'zx_securities_collect'}
    data9 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '国泰君安',
             'message': 'gtja_securities_collect'}
    data10 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data11 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}

    data12 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data13 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}

    data14 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data15 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data16 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}

    data17 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data18 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data19 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}

    data20 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data21 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data22 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}

    data23 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '兴业证券',
              'message': 'xy_securities_collect'}
    data24 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '兴业证券',
              'message': 'xy_securities_collect'}

    data25 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}
    data26 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}
    data27 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}

    data28 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}
    data29 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}
    data30 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}

    data31 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '安信证券',
              'message': 'ax_securities_collect'}
    data32 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '安信证券',
              'message': 'ax_securities_collect'}

    data33 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '申万宏源',
              'message': 'sw_securities_collect'}
    data34 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '申万宏源',
              'message': 'sw_securities_collect'}

    data35 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '中金财富',
              'message': 'zjcf_securities_collect'}
    data36 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '中金财富',
              'message': 'zjcf_securities_collect'}

    data37 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data38 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data39 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}

    data40 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data41 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data42 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data43 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data44 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data45 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data46 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data47 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}

    data48 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data49 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}

    data50 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data51 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data52 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}

    data53 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data54 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data55 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}

    data56 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data57 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data58 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}

    data59 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '兴业证券',
              'message': 'xy_securities_collect'}
    data60 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '兴业证券',
              'message': 'xy_securities_collect'}

    data61 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}
    data62 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}
    data63 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '广发证券',
              'message': 'gf_securities_collect'}

    data64 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}
    data65 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}
    data66 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '中国银河',
              'message': 'yh_securities_collect'}

    data67 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '安信证券',
              'message': 'ax_securities_collect'}
    data68 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '安信证券',
              'message': 'ax_securities_collect'}

    data69 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '申万宏源',
              'message': 'sw_securities_collect'}
    data70 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '申万宏源',
              'message': 'sw_securities_collect'}

    data71 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中金财富',
              'message': 'zjcf_securities_collect'}
    data72 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '中金财富',
              'message': 'zjcf_securities_collect'}
    data73 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data74 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data75 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data76 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data77 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data78 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '华泰证券',
              'message': 'ht_securities_collect'}
    data79 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data80 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data81 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data82 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data83 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data84 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data85 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data86 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data87 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    # list = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10, data11, data12, data13,
    #         data14, data15, data16, data17, data18, data19, data20, data21, data22, data23, data24, data25, data26,
    #         data27, data28, data29, data30, data31, data32, data33, data34, data35, data36, data37, data38, data39,
    #         data40, data41, data42, data43, data44, data45, data46, data47, data48, data49, data50, data51,
    #         data52, data53, data54, data55, data56, data57, data58, data59, data60, data61, data62, data63, data64,
    #         data65, data66, data67, data68, data69, data70, data71, data72]
    # list = [data6, data33, data34, data42, data69, data70, data73, data74, data23, data24, data59,
    #         data60, data71, data72, data35, data36, data48, data49]

    list = [data73, data74, data75, data76, data77, data78, data79, data80, data81, data82, data83, data84, data85,
            data86, data87]
    print(f'手工补录解析数据开始！')
    try:
        for i in list:
            print(f'手工补录---解析数据结束！')
            TempHandler.parsing_data_job(i)
            time.sleep(5)
    except Exception as e:
        pass
        time.sleep(2)
    print(f'手工补录解析数据结束！')
