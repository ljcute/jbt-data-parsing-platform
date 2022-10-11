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
from utils.logs_utils import logger


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

    data6 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data7 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data8 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '上海交易所',
             'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data9 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '深圳交易所',
             'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data10 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data11 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '4', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data12 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '5', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data13 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data14 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data15 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data16 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '4', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data17 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '5', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data18 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data19 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data20 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data21 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '4', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data22 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '5', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data23 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '上海交易所',
              'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data24 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data25 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '3', 'data_source': '深圳交易所',
              'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data181 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data182 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data183 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data184 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data185 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data26 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data27 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data28 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data29 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data30 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}

    data31 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data32 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data33 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data34 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data35 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data36 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data37 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data38 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data39 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}
    data40 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '3', 'data_source': '中信证券',
              'message': 'zx_securities_collect'}

    data41 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data42 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data43 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data44 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data45 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data46 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data47 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '4', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data48 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '5', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data49 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data50 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '4', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data51 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '5', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data52 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data53 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '4', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data54 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '5', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data55 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '国泰君安',
              'message': 'gtja_securities_collect'}
    data186 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data187 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data188 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}

    data56 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data57 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data58 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data59 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data60 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data61 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data62 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data63 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '4', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data64 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '5', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data65 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data66 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '4', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data67 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '5', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data68 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data69 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '4', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data70 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '5', 'data_source': '招商证券',
              'message': 'zs_securities_collect'}
    data189 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data190 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data191 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}

    data71 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data72 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data73 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data74 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data75 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data76 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data77 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data78 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '4', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data79 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '5', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data80 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data81 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '4', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data82 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '5', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data83 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data84 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '4', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}
    data85 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '5', 'data_source': '国信证券',
              'message': 'gx_securities_collect'}

    data192 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data193 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data194 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}

    data86 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data87 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data88 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data89 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data90 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data91 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data92 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data93 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '4', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data94 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '5', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data95 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data96 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '4', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data97 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '5', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data98 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data99 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '4', 'data_source': '国元证券',
              'message': 'gy_securities_collect'}
    data100 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '5', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data195 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data196 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data197 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}

    data101 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data102 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data103 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data104 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data105 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data106 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data107 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data108 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data109 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data110 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}

    data111 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data112 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data113 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data114 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data115 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data116 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data117 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data118 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data119 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data120 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data121 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data122 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data123 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data124 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data125 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data198 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data199 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data200 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}

    data126 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data127 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data128 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data129 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data130 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data131 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data132 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data133 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data134 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data135 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data136 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data137 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data138 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data139 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data140 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data201 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data202 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data203 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}

    data141 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data142 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data143 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data144 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data145 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data146 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data147 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data148 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data149 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data150 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data204 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data205 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}

    data151 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data152 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data153 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data154 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data155 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data156 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data157 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data158 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data159 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data160 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '3', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data206 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data207 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '3', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}

    data161 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data162 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data163 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data164 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data165 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data166 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data167 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data168 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data169 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data170 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data208 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data209 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}

    data171 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data172 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data173 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data174 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data175 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data176 = {'user_id': 1, 'biz_dt': '2022-09-28', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data177 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data178 = {'user_id': 1, 'biz_dt': '2022-09-29', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data179 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data180 = {'user_id': 1, 'biz_dt': '2022-09-30', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data210 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data211 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}

    # data26, data27, data28, data29, data30,
    # data31, data32, data33, data34, data35, data36, data37, data38, data39, data40,
    # data101, data102, data103, data104, data105, data106, data107, data108, data109, data110,

    # list = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10,
    #         data11, data12, data13, data14, data15, data16, data17, data18, data19, data20,
    #         data21, data22, data23, data24, data25, data181, data182, data183, data184, data185,
    #         data41, data42, data43, data44, data45, data46, data47, data48, data49, data50,
    #         data51, data52, data53, data54, data55, data186, data187, data188,
    #         data56, data57, data58, data59, data60, data61, data62, data63, data64, data65,
    #         data66, data67, data68, data69, data70, data189, data190, data191,
    #         data71, data72, data73, data74, data75, data76, data77, data78, data79, data80,
    #         data81, data82, data83, data84, data85, data192, data193, data194,
    #         data86, data87, data88, data89, data90, data91, data92, data93, data94, data95,
    #         data96, data97, data98, data99, data100, data195, data196, data197,
    #         data111, data112, data113, data114, data115, data116, data117, data118, data119, data120,
    #         data121, data122, data123, data124, data125, data198, data199, data200,
    #         data126, data127, data128, data129, data130, data131, data132, data133, data134, data135,
    #         data136, data137, data138, data139, data140, data201, data202, data203,
    #         data141, data142, data143, data144, data145, data146, data147, data148, data149, data150,
    #         data204, data205, data151, data152, data153, data154, data155, data156, data157, data158,
    #         data159, data160, data206, data207, data161, data162, data163, data164, data165, data166,
    #         data167, data168, data169, data170, data208, data209]

    list = [data171, data172, data173, data174, data175, data176, data177, data178, data179, data180, data210, data211]

    logger.info(f'手工补录解析数据开始！')
    try:
        for i in list:
            TempHandler.parsing_data_job(i)
            time.sleep(5)
    except Exception as e:
        pass
        time.sleep(2)
    logger.info(f'手工补录解析数据结束！')
