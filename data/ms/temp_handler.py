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
import traceback

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from data.ms.basehandler import BaseHandler
from util.logs_utils import logger


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

    data400 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data401 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data402 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data403 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data404 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data405 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data406 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data407 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data408 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data409 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data410 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data411 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data412 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data413 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data414 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data415 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data416 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data417 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data418 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data419 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data520 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data521 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data522 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data523 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data524 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data600 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data601 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data602 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data603 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data604 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data605 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data606 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data607 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data608 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data609 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data610 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data611 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data612 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data613 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data614 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data710 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data711 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data712 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data713 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data714 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data715 = {'user_id': 1, 'biz_dt': '2022-10-24', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data716 = {'user_id': 1, 'biz_dt': '2022-10-24', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data717 = {'user_id': 1, 'biz_dt': '2022-10-24', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data718 = {'user_id': 1, 'biz_dt': '2022-10-24', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data719 = {'user_id': 1, 'biz_dt': '2022-10-24', 'data_type': '3', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}

    data720 = {'user_id': 1, 'biz_dt': '2022-10-25', 'data_type': '4', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data721 = {'user_id': 1, 'biz_dt': '2022-10-25', 'data_type': '5', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data722 = {'user_id': 1, 'biz_dt': '2022-10-25', 'data_type': '2', 'data_source': '上海交易所',
               'message': 'sh_exchange_mt_underlying_and_guaranty_security'}
    data723 = {'user_id': 1, 'biz_dt': '2022-10-25', 'data_type': '2', 'data_source': '深圳交易所',
               'message': 'sz_exchange_mt_underlying_and_guaranty_security'}
    data724 = {'user_id': 1, 'biz_dt': '2022-10-25', 'data_type': '3', 'data_source': '深圳交易所',
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
    data31 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data32 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data33 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data34 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '99', 'data_source': '中信建投',
              'message': 'zxjt_securities_collect'}
    data420 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '99', 'data_source': '中信建投',
               'message': 'zxjt_securities_collect'}

    data421 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '2', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data422 = {'user_id': 1, 'biz_dt': '2022-09-26', 'data_type': '3', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data423 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '2', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data424 = {'user_id': 1, 'biz_dt': '2022-09-27', 'data_type': '3', 'data_source': '中信证券',
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
    data425 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data426 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '3', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data427 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data428 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '3', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data429 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data430 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '3', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data431 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data432 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '3', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data433 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '中信证券',
               'message': 'zx_securities_collect'}
    data434 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '3', 'data_source': '中信证券',
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
    data446 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '4', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data435 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '5', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data436 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data437 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '4', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data438 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '5', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data439 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data440 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '4', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data441 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '5', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data442 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data443 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '4', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data444 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '5', 'data_source': '国泰君安',
               'message': 'gtja_securities_collect'}
    data445 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '国泰君安',
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
    data300 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data301 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '4', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data302 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '5', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}

    data455 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data447 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '4', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data448 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '5', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data449 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data450 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '4', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data451 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '5', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data452 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data453 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '4', 'data_source': '招商证券',
               'message': 'zs_securities_collect'}
    data454 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '5', 'data_source': '招商证券',
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
    data456 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data457 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '4', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data458 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '5', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data459 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data460 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '4', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data461 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '5', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data462 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data463 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '4', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data464 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '5', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data465 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data466 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '4', 'data_source': '国信证券',
               'message': 'gx_securities_collect'}
    data467 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '5', 'data_source': '国信证券',
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
    data303 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data304 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '4', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data305 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '5', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data306 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data307 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '4', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data308 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '5', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data309 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data310 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '4', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data311 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '5', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data468 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data469 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '4', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}
    data470 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '5', 'data_source': '国元证券',
               'message': 'gy_securities_collect'}

    data101 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data102 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data103 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data104 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data105 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data106 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data471 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data472 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data525 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data526 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '2', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data527 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '3', 'data_source': '兴业证券',
               'message': 'xy_securities_collect'}
    data528 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '2', 'data_source': '兴业证券',
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
    data201 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data202 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data203 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data204 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data205 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data206 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data207 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data208 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data209 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '4', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data473 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '5', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data474 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '广发证券',
               'message': 'gf_securities_collect'}
    data475 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '4', 'data_source': '广发证券',
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
    data476 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data477 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data478 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data479 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data480 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data481 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data482 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data483 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data484 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data485 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data486 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data487 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data489 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data490 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data491 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data529 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data530 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data531 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '4', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data532 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '5', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data533 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '2', 'data_source': '中国银河',
               'message': 'yh_securities_collect'}
    data534 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '4', 'data_source': '中国银河',
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
    data492 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data493 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data494 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data495 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data496 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data497 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data498 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data499 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data500 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '3', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}
    data501 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '安信证券',
               'message': 'ax_securities_collect'}

    data151 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data152 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '3', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data153 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '申万宏源',
               'message': 'sw_securities_collect'}
    data154 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '3', 'data_source': '申万宏源',
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
    data502 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data503 = {'user_id': 1, 'biz_dt': '2022-10-10', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data504 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data505 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data506 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data507 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data508 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data509 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data510 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '2', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data511 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}

    data800 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data801 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data802 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data803 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data804 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}

    data805 = {'user_id': 1, 'biz_dt': '2022-10-24', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data806 = {'user_id': 1, 'biz_dt': '2022-10-25', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data807 = {'user_id': 1, 'biz_dt': '2022-10-26', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data808 = {'user_id': 1, 'biz_dt': '2022-10-27', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data809 = {'user_id': 1, 'biz_dt': '2022-10-28', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}

    data810 = {'user_id': 1, 'biz_dt': '2022-10-31', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data811 = {'user_id': 1, 'biz_dt': '2022-11-01', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data812 = {'user_id': 1, 'biz_dt': '2022-11-02', 'data_type': '3', 'data_source': '中金财富',
               'message': 'zjcf_securities_collect'}
    data813 = {'user_id': 1, 'biz_dt': '2022-11-03', 'data_type': '3', 'data_source': '中金财富',
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
    data512 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data513 = {'user_id': 1, 'biz_dt': '2022-10-11', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data514 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data515 = {'user_id': 1, 'biz_dt': '2022-10-12', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data516 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data517 = {'user_id': 1, 'biz_dt': '2022-10-13', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data518 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data519 = {'user_id': 1, 'biz_dt': '2022-10-14', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data700 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data701 = {'user_id': 1, 'biz_dt': '2022-10-17', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data702 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data703 = {'user_id': 1, 'biz_dt': '2022-10-18', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data704 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data705 = {'user_id': 1, 'biz_dt': '2022-10-19', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data706 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data707 = {'user_id': 1, 'biz_dt': '2022-10-20', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data708 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': 2, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data709 = {'user_id': 1, 'biz_dt': '2022-10-21', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data820 = {'user_id': 1, 'biz_dt': '2022-10-24', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data821 = {'user_id': 1, 'biz_dt': '2022-10-25', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data822 = {'user_id': 1, 'biz_dt': '2022-10-26', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data823 = {'user_id': 1, 'biz_dt': '2022-10-27', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data824 = {'user_id': 1, 'biz_dt': '2022-10-28', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data825 = {'user_id': 1, 'biz_dt': '2022-10-31', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data826 = {'user_id': 1, 'biz_dt': '2022-11-01', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data827 = {'user_id': 1, 'biz_dt': '2022-11-02', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data828 = {'user_id': 1, 'biz_dt': '2022-11-03', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}
    data829 = {'user_id': 1, 'biz_dt': '2022-11-04', 'data_type': 3, 'data_source': '华泰证券',
               'message': 'ht_securities_collect'}

    # list = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10,
    #         data11, data12, data13, data14, data15, data16, data17, data18, data19, data20,
    #         data21, data22, data23, data24, data25, data181, data182, data183, data184, data185,
    #         data400, data401, data402, data403, data404, data405, data406, data407, data408, data409,
    #         data410, data411, data412, data413, data414, data415, data416, data417, data418, data419,
    #         data26, data27, data28, data29, data30, data31, data32, data33, data34, data420,
    #         data421, data422, data423, data424, data35, data36, data37, data38, data39, data40,
    #         data425, data426, data427, data428, data429, data430, data431, data432, data433, data434,
    #         data41, data42, data43, data44, data45, data46, data47, data48, data49, data50,
    #         data51, data52, data53, data54, data55, data186, data187, data188, data446, data435,
    #         data436, data437, data438, data439, data440, data441, data442, data443, data444, data445,
    #         data56, data57, data58, data59, data60, data61, data62, data63, data64, data65,
    #         data66, data67, data68, data69, data70, data189, data190, data191, data300, data301, data302,
    #         data455, data447, data448, data449, data450, data451, data452, data453, data454,
    #         data71, data72, data73, data74, data75, data76, data77, data78, data79, data80,
    #         data81, data82, data83, data84, data85, data192, data193, data194,
    #         data456, data457, data458, data459, data460, data461, data462, data463, data464, data465, data466, data467,
    #         data86, data87, data88, data89, data90, data91, data92, data93, data94, data95,
    #         data96, data97, data98, data99, data100, data195, data196, data197,
    #         data303, data304, data305, data306, data307, data308, data309, data310, data311, data468, data469, data470,
    #         data101, data102, data103, data104, data105, data106, data471, data472,
    #         data111, data112, data113, data114, data115, data116, data117, data118, data119, data120,
    #         data121, data122, data123, data124, data125, data198, data199, data200,
    #         data201, data202, data203, data204, data205, data206, data207, data208, data209, data473, data474, data475,
    #         data126, data127, data128, data129, data130, data131, data132, data133, data134, data135,
    #         data136, data137, data138, data139, data140, data476, data477, data478, data479, data480, data481, data482,
    #         data483, data484, data485, data486, data487, data489, data490, data491,
    #         data141, data142, data143, data144, data145, data146, data147, data148, data149, data150,
    #         data492, data493, data494, data495, data496, data497, data498, data499, data500, data501,
    #         data151, data152, data153, data154, data161, data162, data163, data164, data165, data166,
    #         data167, data168, data169, data170, data502, data503, data504, data505, data506, data507, data508,
    #         data509, data510, data511, data171, data172, data173, data174, data175, data176, data177, data178, data179,
    #         data180, data210, data211, data512, data513, data514, data515, data516, data517, data518, data519]

    # list = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10,
    #         data11, data12, data13, data14, data15, data16, data17, data18, data19, data20,
    #         data21, data22, data23, data24, data25, data181, data182, data183, data184, data185,
    #         data400, data401, data402, data403, data404, data405, data406, data407, data408, data409,
    #         data410, data411, data412, data413, data414, data415, data416, data417, data418, data419,
    #         data520, data521, data522, data523, data524,data600,data601,data602,data603,data604,data605,
    #         data606,data607,data608,data609,data610,data611,data612,data613,data614,data710,data711,data712,
    #         data713,data714,data715,data716,data717,data718,data719]

    # list = [data101, data102, data103, data104, data105, data106, data471, data472,data525,data526,data527,data528,
    #         data126, data127, data128, data129, data130, data131, data132, data133, data134, data135,
    #         data136, data137, data138, data139, data140, data476, data477, data478, data479, data480, data481, data482,
    #         data483, data484, data485, data486, data487, data489, data490, data491,data529,data530,data531,
    #         data532,data533,data534]

    # list = [data171, data172, data173, data174, data175, data176, data177, data178, data179,
    #         data180, data210, data211, data512, data513, data514, data515, data516, data517, data518, data519,
    #         data700, data701, data702, data703, data704, data705, data706, data707, data708, data709]

    # list = [data3, data4, data8, data9, data13, data14, data18, data19, data23, data24, data183, data184, data402,
    #         data403,data407, data408, data412, data413, data417, data418, data522, data523, data602, data603,
    #         data607, data608, data612, data613, data712, data713, data717, data718, data722, data723]

    s_list = [data172, data174, data176, data178, data180, data211, data513, data515, data517, data519, data701,
            data703, data705, data707, data709, data820, data821, data822, data823, data824, data825, data826,
            data827, data828, data829]

    logger.info(f'手工补录解析数据开始！')
    try:
        for i in s_list:
            ds = i['data_source']
            dt = i['data_type']
            dt_ = i['biz_dt']
            logger.info(f'开始解析：{ds}，类型为：{dt}的数据，业务日期为：{dt_}')
            TempHandler.parsing_data_job(i)
            time.sleep(5)
    except Exception as e:
        logger.info(f'具体异常为：{traceback.format_exc()}，e:{e}')
        time.sleep(2)
    logger.info(f'手工补录解析数据结束！')
