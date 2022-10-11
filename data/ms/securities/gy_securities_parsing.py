#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/24 11:19
# @Site    : 
# @File    : gy_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def gy_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    error_list = []
    if rs[2] == '2':
        logger.info(f'国元证券可充抵保证金证券解析开始...')
        for data in data_:
            if data['stkex'] == '1':
                market = '上海'
                suffix = '.SH'
            elif data['stkex'] == '0':
                market = '深圳'
                suffix = '.SZ'
            else:
                market = '北京'
                suffix = '.BJ'
            sec_code = data['secu_code']
            if str(sec_code).endswith('SZ') or str(sec_code).endswith('SH') or str(sec_code).endswith('BJ'):
                sec_code = sec_code
            else:
                sec_code = str(sec_code).split('.')[0]
            sec_name = data['secu_name']
            rate = rate_is_normal_one(data['exchange_rate'])
            bzj_data.append([market, sec_code, sec_name, rate])
        securities_bzj_parsing_data(rs, 3, bzj_data)
        logger.info(f'国元证券可充抵保证金证券解析结束...')

    elif rs[2] == '4':
        logger.info(f'国元证券融资标的证券解析开始...')
        for data in data_:
            sec_code = data['secu_code']
            if str(sec_code).endswith('SZ') or str(sec_code).endswith('SH') or str(sec_code).endswith('BJ'):
                sec_code = sec_code
            else:
                sec_code = str(sec_code).split('.')[0]
            sec_name = data['secu_name']
            rate = rate_is_normal_two(data['rz_ratio'])
            rz_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rz_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, 1, temp_data)
        logger.info(f'国元证券融资标的证券解析结束...')


    elif rs[2] == '5':
        logger.info(f'国元证券融券标的证券解析开始...')
        for data in data_:
            sec_code = data['secu_code']
            if str(sec_code).endswith('SZ') or str(sec_code).endswith('SH') or str(sec_code).endswith('BJ'):
                sec_code = sec_code
            else:
                sec_code = str(sec_code).split('.')[0]
            sec_name = data['secu_name']
            # rate = round(float(str(data[2]).strip('%')), 3)
            rate = rate_is_normal_two(data['rq_ratio'])
            rq_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rq_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, 2, temp_data)
        logger.info(f'国元证券融券标的证券解析结束...')
