#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/24 13:27
# @Site    : 
# @File    : zxjt_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def zxjt_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    error_list = []
    if rs[2] == '99':
        for data in data_:
            market = data[2]
            sec_code = data[1]
            sec_name = data[3]
            bzj_rate = None if data[4] == '-' else rate_is_normal_one(data[4])
            rz_rate = None if data[5] == '-' else rate_is_normal_two(data[5])
            rq_rate = None if data[6] == '-' else rate_is_normal_two(data[6])

            bzj_data.append([market, sec_code, sec_name, bzj_rate])
            rz_data.append([sec_code, sec_name, rz_rate])
            rq_data.append([sec_code, sec_name, rq_rate])

        logger.info(f'中信建投证券可充抵保证金证券解析开始...')
        securities_bzj_parsing_data(rs, 3, bzj_data)
        logger.info(f'中信建投证券可充抵保证金证券解析开始...')

        time.sleep(5)

        logger.info(f'中信建投证券融资标的证券解析开始...')
        temp_data = securities_normal_parsing_data_no_market(rz_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)
        securities_rzrq_parsing_data(rs, 1, temp_data)
        logger.info(f'中信建投证券融资标的证券解析结束...')

        time.sleep(5)

        logger.info(f'中信建投证券融券标的证券解析开始...')
        temp_data_ = securities_normal_parsing_data_no_market(rq_data)
        for temp_ in temp_data_:
            if len(temp_) == 3:
                logger.error(f'该条记录无证券id{temp_},需人工修复!')
                error_list.append(temp_)

        securities_rzrq_parsing_data(rs, 2, temp_data)
        logger.info(f'中信建投证券融券标的证券解析结束...')






