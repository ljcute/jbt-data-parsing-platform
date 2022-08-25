#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/22 10:03
# @Site    : 
# @File    : cc_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def cc_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    error_list = []
    if rs[2] == '2':
        logger.info(f'长城证券可充抵保证金证券解析开始...')
        for data in data_:
            sec_code = data[0]
            sec_name = data[1]
            rate = data[2]
            bzj_data.append([sec_code, sec_name, rate])
        securities_bzj_parsing_data_no_market(rs, bzj_data)
        logger.info(f'长城证券可充抵保证金证券解析结束...')

    elif rs[2] == '4':
        logger.info(f'长城证券融资标的证券解析开始...')
        for data in data_:
            sec_code = data[0]
            sec_name = data[1]
            rate = data[2]
            rz_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rz_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, rs[2], rz_data)
        logger.info(f'长城证券融资标的证券解析结束...')

    elif rs[2] == '5':
        logger.info(f'长城证券融券标的证券解析开始...')
        for data in data_:
            sec_code = data[0]
            sec_name = data[1]
            rate = data[2]
            rq_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rq_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, rs[2], rq_data)
        logger.info(f'长城证券融券标的证券解析结束...')






