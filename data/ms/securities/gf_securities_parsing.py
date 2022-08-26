#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/23 15:33
# @Site    : 
# @File    : gf_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def gf_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    error_list = []
    if rs[2] == '2':
        logger.info(f'广发证券可充抵保证金证券解析开始...')
        for data in data_:
            sec_code = data[1]
            sec_name = data[0]
            rate = round(float(str(data[2]).strip('%')), 3)
            bzj_data.append([sec_code, sec_name, rate])
        securities_bzj_parsing_data_no_market(rs, bzj_data)
        logger.info(f'广发证券可充抵保证金证券解析结束...')

    elif rs[2] == '4':
        logger.info(f'广发证券融资标的证券解析开始...')
        for data in data_:
            sec_code = data[1]
            sec_name = data[0]
            rate = round(float(str(data[2]).strip('%')), 3)
            rz_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rz_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, rs[2], rz_data)
        logger.info(f'广发证券融资标的证券解析结束...')

    elif rs[2] == '5':
        logger.info(f'广发证券融券标的证券解析开始...')
        for data in data_:
            sec_code = data[1]
            sec_name = data[0]
            rate = round(float(str(data[2]).strip('%')), 3)
            rq_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rq_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, rs[2], rq_data)
        logger.info(f'广发证券融券标的证券解析结束...')

