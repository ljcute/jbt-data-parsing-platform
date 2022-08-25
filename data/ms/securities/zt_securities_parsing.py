#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/23 17:51
# @Site    : 
# @File    : zt_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def zt_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    rzrq_data = []
    if rs[2] == '2':
        logger.info(f'中泰证券可充抵保证金证券解析开始...')
        for data in data_:
            sec_code = data[0]
            sec_name = data[1]
            rate = data[2]
            bzj_data.append([sec_code, sec_name, rate])
        securities_bzj_parsing_data_no_market(rs, bzj_data)
        logger.info(f'中泰证券可充抵保证金证券解析结束...')

    elif rs[2] == '3':
        logger.info(f'中泰证券融资融券标的证券解析开始...')
        for data in data_:
            sec_code = data[0]
            sec_name = data[1]
            rz_rate = data[2]
            rq_rate = data[3]
            rzrq_data.append([sec_code, sec_name, rz_rate, rq_rate])

        temp_data = securities_normal_parsing_data(rzrq_data)
        for temp in temp_data:
            if len(temp) == 5:
                rz_data.append([temp[0], temp[1], temp[2], temp[4]])
                rq_data.append([temp[0], temp[1], temp[3], temp[4]])
            else:
                logger.error(f'该条记录无证券id{temp},需人工修复!')

        logger.info(f'中泰证券融资标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 4, rz_data)
        logger.info(f'中泰证券融资标的证券解析结束...')

        time.sleep(5)
        logger.info(f'中泰证券融券标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 5, rq_data)
        logger.info(f'中泰证券融券标的证券解析结束...')

        logger.info(f'中泰证券融资融券标的证券解析结束...')
