#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/23 18:06
# @Site    : 
# @File    : xy_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def xy_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    rzrq_data = []
    if rs[2] == '2':
        logger.info(f'兴业证券可充抵保证金证券解析开始...')
        for data in data_:
            sec_code = data[1]
            sec_name = data[2]
            rate = round(float(str(data[3])) * 100, 3)
            bzj_data.append([sec_code, sec_name, rate])
        securities_bzj_parsing_data_no_market(rs, bzj_data)
        logger.info(f'兴业证券可充抵保证金证券解析结束...')

    elif rs[2] == '3':
        logger.info(f'兴业证券融资融券标的证券解析开始...')
        for data in data_:
            sec_code = data[1]
            sec_name = data[2]
            rz_rate = None if data[3] == '-' or data[3] == '/' else round(float(str(data[3])) * 100, 3)
            rq_rate = None if data[4] == '-' or data[4] == '/' else round(float(str(data[4])) * 100, 3)
            rzrq_data.append([sec_code, sec_name, rz_rate, rq_rate])

        temp_data = securities_normal_parsing_data(rzrq_data)
        for temp in temp_data:
            if len(temp) == 6:
                rz_data.append([temp[0], temp[1], temp[2], temp[4], temp[5]])
                rq_data.append([temp[0], temp[1], temp[3], temp[4], temp[5]])
            else:
                logger.error(f'该条记录无证券id{temp},需人工修复!')

        logger.info(f'兴业证券融资标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 1, rz_data)
        logger.info(f'兴业证券融资标的证券解析结束...')

        time.sleep(5)
        logger.info(f'兴业证券融券标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 2, rq_data)
        logger.info(f'兴业证券融券标的证券解析结束...')

        logger.info(f'兴业证券融资融券标的证券解析结束...')
