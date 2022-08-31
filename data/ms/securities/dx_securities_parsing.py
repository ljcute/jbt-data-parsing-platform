#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/22 15:39
# @Site    : 
# @File    : dx_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *

'''
东兴证券因数据质量不好不再进行数据采集和解析
'''


def dx_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    rzrq_data = []
    if rs[2] == '2':
        logger.info(f'东兴证券可充抵保证金证券解析开始...')
        for data in data_:
            sec_code = data[1]
            sec_name = data[2]
            rate = round(float(str(data[3]).strip('%')), 3)

            bzj_data.append([sec_code, sec_name, rate])
        securities_bzj_parsing_data_no_market(rs, bzj_data)
        logger.info(f'东兴证券可充抵保证金证券解析结束...')


    elif rs[2] == '3':
        logger.info(f'东兴证券融资融券标的证券解析开始...')
        for data in data_:
            sec_code = data[1]
            sec_name = data[2]
            rz_rate = round(float(str(data[3]).strip('%')), 3)
            rq_rate = round(float(str(data[4]).strip('%')), 3)
            rzrq_data.append([sec_code, sec_name, rz_rate, rq_rate])

        temp_data = securities_normal_parsing_data(rzrq_data)
        for temp in temp_data:
            if len(temp) == 6:
                rz_data.append([temp[0], temp[1], temp[2], temp[4], temp[5]])
                rq_data.append([temp[0], temp[1], temp[3], temp[4], temp[5]])
            else:
                logger.error(f'注册中心和360都查不到证券id{temp},需人工修复!')

        logger.info(f'东兴证券融资标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 1, rz_data)
        logger.info(f'东兴证券融资标的证券解析结束...')

        time.sleep(5)
        logger.info(f'东兴证券融券标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 2, rq_data)
        logger.info(f'东兴证券融券标的证券解析结束...')

        logger.info(f'东兴证券融资融券标的证券解析结束...')
