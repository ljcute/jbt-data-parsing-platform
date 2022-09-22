#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/23 16:20
# @Site    : 
# @File    : zs_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def zs_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    error_list = []
    if rs[2] == '2':
        logger.info(f'招商证券可充抵保证金证券解析开始...')
        for data in data_:
            if data['market'] == '1':
                market = '沪市'
            elif data['market'] == '0':
                market = '深市'
            else:
                market = '北市'
            sec_code = data['stkcode']
            sec_name = data['stkname']
            # rate = round(float(str(data[3])) * 100, 3)
            rate = rate_is_normal_one(data['pledgerate'])
            bzj_data.append([market, sec_code, sec_name, rate])
        securities_bzj_parsing_data(rs, 3, bzj_data)
        logger.info(f'招商证券可充抵保证金证券解析结束...')

    elif rs[2] == '4':
        logger.info(f'招商证券融资标的证券解析开始...')
        for data in data_:
            sec_code = data['stkcode']
            sec_name = data['stkname']
            # rate = round(float(str(data[3])) * 100, 3)
            rate = rate_is_normal_one(data['marginratefund'])
            rz_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rz_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, 1, temp_data)
        logger.info(f'招商证券融资标的证券解析结束...')

    elif rs[2] == '5':
        logger.info(f'招商证券融券标的证券解析开始...')
        for data in data_:
            sec_code = data['stkcode']
            sec_name = data['stkname']
            # rate = round(float(str(data[3])) * 100, 3)
            rate = rate_is_normal_one(data['marginratefund'])
            rq_data.append([sec_code, sec_name, rate])

        temp_data = securities_normal_parsing_data_no_market(rq_data)
        for temp in temp_data:
            if len(temp) == 3:
                logger.error(f'该条记录无证券id{temp},需人工修复!')
                error_list.append(temp)

        securities_rzrq_parsing_data(rs, 2, temp_data)
        logger.info(f'招商证券融券标的证券解析结束...')