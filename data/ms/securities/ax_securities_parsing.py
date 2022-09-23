#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/22 18:38
# @Site    : 
# @File    : ax_securities_parsing.py
# @Software: PyCharm

from data.ms.genralhandler import *


def ax_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    rzrq_data = []
    if rs[2] == '2':
        logger.info(f'安信证券可充抵保证金证券解析开始...')
        for data in data_:
            if data['market'] == '1':
                market = '上海'
            elif data['market'] == '0':
                market = '深圳'
            else:
                market = '北京'
            sec_code = data['secuCode']
            sec_name = data['secuName']
            # rate = round(float(str(data[2])) * 100, 3)
            rate = rate_is_normal_one(data['collatRatio'])
            bzj_data.append([market, sec_code, sec_name, rate])
        securities_bzj_parsing_data(rs, 3, bzj_data)
        logger.info(f'安信证券可充抵保证金证券解析结束...')

    elif rs[2] == '3':
        logger.info(f'安信证券融资融券标的证券解析开始...')
        for data in data_:
            sec_code = data['secuCode']
            sec_name = data['secuName']
            rz_rate = rate_is_normal_one(data['fiMarginRatio'])
            rq_rate = rate_is_normal_one(data['slMarginRatio'])
            rzrq_data.append([sec_code, sec_name, rz_rate, rq_rate])

        temp_data = securities_normal_parsing_data(rzrq_data)
        for temp in temp_data:
            if len(temp) == 6:
                rz_data.append([temp[0], temp[1], temp[2], temp[4], temp[5]])
                rq_data.append([temp[0], temp[1], temp[3], temp[4], temp[5]])
            else:
                logger.error(f'该条记录无证券id{temp},需人工修复!')

        logger.info(f'安信证券融资标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 1, rz_data)
        logger.info(f'安信证券融资标的证券解析结束...')

        time.sleep(5)
        logger.info(f'安信证券融券标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 2, rq_data)
        logger.info(f'安信证券融券标的证券解析结束...')

        logger.info(f'安信证券融资融券标的证券解析结束...')