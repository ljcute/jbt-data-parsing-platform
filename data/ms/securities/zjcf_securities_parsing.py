#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/22 18:38
# @Site    :
# @File    : zjcf_securities_parsing.py
# @Software: PyCharm

from data.ms.genralhandler import *


def zjcf_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    rzrq_data = []
    if rs[2] == '2':
        logger.info(f'中金财富可充抵保证金证券解析开始...')
        for data in data_:
            if data['exchange'] == 'SZ':
                market = '深圳'
            elif data['exchange'] == 'SH':
                market = '上海'
            else:
                market = '北京'
            sec_code = data['stockId']
            sec_name = data['stockName']
            # rate = round(float(str(data[2])) * 100, 3)
            rate = rate_is_normal_one(data['rate'])
            bzj_data.append([market, sec_code, sec_name, rate])
        securities_bzj_parsing_data(rs, 3, bzj_data)
        logger.info(f'中金财富可充抵保证金证券解析结束...')

    elif rs[2] == '3':
        logger.info(f'中金财富融资融券标的证券解析开始...')
        for data in data_:
            sec_code = data['stockId']
            sec_name = data['stockName']
            if data['moneyTarget'] == 'N':
                rz_rate = None
            else:
                rz_rate = rate_is_normal_one(data['guaranteeMoney'])
            if data['stockTarget'] == 'N':
                rq_rate = None
            else:
                rq_rate = rate_is_normal_one(data['guaranteeStock'])
            rzrq_data.append([sec_code, sec_name, rz_rate, rq_rate])

        temp_data = securities_normal_parsing_data(rzrq_data)
        for temp in temp_data:
            if len(temp) == 6:
                rz_data.append([temp[0], temp[1], temp[2], temp[4], temp[5]])
                rq_data.append([temp[0], temp[1], temp[3], temp[4], temp[5]])
            else:
                logger.error(f'该条记录无证券id{temp},需人工修复!')

        logger.info(f'中金财富融资标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 1, rz_data)
        logger.info(f'中金财富融资标的证券解析结束...')

        time.sleep(5)
        logger.info(f'中金财富融券标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 2, rq_data)
        logger.info(f'中金财富融券标的证券解析结束...')

        logger.info(f'中金财富融资融券标的证券解析结束...')