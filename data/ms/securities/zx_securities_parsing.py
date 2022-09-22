#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/18 11:16
# @Site    : 
# @File    : zx_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def zx_parsing_data(rs, data_):
    bzj_data = []
    rz_data = []
    rq_data = []
    rzrq_data = []
    stockgroup_data = []
    if rs[2] == '2':
        logger.info(f'中信证券可充抵保证金证券解析开始...')
        for data in data_:
            market = data['exchangeCode']
            sec_code = data['stockCode']
            sec_name = data['stockName']
            if data['status'] == '受限':
                rate = None
            else:
                rate = rate_is_normal_one(data['percent'])
            bzj_data.append([market, sec_code, sec_name, rate])
        securities_bzj_parsing_data(rs, 3, bzj_data)
        logger.info(f'中信证券可充抵保证金证券解析结束...')

        time.sleep(5)
        logger.info(f'中信证券集中度分组数据解析开始...')
        # 集中度分组数据解析
        for _data in data_:
            stockgroup_name = None
            stock_group = _data['stockgroup_name']
            if stock_group == 'A':
                stockgroup_name = 1
            elif stock_group == 'B':
                stockgroup_name = 2
            elif stock_group == 'C':
                stockgroup_name = 3
            elif stock_group == 'D':
                stockgroup_name = 4
            elif stock_group == 'E':
                stockgroup_name = 5
            elif stock_group == 'F':
                stockgroup_name = 6
            market = _data['exchangeCode']
            sec_code = _data['stockCode']
            sec_name = _data['stockName']
            stockgroup_data.append([market, sec_code, sec_name, stockgroup_name])
        # print(stockgroup_data)
        # print(len(stockgroup_data))
        # title = ['市场', '证券代码', '证券简称', '证券集中度分组']
        # file = pd.DataFrame(columns=title, data=stockgroup_data)
        # # file.to_csv('dir')
        # file.to_excel('中信证券集中度分组.xlsx')

        securities_stockgroup_parsing_data(rs, 4, stockgroup_data)
        logger.info(f'中信证券集中度分组数据解析结束...')

    elif rs[2] == '3':
        logger.info(f'中信证券融资融券标的证券解析开始...')
        for data in data_:
            sec_code = data['stockCode']
            sec_name = data['stockName']
            # rz_rate = round(float(str(data[2])) * 100, 3)
            # rq_rate = round(float(str(data[3])) * 100, 3)
            rz_rate = rate_is_normal_one(data['rzPercent'])
            rq_rate = rate_is_normal_one(data['rqPercent'])
            rzrq_data.append([sec_code, sec_name, rz_rate, rq_rate])

        temp_data = securities_normal_parsing_data(rzrq_data)
        for temp in temp_data:
            if len(temp) == 6:
                rz_data.append([temp[0], temp[1], temp[2], temp[4], temp[5]])
                rq_data.append([temp[0], temp[1], temp[3], temp[4], temp[5]])
            else:
                logger.error(f'该条记录无证券id{temp},需人工修复!')

        logger.info(f'中信证券融资标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 1, rz_data)
        logger.info(f'中信证券融资标的证券解析结束...')

        time.sleep(5)
        logger.info(f'中信证券融券标的证券解析开始...')
        securities_rzrq_parsing_data(rs, 2, rq_data)
        logger.info(f'中信证券融券标的证券解析结束...')

        logger.info(f'中信证券融资融券标的证券解析结束...')



