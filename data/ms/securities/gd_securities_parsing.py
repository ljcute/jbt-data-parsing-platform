#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/23 17:22
# @Site    : 
# @File    : gd_securities_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def gd_parsing_data(rs, data_):
    bzj_data = []
    if rs[2] == '2':
        logger.info(f'光大证券可充抵保证金证券解析开始...')
        for data in data_:
            market = data[0]
            sec_code = data[1]
            sec_name = data[2]
            rate = round(float(str(data[3]).strip('%')), 3)
            bzj_data.append([market, sec_code, sec_name, rate])
        securities_bzj_parsing_data(rs, rs[2], bzj_data)
        logger.info(f'光大证券可充抵保证金证券解析结束...')
