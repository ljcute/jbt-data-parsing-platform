#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/28 17:11
# @Site    : 
# @File    : sh_market_mt_trading_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def sh_parsing_data(rs, data_):
    temp_list = []
    if rs[3] == '上海交易所':
        for data in data_:
            data[1] = data[1] + '.SH'
            temp_list.append(data[1])

    query_datas = {
        "module": "pysec.etl.sec360.api.sec_api",
        "method": "query_sec",
        "args": temp_list
    }
    query_result = post_data_job(query_datas)
    sec_type_list = query_result['data']
    args = []
    for temp_data in data_:
        for sec in sec_type_list:
            if temp_data[1] == sec['sec_code_market']:
                temp_data.append(sec['sec_id'])
                sec_code_market = sec['sec_code_market']
                sec_category = sec['sec_category']
                sec_type = None
                if sec_category == 'sec_stock':
                    sec_type = 'AS'
                elif sec_category == 'sec_bond':
                    sec_type = 'B'
                elif sec_category == 'sec_fund':
                    sec_type = 'OF'
                sec_name = str(temp_data[2]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)') \
                    .replace('⑷', '(4)').replace('⑼', '(9)')
                args_dict = {
                    "sec_code_market": sec_code_market,
                    "sec_type": sec_type,
                    "sec_name": sec_name,
                    "update_flag": 1
                }
                args.append(args_dict)

    sync_datas = {
        "module": "pysec.etl.sec360.api.sec_api",
        "method": "sync_sec",
        "args": args
    }
    post_data_job(sync_datas)
    data_parsing(rs, data_)


def data_parsing(rs, data_):
    biz_type = None
    if rs[2] == '2':
        biz_type = 3
    elif rs[2] == '4':
        biz_type = 1
    elif rs[2] == '5':
        biz_type = 2

    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), biz_type, broker_id)
    if result.empty:
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in data_:
            if i[-1]:
                insert_data_list.append([broker_id, i[-1], biz_type, adjust_status_in, None, None, 1, 1, rs[1],
                                         forever_end_dt, None])
            else:
                logger.error(f'该条数据无证券id，请检查!{i}')
                invalid_data_list.append(i)
                raise Exception(f'该条数据无证券id，请检查!{i}')
        if len(data_) == len(insert_data_list) and len(data_) > 0 and len(insert_data_list) > 0:
            insert_broker_mt_business_security(insert_data_list)
    else:
        haved_list = []
        query_list = []
        for col in result.values.tolist():
            haved_list.append(col[2])
        for row in data_:
            query_list.append(row[-1])

        s_list = list(set(haved_list).intersection(set(query_list)))
        if not s_list:
            temp_insert_data_list = []
            for b in query_list:
                temp_insert_data_list.append([broker_id, b, biz_type, adjust_status_in, None, None, 1, 1,
                                              datetime.datetime.now(), forever_end_dt, None])
            insert_broker_mt_business_security(temp_insert_data_list)
