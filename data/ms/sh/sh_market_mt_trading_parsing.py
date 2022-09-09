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
            data[1] = str(data[1]).replace(' ', '') + '.SH'
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
                secu_type = get_secu_type(sec['sec_category'])
                temp_data.append(secu_type)

        logger.info(temp_data)

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
        logger.info(f'进入为空的判断...')
        insert_data_list = []
        for i in data_:
            if len(i) == 5:
                insert_data_list.append([broker_id, None if i[3] == '-' else i[3], i[4], biz_type, adjust_status_in, None, None, 1, 1, rs[1],
                                         forever_end_dt, None])
            else:
                logger.error(f'该条数据无证券id，请检查!{i}')
                invalid_data_list.append(i)
        if insert_data_list:
            logger.info(f'上海交易所业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list)
            logger.info(f'上海交易所业务数据入库完成，共{len(insert_data_list)}条')
    else:
        logger.info(f'进入不为空的判断...')
        haved_list = []
        query_list = []
        for col in result.values.tolist():
            haved_list.append(col[2])
        for row in data_:
            if len(row) == 5:
                query_list.append(row[3])
        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            temp_insert_data_list = []
            for b in s_list:
                for temp_data in data_:
                    if b == temp_data[3]:
                        secu_type = temp_data[4]
                        temp_insert_data_list.append([broker_id, None if b == '-' else b, secu_type, biz_type, adjust_status_in, None, None, 1, 1, datetime.datetime.now(), forever_end_dt, None])
            logger.info(f'上海交易所业务数据入库开始...')
            insert_broker_mt_business_security(temp_insert_data_list)
            logger.info(f'上海交易所业务数据入库完成，共{len(temp_insert_data_list)}条')




