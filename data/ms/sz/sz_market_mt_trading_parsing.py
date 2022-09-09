#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/16 11:01
# @Site    : 
# @File    : sz_market_mt_trading_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *


def sz_parsing_data(rs, data_):
    new_data = []
    for ss in data_:
        new_data.append([ss[0], ss[1]])

    temp_list = []
    if rs[3] == '深圳交易所':
        for data in new_data:
            data[0] = data[0] + '.SZ'
            temp_list.append(data[0])

    query_datas = {
        "module": "pysec.etl.sec360.api.sec_api",
        "method": "query_sec",
        "args": temp_list
    }
    query_result = post_data_job(query_datas)
    sec_type_list = query_result['data']
    args = []
    for temp_data in new_data:
        for sec in sec_type_list:
            if temp_data[0] == sec['sec_code_market']:
                temp_data.append(sec['sec_id'])
                secu_type = get_secu_type(sec['sec_category'])
                temp_data.append(secu_type)

        logger.info(temp_data)


    if rs[2] == '2':
        logger.info(f'担保券证券解析')
        sz_data_parsing(rs, 3, new_data)
    elif rs[2] == '3':
        logger.info(f'融资标的证券解析')
        sz_data_parsing(rs, 1, new_data)
        time.sleep(5)
        logger.info(f'融券标的证券解析')
        sz_data_parsing(rs, 2, new_data)


def sz_data_parsing(rs, biz_type, data_):
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
            if len(i) == 4:
                insert_data_list.append([broker_id, None if i[1] == '-' else i[1], i[2], biz_type, adjust_status_in, None, None, 1, 1, rs[1],
                                         forever_end_dt, None])
            else:
                logger.error(f'该条数据无证券id，请检查!{i}')
                invalid_data_list.append(i)
                raise Exception(f'该条数据无证券id，请检查!{i}')
        if insert_data_list:
            logger.info(f'深圳交易所业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list)
            logger.info(f'深圳交易所业务数据入库完成，共{len(insert_data_list)}条')

    else:
        logger.info(f'进入不为空的判断...')
        haved_list = []
        query_list = []
        for col in result.values.tolist():
            haved_list.append(col[2])

        for row in data_:
            if len(row) == 4:
                query_list.append(row[2])


        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            temp_insert_data_list = []
            for b in s_list:
                for temp_data in data_:
                    if b == temp_data[2]:
                        secu_type = temp_data[3]
                        temp_insert_data_list.append([broker_id, None if b == '-' else b, secu_type, biz_type, adjust_status_in, None, None, 1, 1,
                                                      datetime.datetime.now(), forever_end_dt, None])
            logger.info(f'深圳交易所业务数据入库开始...')
            insert_broker_mt_business_security(temp_insert_data_list)
            logger.info(f'深圳交易所业务数据入库完成，共{len(temp_insert_data_list)}条')

