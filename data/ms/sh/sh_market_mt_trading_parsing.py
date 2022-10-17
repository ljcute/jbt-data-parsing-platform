#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/28 17:11
# @Site    : 
# @File    : sh_market_mt_trading_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
factor_request_url = cf.get('factor-url', 'url')


def sh_parsing_data(rs, data_):
    temp_list = []
    ls = []
    if rs[3] == '上海交易所':
        for a in data_:
            ls.append([a['0'], a['1'], a['2']])

        for data in ls:
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
    actual_date = rs[1]
    for temp_data in ls:
        for sec in sec_type_list:
            if temp_data[1] == sec['sec_code_market']:
                secu_id = sec['sec_id']
                temp_data.append(secu_id)
                secu_type = get_secu_type(sec['sec_category'])
                temp_data.append(secu_type)

        logger.info(temp_data)
    if rs[2] == '2':
        # 担保券
        stock_temp_list = []
        bond_temp_list = []
        fund_temp_list = []
        for i in ls:
            if i[4] == 'stock':
                stock_temp_list.append([i[3]])
            elif i[4] == 'bond':
                bond_temp_list.append([i[3]])
            elif i[4] == 'fund':
                fund_temp_list.append([i[3]])

        stock_query_data = {
            "module": "server.fdb.factor.api.factor_api",
            "method": "get_factor_data_cube",
            "kwargs": {
                "dt": str(rs[1]),
                "obj_type": "stock",
                "object_ids": stock_temp_list,
                "factors": "s_ex_discount_rate"
            }
        }
        stock_res_list = query_normal_rate(stock_query_data)['data']['data']
        for b in ls:
            for res in stock_res_list:
                if str(b[3]) == res[0]:
                    b.append(res[1])

        bond_query_data = {
            "module": "server.fdb.factor.api.factor_api",
            "method": "get_factor_data_cube",
            "kwargs": {
                "dt": str(rs[1]),
                "obj_type": "bond",
                "object_ids": bond_temp_list,
                "factors": "b_ex_discount_rate"
            }
        }
        bond_res_list = query_normal_rate(bond_query_data)['data']['data']
        for bb in ls:
            for res in bond_res_list:
                if str(bb[3]) == res[0]:
                    bb.append(res[1])

        fund_query_data = {
            "module": "server.fdb.factor.api.factor_api",
            "method": "get_factor_data_cube",
            "kwargs": {
                "dt": str(rs[1]),
                "obj_type": "fund",
                "object_ids": fund_temp_list,
                "factors": "f_ex_discount_rate"
            }
        }
        fund_res_list = query_normal_rate(fund_query_data)['data']['data']
        for bbb in ls:
            for res in fund_res_list:
                if str(bbb[3]) == res[0]:
                    bbb.append(res[1])

        a = []
        b = []
        for bb in ls:
            if len(bb) == 5:
                a.append(bb)
            elif len(bb) == 6:
                b.append(bb)
        if a:
            logger.error(f'如下数据无法通过全量因子库查询到对应折算率上限:{a}')
    data_parsing(rs, ls)


# 调用全量因子库接口
def query_normal_rate(data):
    url = factor_request_url + '/api/gateway'
    res = requests.post(url=url, json=data)
    if res.status_code != 200:
        logger.error(f'请求服务异常，uri={url}，json={data}，response={res.text}', exc_info=True)
        raise Exception(res.text)
    if res.text:
        try:
            return res.json()
        except Exception as ex:
            logger.error(f'请求服务异常，uri={url}，json={data}，response={res.text}，error={ex}', exc_info=True)
            raise ex
    else:
        return None


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
    result = query_business_security_item_jys(get_yesterday_date(str(rs[1])), biz_type, broker_id, 1)
    if result.empty:
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        logger.info(f'进入为空的判断...')
        insert_data_list = []
        if biz_type == 3:
            for i in data_:
                if len(i) == 5:
                    insert_data_list.append(
                        [broker_id, None if i[3] == '-' else i[3], i[4], biz_type, adjust_status_in, None, None, 1, 1,
                         rs[1],
                         forever_end_dt, 1])

                if len(i) == 6:
                    insert_data_list.append(
                        [broker_id, None if i[3] == '-' else i[3], i[4], biz_type, adjust_status_in, None, i[5], 1, 1,
                         rs[1],
                         forever_end_dt, 1])

        elif biz_type == 1:
            for i in data_:
                if len(i) == 5:
                    insert_data_list.append(
                        [broker_id, None if i[3] == '-' else i[3], i[4], biz_type, adjust_status_in, None, 100, 1, 1,
                         rs[1],
                         forever_end_dt, 1])

        elif biz_type == 2:
            for i in data_:
                if len(i) == 5:
                    insert_data_list.append(
                        [broker_id, None if i[3] == '-' else i[3], i[4], biz_type, adjust_status_in, None, 50, 1, 1,
                         rs[1],
                         forever_end_dt, 1])

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
            if len(row) == 5 or len(row) == 6:
                query_list.append(row[3])
        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            temp_insert_data_list = []
            for b in s_list:
                for temp_data in data_:
                    if b == temp_data[3]:
                        res = query_is_have_secu_id_jys(str(rs[1]).replace('-', ''), biz_type, broker_id, b, 1)
                        if not res:
                            secu_type = temp_data[4]
                            temp_insert_data_list.append(
                                [broker_id, None if b == '-' else b, secu_type, biz_type, adjust_status_in, None,
                                 temp_data[5] if len(temp_data) == 6 else None, 1, 1, rs[1],
                                 forever_end_dt, 1])
            if temp_insert_data_list:
                logger.info(f'上海交易所业务数据入库开始...')
                insert_broker_mt_business_security(temp_insert_data_list)
                logger.info(f'上海交易所业务数据入库完成，共{len(temp_insert_data_list)}条')

        b_list = list(set(haved_list).difference(set(query_list)))
        if b_list:
            for s in b_list:
                rs1 = query_is_have_secu_id_jys((str(rs[1])).replace('-', ''), biz_type, broker_id, s, 1)
                secu_type = rs1[0][3]
                pre = rs1[0][7]
                rss = query_is_have_secu_id_jys_out((str(rs[1])).replace('-', ''),biz_type,broker_id,s, 1, adjust_status_out)
                if not rss:
                    update_business_security_jys((str(rs[1])).replace('-', ''), s, broker_id, biz_type, 1)
                    insert_data_list = []
                    insert_data_list.append([broker_id, s, secu_type, biz_type, adjust_status_out, pre,
                                         None, 1, 1, str(rs[1]), forever_end_dt, 1])
                    if insert_data_list:
                        insert_broker_mt_business_security(insert_data_list)