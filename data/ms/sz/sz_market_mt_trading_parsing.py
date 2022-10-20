#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/16 11:01
# @Site    : 
# @File    : sz_market_mt_trading_parsing.py
# @Software: PyCharm
from data.ms.genralhandler import *
from data.ms.sh.sh_market_mt_trading_parsing import query_normal_rate


def sz_parsing_data(rs, data_):
    new_data = []
    for ss in data_:
        new_data.append([ss['0'], ss['1']])

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
    actual_date = rs[1]
    for temp_data in new_data:
        for sec in sec_type_list:
            if temp_data[0] == sec['sec_code_market']:
                secu_id = sec['sec_id']
                temp_data.append(secu_id)
                secu_type = get_secu_type(sec['sec_category'])
                temp_data.append(secu_type)

        logger.info(temp_data)
    if rs[2] == '2':
        stock_temp_list = []
        bond_temp_list = []
        fund_temp_list = []
        for i in new_data:
            if i[3] == 'stock':
                stock_temp_list.append([i[2]])
            elif i[3] == 'bond':
                bond_temp_list.append([i[2]])
            elif i[3] == 'fund':
                fund_temp_list.append([i[2]])

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
        for b in new_data:
            for res in stock_res_list:
                if str(b[2]) == res[0]:
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
        for bb in new_data:
            for res in bond_res_list:
                if str(bb[2]) == res[0]:
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
        for bbb in new_data:
            for res in fund_res_list:
                if str(bbb[2]) == res[0]:
                    bbb.append(res[1])

        a = []
        b = []
        for bb in new_data:
            if len(bb) == 4:
                a.append(bb)
            elif len(bb) == 5:
                b.append(bb)
        if a:
            logger.error(f'如下数据无法通过全量因子库查询到对应折算率上限:{a}')

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
    result = query_business_security_item_jys(get_yesterday_date(str(rs[1])), biz_type, broker_id, 2)
    if result.empty:
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        logger.info(f'进入为空的判断...')
        insert_data_list = []
        if biz_type == 3:
            for i in data_:
                if len(i) == 4:
                    insert_data_list.append(
                        [broker_id, None if i[2] == '-' else i[2], i[3], biz_type, adjust_status_in, None, 0, 1, 1,
                         rs[1],
                         forever_end_dt, 2])

                if len(i) == 5:
                    insert_data_list.append(
                        [broker_id, None if i[2] == '-' else i[2], i[3], biz_type, adjust_status_in, None, i[4], 1, 1,
                         rs[1],
                         forever_end_dt, 2])

        elif biz_type == 1:
            for i in data_:
                if len(i) == 4:
                    insert_data_list.append(
                        [broker_id, None if i[2] == '-' else i[2], i[3], biz_type, adjust_status_in, None, 100, 1, 1,
                         rs[1],
                         forever_end_dt, 2])

        elif biz_type == 2:
            for i in data_:
                if len(i) == 4:
                    insert_data_list.append(
                        [broker_id, None if i[2] == '-' else i[2], i[3], biz_type, adjust_status_in, None, 50, 1, 1,
                         rs[1],
                         forever_end_dt, 2])

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
            if len(row) == 4 or len(row) == 5:
                query_list.append(row[2])

        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            temp_insert_data_list = []
            for b in s_list:
                for temp_data in data_:
                    if b == temp_data[2]:
                        res = query_is_have_secu_id_jys(str(rs[1]).replace('-', ''), biz_type, broker_id, b, 2)
                        if not res:
                            secu_type = temp_data[3]
                            temp_insert_data_list.append([broker_id, None if b == '-' else b, secu_type, biz_type, adjust_status_in, None, temp_data[4] if len(temp_data)== 5 else 0, 1, 1,
                                                          rs[1], forever_end_dt, 2])
            if temp_insert_data_list:
                logger.info(f'深圳交易所业务数据入库开始...')
                insert_broker_mt_business_security(temp_insert_data_list)
                logger.info(f'深圳交易所业务数据入库完成，共{len(temp_insert_data_list)}条')

        b_list = list(set(haved_list).difference(set(query_list)))
        if b_list:
            for s in b_list:
                rs1 = query_is_have_secu_id_jys((str(rs[1])).replace('-', ''), biz_type, broker_id, s, 2)
                secu_type = rs1[0][3]
                pre = rs1[0][7]
                rss = query_is_have_secu_id_jys_out((str(rs[1])).replace('-', ''),biz_type,broker_id,s, 2, adjust_status_out)
                if not rss:
                    update_business_security_jys((str(rs[1])).replace('-', ''), s, broker_id,  biz_type, 2)
                    insert_data_list = []
                    insert_data_list.append([broker_id, s, secu_type, biz_type, adjust_status_out, pre,
                                         None, 1, 1, str(rs[1]), forever_end_dt, 2])
                    if insert_data_list:
                        insert_broker_mt_business_security(insert_data_list)


        today_res = query_business_security_item_today_jys((str(rs[1])).replace('-', ''),biz_type,broker_id,2)
        today_list = today_res['secu_id'].values.tolist()

        insert_data_list_noempty = []
        for row in data_:
            if len(row) == 4 or len(row) == 5:
                sec_code = row[0]
                sec_id = row[2]
                secu_type = row[3]
                round_rate = row[4] if len(row) == 5 else 0

                db_record = df_exists_index(result, sec_code, sec_id)
                if db_record is not None:
                    old_rate = db_record[7]
                    adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                    if adjust_status != adjust_status_invariant:
                        temp_result = query_business_secu_id_item_jys((str(rs[1])).replace('-', ''), biz_type, broker_id,
                                                                  sec_id, 2)
                        if temp_result.empty:
                            if adjust_status == adjust_status_high:
                                # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security_one_jys((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type,2)

                                insert_data_list_noempty = [
                                    [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate, round_rate,
                                     1, 1,str(rs[1]), forever_end_dt, 2]]
                            elif adjust_status == adjust_status_low:
                                # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security_one_jys((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type,2)

                                insert_data_list_noempty = [
                                    [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate, round_rate, 1,
                                     1,str(rs[1]), forever_end_dt, 2]]
                        else:
                            # 如果有，则不为第一次解析，需要判断pre_value是否相同
                            pre_temp_vaule = temp_result.values.tolist()[0][7]
                            if round_rate == pre_temp_vaule:
                                pass
                            else:
                                # 值不同，则失效之前的值，新插入最新的值
                                update_business_security_useless_jys((str(rs[1])).replace('-', ''), sec_id, broker_id,
                                                                 biz_type,2)
                                # 再做对应逻辑判断
                                if adjust_status == adjust_status_high:
                                    # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                    update_business_security_one_jys((str(rs[1])).replace('-', ''), sec_id, broker_id,
                                                                     biz_type, 2)

                                    insert_data_list_noempty = [
                                        [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate,
                                         round_rate,1, 1, str(rs[1]), forever_end_dt, 2]]
                                elif adjust_status == adjust_status_low:
                                    # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                    update_business_security_one_jys((str(rs[1])).replace('-', ''), sec_id, broker_id,
                                                                     biz_type, 2)

                                    insert_data_list_noempty = [
                                        [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate,
                                         round_rate, 1,1, str(rs[1]), forever_end_dt, 2]]
                    else:
                        if sec_id in today_list:
                            update_business_expalin_jys((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type,
                                                    forever_end_dt,2)
                            update_business_expalin_other_jys((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type,
                                                          forever_end_dt,2)
        if insert_data_list_noempty:
            logger.info(f'业务数据入库开始,t_broker_mt_business_security...')
            insert_broker_mt_business_security(insert_data_list_noempty)
            logger.info(f'业务数据入库完成，共{len(insert_data_list_noempty)}条')