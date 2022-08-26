#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/18 11:51
# @Site    : 
# @File    : genralhandler.py
# @Software: PyCharm
import requests
from utils.logs_utils import logger
from constants import *
from decimal import Decimal
from data.dao.biz.biz_data_deal import *

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
request_url = cf.get('360etl-url', 'url')
request_url_zc_center = cf.get('zc-center-url', 'url')


# 比较调整前，调整后的rate值
def get_adjust_status_by_two_rate(pre_value, current_rate):
    if current_rate is None or current_rate == '-' or current_rate == '--':  # 当前rate是'-'表示调出
        return adjust_status_out

    pre_value2 = Decimal(pre_value).quantize(Decimal('0.00'))
    current_value2 = Decimal(current_rate).quantize(Decimal('0.00'))

    result = pre_value2.compare(current_value2)

    if result == 1:
        return adjust_status_low
    elif result == -1:
        return adjust_status_high
    return adjust_status_invariant  # 不变


# 判断df中是否存在某个字段
def df_exists_index(df, sec_code, sec_id):
    try:
        df_record = df[(df.secu_id == sec_id) & (sec_code is not None)]
        if not df_record.empty:
            for i, row in df_record.iterrows():
                return row
    except Exception as es:
        logger.error(es)
    return None


# 调用360etl接口
def post_data_job(data):
    """
    POST JSON请求指定服务
    :param url:<str> 请求资源URI
    :param data:<obj> 请求对象
    :return :<dict> 响应一个字典对象
    """

    url = request_url + '/api/gateway'
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


# 调用注册中心接口，通过证券代码，证券简称查询类型，code
def get_securities_type_job(data):
    """
        POST JSON请求指定服务
        :param data: boCode,boName
        :param boCode:<str> 证券代码
        :param boName:<str> 证券简称
        :return :<dict> 响应字典对象，如只传证券代码，则可能会有多个返回结果，如都传，则可能匹配不到
        """

    url = request_url_zc_center + '/bo/search'
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


# 融资融券组合数据获取证券id公共方法--带市场的处理
def securities_normal_parsing_data(data):
    for data_ in data:
        boCode = data_[0]
        boName = str(data_[1]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷', '(4)').replace(
            '⑼', '(9)')
        data_dict = {"boCode": boCode, "boName": boName}
        res = get_securities_type_job(data_dict)
        if res:
            if len(res) == 1:
                secu_id = res[0]['boId']
                secu_type = res[0]['boIdType']
                data_.append(secu_id, secu_type)
            elif len(res) > 1:
                for r in res:
                    if r['boName'] == boName:
                        secu_id = r['boId']
                        secu_type = r['boIdType']
                        data_.append(secu_id, secu_type)
                        break
                    else:
                        continue

                # 说明没有查到证券id
                if len(data_) == 4:
                    logger.info(f'未查询到证券id{data_},调用处理方法获取证券id')
                    temp_deal_other(data_, boName)
        else:
            logger.info(f'未查询到证券id{data_},调用处理方法获取证券id')
            temp_deal_other(data_, boName)
        # logger.info(data_)

    return data


# 融资融券组合数据获取证券id公共方法--不带市场的处理
def securities_normal_parsing_data_no_market(data):
    for data_ in data:
        boCode = str(data_[0]).replace(' ', '')
        boName = str(data_[1]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷', '(4)').replace(
            '⑼', '(9)')
        data_dict = {"boCode": boCode, "boName": boName}
        res = get_securities_type_job(data_dict)
        if res:
            if len(res) == 1:
                secu_id = res[0]['boId']
                secu_type = res[0]['boIdType']
                data_.append(secu_id, secu_type)
            elif len(res) > 1:
                for r in res:
                    if r['boName'] == boName:
                        secu_id = r['boId']
                        secu_type = r['boIdType']
                        data_.append(secu_id, secu_type)
                        break
                    else:
                        continue

                # 说明没有查到证券id
                if len(data_) == 3:
                    logger.info(f'未查询到证券id{data_},调用处理方法获取证券id')
                    temp_deal_other(data_, boName)
        else:
            logger.info(f'未查询到证券id{data_},调用处理方法获取证券id')
            temp_deal_other(data_, boName)
        # logger.info(data_)

    return data


# 可充抵保证金rate解析公共方法--带市场的处理
def securities_bzj_parsing_data(rs, biz_type, data_):
    error_list = []
    for data in data_:
        boCode = str(data[1]).replace(' ', '')
        boName = str(data[2]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷', '(4)').replace(
            '⑼', '(9)')
        data_dict = {"boCode": boCode, "boName": boName}
        res = get_securities_type_job(data_dict)
        if res:
            if len(res) == 1:
                secu_id = res[0]['boId']
                secu_type = res[0]['boIdType']
                data.append(secu_id, secu_type)
            elif len(res) > 1:
                for r in res:
                    if r['boName'] == boName:
                        secu_id = r['boId']
                        secu_type = r['boIdType']
                        data.append(secu_id, secu_type)
                        break
                    else:
                        continue
                # 说明没有查到证券id
                if len(data) == 4:
                    temp_deal(data, boName)
        else:
            temp_deal(data, boName)
        # logger.info(data)

    for tempp in data_:
        if len(tempp) == 4:
            logger.error(f'该条记录无证券id{tempp}，需人工修复!')
            error_list.append(tempp)

    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), biz_type, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in data_:
            if len(i) == 6:
                insert_data_list.append([broker_id, i[4], i[5], biz_type, adjust_status_in, None, i[3], 1, 1, rs[1],
                                         forever_end_dt, None])
            else:
                logger.error(f'该条记录无证券id{i},需人工修复!')
                invalid_data_list.append(i)
                # raise Exception(f'该条数据无证券id，请检查!{i}')
        insert_broker_mt_business_security(insert_data_list)
    else:
        logger.info(f'进入不为空的判断')
        for row in data_:
            if len(row) == 6:
                sec_code = row[1]
                sec_id = row[4]
                secu_type = row[5]
                round_rate = row[3]

                db_record = df_exists_index(result, sec_code, sec_id)
                if db_record is not None:
                    old_rate = db_record[6]
                    adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                    if adjust_status != adjust_status_invariant:
                        if adjust_status == adjust_status_high:
                            # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                        elif adjust_status == adjust_status_low:
                            # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                else:
                    insert_list = [[broker_id, sec_id, secu_type, biz_type, adjust_status_in, None, round_rate, 1, 1,
                                    datetime.datetime.now(), forever_end_dt, None]]
                    insert_broker_mt_business_security(insert_list)
            else:
                logger.error(f'该条记录无证券id{row},需人工修复!')
                invalid_data_list.append(row)


# 可充抵保证金rate解析公共方法--不带市场的处理
def securities_bzj_parsing_data_no_market(rs, data_):
    error_list = []
    for data in data_:
        boCode = str(data[0]).replace(' ', '')
        boName = str(data[1]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷', '(4)').replace(
            '⑼', '(9)')
        data_dict = {"boCode": boCode, "boName": boName}
        res = get_securities_type_job(data_dict)
        if res:
            if len(res) == 1:
                secu_id = res[0]['boId']
                secu_type = res[0]['boIdType']
                data.append(secu_id, secu_type)
            elif len(res) > 1:
                for r in res:
                    if r['boName'] == boName:
                        secu_id = r['boId']
                        secu_type = r['boIdType']
                        data.append(secu_id, secu_type)
                        break
                    else:
                        continue
                # 说明没有查到证券id
                if len(data) == 3:
                    temp_deal_other(data, boName)
        else:
            temp_deal_other(data, boName)
        # logger.info(data)

    for tempp in data_:
        if len(tempp) == 3:
            logger.error(f'该条记录无证券id{tempp},需人工修复!')
            error_list.append(tempp)

    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), 3, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in data_:
            if len(i) == 5:
                insert_data_list.append([broker_id, i[3], i[4], 3, adjust_status_in, None, i[2], 1, 1, rs[1],
                                         forever_end_dt, None])
            else:
                logger.error(f'该条记录无证券id{i},需人工修复!')
                invalid_data_list.append(i)
                # raise Exception(f'该条数据无证券id，请检查!{i}')
        insert_broker_mt_business_security(insert_data_list)
    else:
        logger.info(f'进入不为空的判断')
        for row in data_:
            if len(row) == 5:
                sec_code = row[0]
                sec_id = row[3]
                secu_type = row[4]
                round_rate = row[2]

                db_record = df_exists_index(result, sec_code, sec_id)
                if db_record is not None:
                    old_rate = db_record[6]
                    adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                    if adjust_status != adjust_status_invariant:
                        if adjust_status == adjust_status_high:
                            # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, 3)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, 3, adjust_status_high, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                        elif adjust_status == adjust_status_low:
                            # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, 3)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, 3, adjust_status_low, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                else:
                    insert_list = [[broker_id, sec_id, secu_type, 3, adjust_status_in, None, round_rate, 1, 1,
                                    datetime.datetime.now(), forever_end_dt, None]]
                    insert_broker_mt_business_security(insert_list)
            else:
                logger.error(f'该条记录无证券id{row},需人工修复!')


def get_secu_type(etl_type):
    secu_type = None
    if etl_type == 'sec_fund':
        secu_type = 'fund'
    elif etl_type == 'sec_stock':
        secu_type = 'stock'
    elif etl_type == 'sec_bond':
        secu_type = 'bond'
    elif etl_type == 'sec_future':
        secu_type = 'future'
    elif etl_type == 'sec_options':
        secu_type = 'options'
    elif etl_type == 'sec_repurchase':
        secu_type = 'repurchase'
    elif etl_type == 'sec_index':
        secu_type = 'index'
    return secu_type


# 从注册中心无法查到证券id，根据代码规则获取证券id的公共方法--无市场的处理
def temp_deal_other(data, boName):
    args_code = None
    temp_sec_code = str(data[0]).replace(' ', '')
    if temp_sec_code.startswith('0') or temp_sec_code.startswith('3'):
        args_code = temp_sec_code + '.SZ'
    elif temp_sec_code.startswith('6'):
        args_code = temp_sec_code + '.SH'
    elif temp_sec_code.startswith('4') or temp_sec_code.startswith('8'):
        args_code = temp_sec_code + '.BJ'

    args = [args_code]
    query_datas = {
        "module": "pysec.etl.sec360.api.sec_api",
        "method": "query_sec",
        "args": args
    }
    query_result = post_data_job(query_datas)
    res_data = query_result['data']
    error_list = []
    if res_data:
        secu_id = res_data[0]['sec_id']
        etl_type = res_data[0]['sec_category']
        secu_type = get_secu_type(etl_type)
        data.append(secu_id, secu_type)
        sec_category = res_data[0]['sec_category']
        sec_type = None
        if sec_category == 'sec_stock':
            sec_type = 'AS'
        elif sec_category == 'sec_bond':
            sec_type = 'B'
        elif sec_category == 'sec_fund':
            sec_type = 'OF'
        # 注册到注册中心
        args_dict = {
            "sec_code_market": args_code,
            "sec_type": sec_type,
            "sec_name": boName.replace(' ', ''),
            "update_flag": 1
        }
        args_ = [args_dict]
        sync_datas = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "sync_sec",
            "args": args_
        }
        post_data_job(sync_datas)
        logger.info(f'该条已注册到注册中心{data}')
    else:
        error_list.append(data)
        logger.error(f'注册中心和360都查不到证券id{data},需人工修复!')


# 从注册中心无法查到证券id，根据市场从360获取证券id的公共方法--有市场的处理方法
def temp_deal(data, boName):
    args_code = None
    temp_market = str(data[0]).replace(' ', '')
    if temp_market == '深圳' or temp_market == '深市' or temp_market == '2' or temp_market == '深交所' or temp_market == '深A':
        args_code = str(data[1]).replace(' ', '') + '.SZ'
    elif temp_market == '上海' or temp_market == '沪市' or temp_market == '1' or temp_market == '上交所' or temp_market == '沪A':
        args_code = str(data[1]).replace(' ', '') + '.SH'
    args = [args_code]
    query_datas = {
        "module": "pysec.etl.sec360.api.sec_api",
        "method": "query_sec",
        "args": args
    }
    query_result = post_data_job(query_datas)
    res_data = query_result['data']
    error_list = []
    if res_data:
        secu_id = res_data[0]['sec_id']
        etl_type = res_data[0]['sec_category']
        secu_type = get_secu_type(etl_type)
        data.append(secu_id, secu_type)
        sec_category = res_data[0]['sec_category']
        sec_type = None
        if sec_category == 'sec_stock':
            sec_type = 'AS'
        elif sec_category == 'sec_bond':
            sec_type = 'B'
        elif sec_category == 'sec_fund':
            sec_type = 'OF'
        # 注册到注册中心
        args_dict = {
            "sec_code_market": args_code,
            "sec_type": sec_type,
            "sec_name": boName.replace(' ', ''),
            "update_flag": 1
        }
        args_ = [args_dict]
        sync_datas = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "sync_sec",
            "args": args_
        }
        post_data_job(sync_datas)
        logger.info(f'该条已注册到注册中心{data}')
    else:
        error_list.append(data)


# 融资融券rate解析公共方法
def securities_rzrq_parsing_data(rs, biz_type, data_):
    logger.info(f'进入数据解析处理，biz_type={biz_type}')
    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), biz_type, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in data_:
            if len(i) == 5:
                insert_data_list.append([broker_id, i[3], i[4], biz_type, adjust_status_in, None, i[2], 1, 1, rs[1],
                                         forever_end_dt, None])
            else:
                logger.error(f'该条数据无证券id，请检查!{i}')
                invalid_data_list.append(i)
        insert_broker_mt_business_security(insert_data_list)
    else:
        logger.info(f'进入不为空的判断')
        for row in data_:
            if len(row) == 5:
                sec_code = row[0]
                sec_id = row[3]
                secu_type = row[4]
                round_rate = row[2]

                db_record = df_exists_index(result, sec_code, sec_id)
                if db_record is not None:
                    old_rate = db_record[6]
                    adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                    if adjust_status != adjust_status_invariant:
                        if adjust_status == adjust_status_high:
                            # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                        elif adjust_status == adjust_status_low:
                            # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                else:
                    insert_list = [[broker_id, sec_id, secu_type, biz_type, adjust_status_in, None, round_rate, 1, 1,
                                    datetime.datetime.now(), forever_end_dt, None]]
                    insert_broker_mt_business_security(insert_list)
            else:
                logger.error(f'该条数据无证券id，请检查!{row}')
                invalid_data_list.append(row)


# 集中度分组数据解析--可充抵保证金
def securities_stockgroup_parsing_data(rs, biz_type, stockgroup_data):
    error_list = []
    for data in stockgroup_data:
        boCode = data[1]
        boName = str(data[2]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷', '(4)').replace(
            '⑼', '(9)')
        data_dict = {"boCode": boCode, "boName": boName}
        res = get_securities_type_job(data_dict)
        if res:
            if len(res) == 1:
                secu_id = res[0]['boId']
                secu_type = res[0]['boIdType']
                data.append(secu_id, secu_type)
            elif len(res) > 1:
                for r in res:
                    if r['boName'] == boName:
                        secu_id = r['boId']
                        secu_type = r['boIdType']
                        data.append(secu_id, secu_type)
                        break
                    else:
                        continue
                # 说明没有查到证券id
                if len(data) == 4:
                    temp_deal(data, boName)
        else:
            temp_deal(data, boName)
        # logger.info(data)

    for tempp in stockgroup_data:
        if len(tempp) == 4:
            logger.error(f'该条记录无证券id{tempp}，需人工修复!')
            error_list.append(tempp)

    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), biz_type, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in stockgroup_data:
            if len(i) == 6:
                insert_data_list.append([broker_id, i[4], i[5], biz_type, adjust_status_in, None, i[3], 1, 1, rs[1],
                                         forever_end_dt, None])
            else:
                logger.error(f'该条数据无证券id，请检查!{i}')
                invalid_data_list.append(i)
                # raise Exception(f'该条数据无证券id，请检查!{i}')
        insert_broker_mt_business_security(insert_data_list)
    else:
        logger.info(f'进入不为空的判断')
        for row in stockgroup_data:
            if len(row) == 6:
                sec_code = row[1]
                sec_id = row[4]
                secu_type = row[5]
                round_rate = row[3]

                db_record = df_exists_index(result, sec_code, sec_id)
                if db_record is not None:
                    old_rate = db_record[6]
                    adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                    if adjust_status != adjust_status_invariant:
                        if adjust_status == adjust_status_high:
                            # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                        elif adjust_status == adjust_status_low:
                            # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status TODO
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            insert_broker_mt_business_security(insert_data_list)
                else:
                    insert_list = [[broker_id, sec_id, secu_type, biz_type, adjust_status_in, None, round_rate, 1, 1,
                                    datetime.datetime.now(), forever_end_dt, None]]
                    insert_broker_mt_business_security(insert_list)
            else:
                logger.error(f'该条数据无证券id，请检查!{row}')
                invalid_data_list.append(row)
