#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/27 15:41
# @Site    : 
# @File    : basehandler.py
# @Software: PyCharm
import datetime
import io
import json
import os
import time
from configparser import ConfigParser
from kafka.consumer import KafkaConsumer

import pandas as pd
import requests

from data.dao.biz.biz_data_deal import *
from data.dao.raw.raw_data_deal import *
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
request_url = cf.get('360etl-url', 'url')
request_url_zc_center = cf.get('zc-center-url', 'url')


# 数据解析基类
class BaseHandler(object):

    # 初始化
    def __init__(self, data_processor):
        self.data_processor = data_processor

    # 从mq接收消息
    @classmethod
    def kafka_mq_consumer(cls):
        consumer = KafkaConsumer(
            "collect_test",
            bootstrap_servers=["172.16.10.48:19092"], auto_offset_reset='earliest', group_id='my_group',
            consumer_timeout_ms=1000, enable_auto_commit=False
        )

        recv = None
        for msg in consumer:
            recv = msg.value.decode('unicode_escape')
            recv = recv[1:-1]
            recv_ = eval(recv)
            if recv_:
                cls.parsing_data_job(recv_)
            else:
                logger.error(f'消费消息失败，请检查{recv_}')
            time.sleep(5)
            # consumer.commit()

        print(recv)

    # 从采集平台查询数据
    def query_data_job(self):
        if self.data_processor == 'sh_market_mt_trading_collect' or 'sz_market_mt_trading_collect':
            total_result = select_sh_mt_trading_total_data()
            items_result = select_sh_mt_trading_items_data()
            total_log_id = total_result[0]
            total_date = total_result[1]
            total_data_type = total_result[2]
            total_data_source = total_result[3]
            # '["date", "rzye", "rzmre", "rjyl", "rjylje", "rjmcl", "rzrjye"]'
            total_text = eval(total_result[4])['data'][0]
            time = datetime.datetime.now()
            insert_data_process_controler(biz_id=total_log_id, data_processor='sh_market_mt_trading_collect'
                                          , data_type=total_data_type, data_source=total_data_source, data_status=1
                                          , last_process_bizdt=total_date, last_process_status=1,
                                          last_process_result='success')

            insert_data_process_log(biz_id=total_log_id, data_processor='sh_market_mt_trading_collect',
                                    data_type=total_data_type, data_source=total_data_source, record_num=10,
                                    data_status=1, last_process_bizdt=total_date, last_process_status=1,
                                    last_process_result='success')

            insert_exchange_mt_transactions_total(biz_dt=total_date, exchange_market='SSZ',
                                                  financing_balance=total_text[1],
                                                  financing_purchase_amount=total_text[2],
                                                  lending_securities_volume=total_text[3],
                                                  lending_securities_amount=total_text[4],
                                                  lending_securities_sales_volume=total_text[5],
                                                  margin_trading_balance=total_text[6], data_status=1,
                                                  creator_id=960529, updater_id=960529)

            # items_data_list = []
            # for i in total_result:
            #     total_data_list.append(eval(i[4])['data'][0])
            #
            # for i in items_result:
            #     items_data_list.append(eval(i[4])['data'][0])

    # 进行业务数据解析
    @classmethod
    def parsing_data_job(cls, data):
        if not data:
            raise Exception(f'数据解析失败，传入参数{data}为空!')
        # 从采集平台查询数据
        rs = select_collected_data(data['biz_dt'], data['data_type'], data['data_source'])
        if rs is None:
            raise Exception(f'数据采集平台未查询到对应数据{rs}，中止解析进行！')
        rs = rs[0]
        df_dict = eval(rs[4])
        data_ = df_dict['data']
        # insert_data_process_controler(rs[0], data['message'], rs[2], rs[3], 1, rs[1], 1, 'success')
        # logger.info(f'数据处理控制器表入库完成!')
        #
        # insert_data_process_log(rs[0], data['message'], rs[2], rs[3], len(rs), 1, rs[1], 1, 'success')
        # logger.info(f'数据处理日志表入库完成!')

        if data['data_type'] == '0':
            cls.exchange_items_deal(rs, data_)
        elif data['data_type'] == '1':
            cls.exchange_total_deal(data_, data['message'], rs)
        else:
            cls.securities_deal(rs, data_)
        # try:
        #
        #
        # except Exception as e:
        #     logger.error(e)

    @classmethod
    def securities_deal(cls, rs, data_):
        if not data_:
            raise Exception(f'业务解析失败，传入数据{data_}为空!')
        sec_code_list = []
        empty_data_list = []
        many_data_list = []
        for data in data_:
            sec_code = data[0]
            sec_name = data[1]
            if sec_code.startswith('0') or sec_code.startswith('3'):
                sec_code += '.SZ'
                data[0] = sec_code
                sec_code_list.append(data[0])
            elif sec_code.startswith('6'):
                sec_code += '.SH'
                data[0] = sec_code
                sec_code_list.append(data[0])
            elif sec_code.startswith('4') or sec_code.startswith('8'):
                sec_code += '.BJ'
                data[0] = sec_code
                sec_code_list.append(data[0])
            else:
                zc_data = {
                    "boCode": sec_code,
                    "boName": sec_name
                }
                result = cls.get_securities_type_job(zc_data)
                if result:
                    if len(result) == 1:
                        # 等于1说明能够精准匹配，大于需要后续人工处理数据问题
                        sec_code = result[0]['boIdCode']
                        data[0] = sec_code
                        sec_code_list.append(data[0])
                    elif len(result) == 0:
                        empty_data_list.append(zc_data)
                    elif len(result) > 1:
                        many_data_list.append(zc_data)
                else:
                    empty_data_list.append(zc_data)

        # empty_data_list,many_data_list为有误数据，需后期人工进行处理

        etl_datas = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "query_sec",
            "args": sec_code_list
        }

        etl_result = cls.post_data_job(etl_datas)
        sec_id_list = []
        if etl_result:
            for _result in etl_result['data']:
                if not _result['sec_id']:
                    raise Exception("{}没有对应的证券id,请人工确认！".format(_result['sec_code_market']))
                sec_id_list.append(_result['sec_id'])

        print(sec_id_list)
        print('---')






        # 调用注册中心接口，通过证券代码和证券名称查询证券类型然后调用etl360接口获取证券id
        pass

        # 待注册中心提供查询类型接口

    @classmethod
    def exchange_items_deal(cls, rs, data_):
        insert_exchange_mt_transactions_total(rs[1], 'SSZ', data_[0][1], data_[0][2], data_[0][3], data_[0][4]
                                              , data_[0][5], data_[0][6], 1, 414, 414)
        logger.info(f'交易市场融资融券交易总量表入库完成!')

    @classmethod
    def exchange_total_deal(cls, data_, message, rs):
        sql_data_list = []
        secu_code_list = []
        for _data in data_:
            if message == 'sh_market_mt_trading_collect':
                _secu_code = _data[1] + '.SH'
                secu_code_list.append(_secu_code)
                sql_data_list.append((_data[1], _secu_code, _data[2], rs[1], _data[3], _data[4],
                                      _data[5], _data[6], _data[7], _data[8], 1, 411, 411))
            elif message == 'sz_market_mt_trading_collect':
                _secu_code = _data[0] + '.SZ'
                secu_code_list.append(_secu_code)
                sql_data_list.append((_data[0], _secu_code, _data[1], rs[1], _data[2], _data[3],
                                      _data[4], _data[5], _data[6], _data[7], 1, 411, 411))
            else:
                _secu_code = _data[1]

        etl_datas = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "query_sec",
            "args": secu_code_list
        }
        etl_result = cls.post_data_job(etl_datas)
        sec_id_list = []
        if etl_result:
            for _result in etl_result['data']:
                if not _result['sec_id']:
                    raise Exception("{}没有对应的证券id,请人工确认！".format(_result['sec_code_market']))
                sec_id_list.append(_result['sec_id'])
            # print(f'sec_id_list:{sec_id_list}')

        if len(sql_data_list) == len(sec_id_list):
            _sql_data_list = []
            for _list in sql_data_list:
                list_ = list(_list)
                _sql_data_list.append(list_)

            if _sql_data_list:
                for j in range(0, len(_sql_data_list)):
                    for i in range(0, len(sec_id_list)):
                        if j == i:
                            _sql_data_list[i][0] = sec_id_list[j]

                insert_exchange_mt_transactions_items(_sql_data_list)
                # insert_exchange_mt_transactions_items(_data[1], _secu_code, _data[2], rs[1], _data[3],
                #                                       _data[4], _data[5], _data[6], _data[7], _data[8],
                #                                       1, 411, 411)
                logger.info(f'交易市场融资融券交易交易明细表入库完成!')

    # 调用360etl接口
    @classmethod
    def post_data_job(cls, data):
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
    @classmethod
    def get_securities_type_job(cls, data):
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

    # 解析后的业务数据入库
    @classmethod
    def insert_biz_data(cls, data):
        pass


if __name__ == '__main__':
    query_param = 'sh_market_mt_trading_collect'
    bs = BaseHandler(query_param)
    # bs.query_data_job()

    bs.kafka_mq_consumer()

    # bs.post_data_job()
    # data = {
    #     "boCode": "561550",
    #     "boName": "500指增"
    # }
    # rs = bs.get_securities_type_job(data)

