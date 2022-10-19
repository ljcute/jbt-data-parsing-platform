#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/7/27 15:41
# @Site    : 
# @File    : basehandler.py
# @Software: PyCharm
import json
import os
import sys
import traceback

import pandas

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
# sys.path.append(r'D:\jbt-data-parsing-platform')

from kafka import KafkaConsumer
from data.dao.biz.biz_data_deal import *
from data.dao.raw.raw_data_deal import *
from data.ms.genralhandler import post_data_job, sec_code_rules_match
from data.ms.securities.cc_securities_parsing import cc_parsing_data
from data.ms.securities.cj_securities_parsing import cj_parsing_data
from data.ms.securities.ct_securities_parsing import ct_parsing_data
from data.ms.securities.dfcf_securities_parsing import dfcf_parsing_data
from data.ms.securities.dx_securities_parsing import dx_parsing_data
from data.ms.securities.gd_securities_parsing import gd_parsing_data
from data.ms.securities.gf_securities_parsing import gf_parsing_data
from data.ms.securities.gtja_securities_parsing import gtja_parsing_data
from data.ms.securities.gx_securities_parsing import gx_parsing_data
from data.ms.securities.gy_securities_parsing import gy_parsing_data
from data.ms.securities.ht_securities_parsing import ht_parsing_data
from data.ms.securities.sw_securities_parsing import sw_parsing_data
from data.ms.securities.xy_securities_parsing import xy_parsing_data
from data.ms.securities.yh_securities_parsing import yh_parsing_data
from data.ms.securities.zs_securities_parsing import zs_parsing_data
from data.ms.securities.zt_securities_parsing import zt_parsing_data
from data.ms.securities.zx_securities_parsing import zx_parsing_data
from data.ms.securities.zxjt_securities_parsing import zxjt_parsing_data
from data.ms.securities.ax_securities_parsing import ax_parsing_data
from data.ms.securities.hait_securities_parsing import hait_parsing_data
from data.ms.securities.zjcf_securities_parsing import zjcf_parsing_data
from data.ms.sh.sh_market_mt_trading_parsing import sh_parsing_data, query_normal_rate
from data.ms.sz.sz_market_mt_trading_parsing import sz_parsing_data
from utils.logs_utils import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
kafkaList = cf.get('kafka', 'kafkaList')
Topic = cf.get('kafka', 'topic')
Group = cf.get('kafka', 'group')

biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券"
    , 4: "融资标的证券", 5: "融券标的证券", 99: "融资融券可充抵保证金证券和融资融券标的证券"}


# 数据解析基类
class BaseHandler(object):

    # 从mq接收消息
    @classmethod
    def kafka_mq_consumer(cls):
        consumer = KafkaConsumer(
            Topic,
            bootstrap_servers=kafkaList, auto_offset_reset='earliest', group_id=Group,
            consumer_timeout_ms=1000, enable_auto_commit=False, max_poll_interval_ms=86400000
        )
        logger.info(f'============================')
        logger.info(f'数据解析开始......')
        while True:
            for msg in consumer:
                try:
                    recv = msg.value.decode('unicode_escape')
                    recv = recv[1:-1]
                    recv_ = eval(recv)
                    mq_content = {'biz_dt': recv_['biz_dt'], 'data_type': recv_['data_type'],
                                  'data_source': recv_['data_source'], 'message': recv_['message']}
                    data_source_info = recv_['data_source']
                    data_type_info = recv_['data_type']
                    biz_dt_info = recv_['biz_dt']
                    logger.info(f'此次消费的消息内容为：{mq_content}')
                    if recv_:
                        cls.parsing_data_job(recv_)
                    else:
                        logger.error(f'消费消息失败，请检查{mq_content}')
                    consumer.commit()
                    logger.info('此次消息已消费完成!')
                    time.sleep(5)
                except Exception as es:
                    logger.error(
                        f'{data_source_info}的{biz_type_map.get(data_type_info)}解析任务失败，biz_dt为：{biz_dt_info}，请检查！具体异常信息为：{traceback.format_exc()}，Exception:{es}')

    # 进行业务数据解析
    @classmethod
    def parsing_data_job(cls, data):
        global null
        null = '-'
        if data:
            # 从采集平台查询数据
            rs = select_collected_data(str(data['biz_dt']).replace('-', ''), data['data_type'], data['data_source'])
            data_source_info = data['data_source']
            data_type_info = data['data_type']
            biz_dt_info = str(data['biz_dt']).replace('-', '')
            if rs:
                start_dt = datetime.datetime.now()
                data_ = None
                if data['data_source'] == '财通证券':
                    data_ = eval((rs[0][4]).replace("null", "999"))['data']
                elif data['data_source'] == '华泰证券':
                    if data_type_info == 2:
                        data_ = eval(rs[0][4])
                        pd = pandas.DataFrame(data_)
                        pd.sort_values(by=["exchangeType", "stockCode", "stockName", "assureRatio"], ascending=[True, True, True, True])
                        dep_data = pd.duplicated(["exchangeType", "stockCode"]).sum()
                        dep_line = pd[pd.duplicated(["exchangeType", "stockCode"], keep='last')]  # 查看删除重复的行
                        dep_list = dep_line.values.tolist()
                        warn_list = []
                        for i in dep_list:
                            warn_list.append(i[3])
                        if warn_list:
                            logger.warning(
                                f'{biz_dt_info}-{data_source_info}的{biz_type_map.get(data_type_info)}解析中包含{dep_data}条重复数据，具体证券代码如下：{list(warn_list)},请业务人员核对！')
                        # pd.sort_index(axis=0, ascending=True, inplace=True)
                        pd.drop_duplicates(subset=["exchangeType", "stockCode"], keep='first', inplace=True, ignore_index=False)
                        data_ = pd.to_json(orient="records", force_ascii=False)
                        data_ = eval(data_)
                    elif data_type_info == 3:
                        data_ = eval(rs[0][4])
                        # data_ = eval(rs[0][4])
                        # pd = pandas.DataFrame(data_)
                        # pd.sort_values(by=["exchangeType", "stockCode", "stockName", "finRatio", "sloRatio"],
                        #                ascending=[True, True, True, True,True])
                        # dep_data = pd.duplicated(["exchangeType", "stockCode"]).sum()
                        # dep_line = pd[pd.duplicated(["exchangeType", "stockCode"], keep='last')]  # 查看删除重复的行
                        # dep_list = dep_line.values.tolist()
                        # warn_list = []
                        # for i in dep_list:
                        #     warn_list.append(i[3])
                        # if warn_list:
                        #     logger.warning(
                        #         f'{biz_dt_info}-{data_source_info}的{biz_type_map.get(data_type_info)}解析中包含{dep_data}条重复数据，具体证券代码如下：{list(set(warn_list))},请业务人员核对！')
                        # # pd.sort_index(axis=0, ascending=True, inplace=True)
                        # pd.drop_duplicates(subset=["exchangeType", "stockCode"], keep='first', inplace=True,
                        #                    ignore_index=False)
                        # data_ = pd.to_json(orient="records", force_ascii=False)
                        # data_ = eval(data_)
                elif data['data_source'] == '申万宏源':
                    data_ = eval(rs[0][4])
                    pd = pandas.DataFrame(data_)
                    pd.sort_values(by=["0", "1", "2"], ascending=[True, True, True])
                    dep_data = pd.duplicated(["0", "1", "2"]).sum()
                    dep_line = pd[pd.duplicated(["0", "1", "2"], keep='last')]  # 查看删除重复的行
                    dep_list = dep_line.values.tolist()
                    warn_list = []
                    for i in dep_list:
                        warn_list.append(i[1])
                    if warn_list:
                        logger.warning(
                            f'{biz_dt_info}-{data_source_info}的{biz_type_map.get(data_type_info)}解析中包含{dep_data}条重复数据，具体证券代码如下：{list(warn_list)},请业务人员核对！')
                    # pd.sort_index(axis=0, ascending=True, inplace=True)
                    pd.drop_duplicates(subset=["0", "1"], keep='first', inplace=True, ignore_index=False)
                    data_ = pd.to_json(orient="records", force_ascii=False)
                    data_ = eval(data_)
                elif data['data_source'] == '中国银河':
                    data_ = eval(rs[0][4])
                    pd = pandas.DataFrame(data_)
                    pd.sort_values(by=["0"], ascending=[True])
                    dep_data = pd.duplicated(["0"]).sum()
                    dep_line = pd[pd.duplicated(["0"], keep='last')]  # 查看删除重复的行
                    dep_list = dep_line.values.tolist()
                    warn_list = []
                    for i in dep_list:
                        warn_list.append(i[1])
                    if warn_list:
                        logger.warning(
                            f'{biz_dt_info}-{data_source_info}的{biz_type_map.get(data_type_info)}解析中包含{dep_data}条重复数据，具体证券代码如下：{list(warn_list)},请业务人员核对！')
                    # pd.sort_index(axis=0, ascending=True, inplace=True)
                    pd.drop_duplicates(subset=["0"], keep='first', inplace=True, ignore_index=False)
                    data_ = pd.to_json(orient="records", force_ascii=False)
                    data_ = eval(data_)
                else:
                    data_ = eval(rs[0][4])
                insert_data_process_controler(rs[0][0], data['message'], rs[0][2], rs[0][3], 1, rs[0][1], 1, 'success')
                logger.info(f'数据处理控制器表入库完成!')

                if str(data['data_type']) == '0':
                    cls.exchange_items_deal(rs[0], data_)
                elif str(data['data_type']) == '1':
                    cls.exchange_total_deal(data_, data['message'], rs[0])
                else:
                    cls.securities_deal(rs[0], data_)

                end_dt = datetime.datetime.now()
                used_time = (end_dt - start_dt).seconds

                insert_data_process_log(rs[0][0], data['message'], rs[0][2], rs[0][3], start_dt, end_dt, used_time,
                                        len(data_),
                                        1, rs[0][1], 1, 'success')
                logger.info(f'数据处理日志表入库完成!')
            else:
                logger.info(f'数据采集平台未查询到{data["data_source"]}对应数据，中止解析进行！')
                # logger.error(f'数据采集平台未查询到{data["data_source"]}对应数据，中止解析进行！')
                # raise Exception(f'数据采集平台未查询到{data["data_source"]}对应采集数据，中止解析进行！')
        else:
            logger.error(f'数据解析失败，mq传入参数{data}为空!')
            raise Exception(f'数据解析失败，mq传入参数{data}为空!')

    @classmethod
    def exchange_items_deal(cls, rs, data_):
        exchange_market = None
        if rs[3] == '上海交易所':
            exchange_market = 'SSE'
            insert_exchange_mt_transactions_total(rs[1], exchange_market, data_[0]['1'], data_[0]['2'], data_[0]['3'],
                                                  data_[0]['4']
                                                  , data_[0]['5'], data_[0]['6'], 1, 414, 414)
        elif rs[3] == '深圳交易所':
            exchange_market = 'SZSE'
            insert_exchange_mt_transactions_total(rs[1], exchange_market, data_[0]['0'], data_[0]['1'], data_[0]['2'],
                                                  data_[0]['3']
                                                  , data_[0]['4'], data_[0]['5'], 1, 414, 414)
        logger.info(f'交易市场融资融券交易总量表入库完成!')

    @classmethod
    def exchange_total_deal(cls, data_, message, rs):
        sql_data_list = []
        secu_code_list = []
        for _data in data_:
            if message == 'sh_market_mt_trading_collect':
                _secu_code = _data['1'] + '.SH'
                secu_code_list.append(_secu_code)
                sql_data_list.append((_data['1'], _secu_code, _data['2'], rs[1], _data['3'], _data['4'],
                                      _data['5'], _data['6'], _data['7'], _data['8'], 1, 411, 411))
            elif message == 'sz_market_mt_trading_collect':
                _secu_code = _data['0'] + '.SZ'
                secu_code_list.append(_secu_code)
                sql_data_list.append((_data['0'], _secu_code, _data['1'], rs[1], _data['2'], _data['3'],
                                      _data['4'], _data['5'], _data['6'], _data['7'], 1, 411, 411))
            else:
                _secu_code = _data['1']

        etl_datas = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "query_sec",
            "args": secu_code_list
        }
        etl_result = post_data_job(etl_datas)
        sec_id_list = []
        if etl_result:
            for _result in etl_result['data']:
                if not _result['sec_id']:
                    raise Exception("{}没有对应的证券id,请人工确认！".format(_result['sec_code_market']))
                sec_id_list.append(_result['sec_id'])

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

    @classmethod
    def securities_deal(cls, rs, data_):
        if rs[3] == '上海交易所':
            sh_parsing_data(rs, data_)
        elif rs[3] == '深圳交易所':
            sz_parsing_data(rs, data_)
        elif rs[3] == '中信证券':
            zx_parsing_data(rs, data_)
        elif rs[3] == '华泰证券':
            ht_parsing_data(rs, data_)
            # elif rs[3] == '财通证券':
            #     ct_parsing_data(rs, data_)
            # elif rs[3] == '东方财富':
            #     dfcf_parsing_data(rs, data_)
            # elif rs[3] == '长城证券':
            #     cc_parsing_data(rs, data_)
            # elif rs[3] == '长江证券':
            # cj_parsing_data(rs, data_)
        # elif rs[3] == '东兴证券':
        #     dx_parsing_data(rs, data_)
        elif rs[3] == '国泰君安':
            gtja_parsing_data(rs, data_)
        elif rs[3] == '中国银河':
            yh_parsing_data(rs, data_)
        elif rs[3] == '申万宏源':
            sw_parsing_data(rs, data_)
        elif rs[3] == '广发证券':
            gf_parsing_data(rs, data_)
        elif rs[3] == '招商证券':
            zs_parsing_data(rs, data_)
        elif rs[3] == '国信证券':
            gx_parsing_data(rs, data_)
        # elif rs[3] == '光大证券':
        #     gd_parsing_data(rs, data_)
        # elif rs[3] == '中泰证券':
        #     zt_parsing_data(rs, data_)
        elif rs[3] == '兴业证券':
            xy_parsing_data(rs, data_)
        elif rs[3] == '国元证券':
            gy_parsing_data(rs, data_)
        elif rs[3] == '中信建投':
            zxjt_parsing_data(rs, data_)
        elif rs[3] == '安信证券':
            ax_parsing_data(rs, data_)
        elif rs[3] == '中金财富':
            zjcf_parsing_data(rs, data_)
        # elif rs[3] == '海通证券':
        #     hait_parsing_data(rs, data_)


if __name__ == '__main__':
    bs = BaseHandler()
    bs.kafka_mq_consumer()
