#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
@ss          :互联网业务数据解析APP
"""
import math
import os
import re
import sys
from datetime import datetime, timedelta


import pandas as pd
from io import StringIO


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from config import Config
from database import MysqlClient
from util.logs_utils import logger
from data.ms.base_tools import get_df_from_cdata, match_sid_by_code_and_name, code_ref_id, \
    get_exchange_discount_limit_rate, next_trading_day
from data.ms.InternetBizDataParsingApp import get_last_exchange_collect_date, handle_range_collected_data


cfg = Config.get_cfg()
db_biz_pro = MysqlClient(**cfg.get_content(f'pro_db_biz'))
db_raw_pro = MysqlClient(**cfg.get_content(f'pro_db_raw'))


def get_last_exchange_collect_date(days):
    sql = f"""
    select data_source, data_type, biz_dt
      from t_ndc_data_collect_log 
     where data_source in ('上海交易所', '深圳交易所', '北京交易所')
       and data_type = 2
       and data_status = 1
  group by data_source, data_type, biz_dt
  order by biz_dt desc
  limit {3*days}
    """
    return db_raw_pro.select(sql)


def get_adjust_data(biz_dt):
    sql = f"""
    SELECT broker_id, 
    (case biz_type when 1 then '融资标的' when 2 then '融券标的' when 3 then '担保券' when 4 then '集中度' else biz_type end) as data_type, 
    adjust_type, data_desc,
    (case adjust_type when 1 then '调入' when 2 then '调出' when 3 then '调高' when 4 then '调低' else adjust_type end) as adjust_type_cn, 
    count(*) as adjust_num
    FROM t_broker_mt_business_security 
    WHERE data_status=1 and start_dt <= '{biz_dt} 00:00:00' and end_dt > '{biz_dt} 00:00:00'
     and biz_type in (1,2,3) and start_dt='{biz_dt} 00:00:00'
    GROUP BY broker_id, biz_type, adjust_type, data_desc
    """
    return db_biz_pro.select(sql)


def get_collected_data(biz_dt):
    # if data_source == '交易所':
    #     data_source_str = f" in ('深圳交易所', '上海交易所', '北京交易所')"
    # else:
    #     data_source_str = f" = '{data_source}'"
    sql = f"""
    select data_source, data_type, data_text, biz_dt, log_id
      from t_ndc_data_collect_log 
     where log_id in (select max(log_id) 
                       from t_ndc_data_collect_log 
                      where biz_dt=(select max(biz_dt) from t_ndc_data_collect_log where data_status=1 and biz_dt <= '{biz_dt}')
                        and data_status=1
                      group by data_source, data_type)
    """
    return db_raw_pro.select(sql)


def get_pre_collected_data(biz_dt):
    # if data_source == '交易所':
    #     data_source_str = f" in ('深圳交易所', '上海交易所', '北京交易所')"
    # else:
    #     data_source_str = f" = '{data_source}'"
    sql = f"""
    select data_source, data_type, data_text as pre_data_text, biz_dt as pre_biz_dt, log_id as pre_log_id
      from t_ndc_data_collect_log 
     where log_id in (select max(log_id) 
                       from t_ndc_data_collect_log 
                      where biz_dt= (select max(biz_dt) from t_ndc_data_collect_log where data_status=1 and biz_dt<(select max(biz_dt) from t_ndc_data_collect_log where data_status=1 and biz_dt <= '{biz_dt}'))
                        and data_status=1
                      group by data_source, data_type)
    """
    return db_raw_pro.select(sql)


def get_security_data():
    sql = f"""
        select broker_id, broker_code, broker_name, order_no from t_security_broker
    """
    return db_biz_pro.select(sql)


def get_message(_adjust, row, biz_dt):
    _in = _adjust.loc[_adjust['adjust_type'] == 1]
    _out = _adjust.loc[_adjust['adjust_type'] == 2]
    _up = _adjust.loc[_adjust['adjust_type'] == 3]
    _down = _adjust.loc[_adjust['adjust_type'] == 4]
    _in_size = 0
    _out_size = 0
    _up_size = 0
    _down_size = 0
    if not _in.empty:
        _in_size = _in['adjust_num'].tolist()[0]
    if not _out.empty:
        _out_size = _out['adjust_num'].tolist()[0]
    if not _up.empty:
        _up_size = _up['adjust_num'].tolist()[0]
    if not _down.empty:
        _down_size = _down['adjust_num'].tolist()[0]

    parsing_temp_list = [row['broker_name'], _adjust['biz_type'].to_list()[0], biz_dt, '解析业务数据', _in_size, _out_size,
                         _up_size, _down_size]
    return parsing_temp_list


def handle_cmp(biz_dt):
    # 启动应用跑交易所数据，填充共享内存证券代码与对象ID内容
    exchange_df = get_last_exchange_collect_date(2).sort_values(by='biz_dt', axis=0, ascending=True)
    for index, row in exchange_df.iterrows():
        handle_range_collected_data(row['data_source'], row['data_type'], row['biz_dt'], persist_flag=False)
    logger.info(f'开始当日采集数据提取---')
    logs = get_collected_data(biz_dt[:10])
    biz_dt = str(logs['biz_dt'].tolist()[0])
    logger.info(f'结束当日采集数据提取---real biz_dt={biz_dt}')
    logger.info(f'开始前日采集数据提取---')
    pre_logs = get_pre_collected_data(biz_dt[:10])
    logger.info(f'结束前日采集数据提取---')
    if pre_logs.empty:
        logger.info(f"pre_logs is empty，biz_dt={biz_dt}")
    # 合并
    union = logs.merge(pre_logs, on=['data_source', 'data_type'])
    db_union = union.loc[(union['data_type'] == '2') | (union['data_type'] == '99')].copy()
    logger.info(f'开始核验担保券数据---')
    db_collect_df = db_handle(biz_dt, db_union)
    logger.info(f'担保券数据核验结束---')

    rz_union = union.loc[
        (union['data_type'] == '3') | (union['data_type'] == '4') | (union['data_type'] == '99')].copy()
    logger.info(f'开始核验融资标的券数据---')
    rz_collect_df = rz_handle(biz_dt, rz_union)
    logger.info(f'融资标的券数据核验结束---')

    rq_union = union.loc[
        (union['data_type'] == '3') | (union['data_type'] == '5') | (union['data_type'] == '99')].copy()
    logger.info(f'开始核验融券标的券数据---')
    rq_collect_df = rq_handle(biz_dt, rq_union)
    logger.info(f'融券标的券数据核验结束---')
    logger.info(f'数据对比开始---')
    # 采集数据df
    df_collect = pd.concat([db_collect_df, rz_collect_df, rq_collect_df], ignore_index=False)
    # 券商配置表df
    df_broker = get_security_data()
    # 解析数据df
    _df_parsing = get_adjust_data(biz_dt)
    _df_parsing = pd.merge(_df_parsing, df_broker, how='left', on='broker_id')
    _df_parsing['data_desc'] = _df_parsing['data_desc'].astype(str)
    _df_parsing['data_source'] = _df_parsing['broker_name']
    _df_parsing['data_source'] = (_df_parsing['data_desc'] == '1').replace({True: '深圳', False: ''}) + _df_parsing['data_source']
    _df_parsing['data_source'] = (_df_parsing['data_desc'] == '2').replace({True: '上海', False: ''}) + _df_parsing['data_source']
    _df_parsing['data_source'] = (_df_parsing['data_desc'] == '3').replace({True: '北京', False: ''}) + _df_parsing['data_source']
    _df_parsing['adjust_type'] = _df_parsing['adjust_type'].replace(1, 'p_in').replace(2, 'p_out').replace(3, 'p_up').replace(4, 'p_down')
    df_parsing = _df_parsing.pivot_table(index=['data_source', 'data_type'], columns='adjust_type', values='adjust_num')
    df_parsing = df_parsing.reset_index()
    df_parsing.fillna(0, inplace=True)
    exchange_rows = df_broker[df_broker['broker_name'] == '交易所']
    sse_rows = exchange_rows.copy()
    szse_rows = exchange_rows.copy()
    bse_rows = exchange_rows.copy()
    sse_rows['broker_name'] = '上海交易所'
    szse_rows['broker_name'] = '深圳交易所'
    bse_rows['broker_name'] = '北京交易所'
    df_broker = pd.concat([sse_rows, szse_rows, bse_rows, df_broker[df_broker['broker_name'] != '交易所']])
    df_collect.rename(columns={'in': 'c_in', 'out': 'c_out', 'up': 'c_up', 'down': 'c_down'}, inplace=True)
    df_collect = df_collect[['data_source', 'data_type', 'biz_dt', 'c_in', 'c_out', 'c_up', 'c_down']]
    df_tmp = pd.merge(df_collect, df_parsing, how='left', on=['data_source', 'data_type'])
    df_tmp.fillna(0, inplace=True)
    df_tmp['c_in'] = df_tmp['c_in'].astype(int)
    df_tmp['p_in'] = df_tmp['p_in'].astype(int)
    df_tmp['c_out'] = df_tmp['c_out'].astype(int)
    df_tmp['p_out'] = df_tmp['p_out'].astype(int)
    df_tmp['c_up'] = df_tmp['c_up'].astype(int)
    df_tmp['p_up'] = df_tmp['p_up'].astype(int)
    df_tmp['c_down'] = df_tmp['c_down'].astype(int)
    df_tmp['p_down'] = df_tmp['p_down'].astype(int)
    df_tmp['告警状态'] = (df_tmp['c_in'] == df_tmp['p_in']) & (df_tmp['c_out'] == df_tmp['p_out']) & (df_tmp['c_up'] == df_tmp['p_up']) & (df_tmp['c_down'] == df_tmp['p_down'])
    df_tmp['告警状态'] = df_tmp['告警状态'].replace({True: '正常', False: '告警'})
    df_tmp['in'] = df_tmp['c_in'].astype(str) + '-' + df_tmp['p_in'].astype(str) + (df_tmp['c_in'] == df_tmp['p_in']).replace({True: '', False: '(告警)'})
    df_tmp['out'] = df_tmp['c_out'].astype(str) + '-' + df_tmp['p_out'].astype(str) + (df_tmp['c_out'] == df_tmp['p_out']).replace({True: '', False: '(告警)'})
    df_tmp['up'] = df_tmp['c_up'].astype(str) + '-' + df_tmp['p_up'].astype(str) + (df_tmp['c_up'] == df_tmp['p_up']).replace({True: '', False: '(告警)'})
    df_tmp['down'] = df_tmp['c_down'].astype(str) + '-' + df_tmp['p_down'].astype(str) + (df_tmp['c_down'] == df_tmp['p_down']).replace({True: '', False: '(告警)'})
    # 返回结果df合并排序
    df_tmp = pd.merge(df_tmp, df_broker, how='left', left_on='data_source', right_on='broker_name')
    df_tmp['broker_id'] = df_tmp['broker_id'].astype(int)
    df_tmp['order_no'] = df_tmp['order_no'].astype(int)
    df_result = df_tmp[['biz_dt', 'broker_id', 'broker_code', 'broker_name', 'order_no', 'data_type', 'in', 'out', 'up', 'down', '告警状态']]
    df_result = df_result.sort_values(by=['告警状态', 'order_no', 'broker_name', 'data_type'], ascending=True)
    df_result.reset_index(inplace=True, drop=True)
    df_result.rename(columns={'biz_dt': '数据日期', 'broker_id': '机构ID', 'broker_code': '机构代码', 'broker_name': '机构名称', 'order_no': '排名', 'data_type': '业务类型',
                              'in': '调入[采-解]', 'out': '调出[采-解]', 'up': '调高[采-解]', 'down': '调低[采-解]', '告警状态': '解析告警状态'}, inplace=True)
    logger.info(f'数据对比结束---')
    # df_result.to_csv('data.csv',index=False)
    # print(df_result)
    return df_result


def rq_handle(biz_dt, union):
    _message = []
    for idx, rw in union.iterrows():
        try:
            _data_source = rw['data_source']
            _data_type = int(rw['data_type'])
            if _data_type not in (3, 5, 99):
                continue
            pre = pd.read_csv(StringIO(rw['pre_data_text']), sep=",")
            cur = pd.read_csv(StringIO(rw['data_text']), sep=",")
            # 对比：每家不一样
            if _data_source in ('上海交易所', '深圳交易所', '北京交易所'):
                pre['证券代码'] = pre['证券代码'].astype('str')
                cur['证券代码'] = cur['证券代码'].astype('str')
                pre['key'] = pre['证券代码']
                cur['key'] = cur['证券代码']
                pre['pre_rate'] = 50
                cur['cur_rate'] = 50
            elif _data_source in ('华泰证券',):
                pre = pre.loc[pre['sloStatus'] == 0].copy()
                cur = cur.loc[cur['sloStatus'] == 0].copy()
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['exchangeType'].map(
                                 lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(
                                     x) == 9 else str(x))
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['exchangeType'].map(
                                 lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(
                                     x) == 9 else str(x))
                pre['pre_rate'] = pre['sloRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['sloRatio'].apply(lambda x: int(x * 100))
            elif _data_source in ('国元证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rq_ratio'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rq_ratio'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('广发证券',):
                pre['key'] = pre['0'].apply(lambda x: (str(x))[-6:])
                cur['key'] = cur['0'].apply(lambda x: (str(x))[-6:])
                pre['pre_rate'] = pre['1'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['1'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('安信证券',):
                pre = pre.loc[pre['enableSl'] == '可以融券'].copy()
                cur = cur.loc[cur['enableSl'] == '可以融券'].copy()
                pre['key'] = pre['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['market'].map(
                                 lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(
                                     x) == '3' else str(x))
                cur['key'] = cur['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['market'].map(
                                 lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(
                                     x) == '3' else str(x))
                pre['pre_rate'] = pre['slMarginRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['slMarginRatio'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('中信证券',):
                pre = pre.loc[pre['rqFloag'] == 1].copy()
                cur = cur.loc[cur['rqFloag'] == 1].copy()
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['exchangeCode'].map(
                                 lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(
                                     x) == '北京' else str(x))
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['exchangeCode'].map(
                                 lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(
                                     x) == '北京' else str(x))
                pre['pre_rate'] = pre['rqPercent'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rqPercent'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('国泰君安',):
                pre = pre.loc[pre['type'] == 2].copy()
                cur = cur.loc[cur['type'] == 2].copy()
                pre['key'] = pre['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                    x) == '北交所' else str(x))
                cur['key'] = cur['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                    x) == '北交所' else str(x))
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('中国银河',):
                pre = pre.loc[pre['type'] == 'rq'].copy()
                cur = cur.loc[cur['type'] == 'rq'].copy()
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('招商证券',):
                pre = pre.loc[pre['creditstkctrl'] == 0].copy()
                cur = cur.loc[cur['creditstkctrl'] == 0].copy()
                pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(
                        x))
                cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(
                        x))
                pre['pre_rate'] = pre['marginratestk'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['marginratestk'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('中信建投',):
                pre = pre.loc[pre['stkctrlflag'] == 0].copy()
                cur = cur.loc[cur['stkctrlflag'] == 0].copy()
                pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) in (
                        '2', 'B') else str(
                        x))
                cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) in (
                        '2', 'B') else str(
                        x))
                pre['pre_rate'] = pre['marginratestk'].apply(lambda x: int(x))
                cur['cur_rate'] = cur['marginratestk'].apply(lambda x: int(x))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('国信证券',):
                pre['key'] = pre['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(x))
                cur['key'] = cur['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(x))
                pre['pre_rate'] = pre['rzbzjbl'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rzbzjbl'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('兴业证券',):
                pre['key'] = pre['证券代码'].str.upper()
                cur['key'] = cur['证券代码'].str.upper()
                pre['pre_rate'] = pre['融券保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
                cur['cur_rate'] = cur['融券保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('申万宏源',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                pre['pre_rate'] = pre['融券保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                cur['cur_rate'] = cur['融券保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('中金财富',):
                pre = pre.loc[pre['stockTarget'] == 'Y'].copy()
                cur = cur.loc[cur['stockTarget'] == 'Y'].copy()
                pre['key'] = pre['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                cur['key'] = cur['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                pre['pre_rate'] = pre['guaranteeStock'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['guaranteeStock'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('中泰证券',):
                pre = pre.loc[(pre['STOCK_STATE'] == '可融资可融券') | (pre['STOCK_STATE'] == '禁止融资可融券')].copy()
                cur = cur.loc[(cur['STOCK_STATE'] == '可融资可融券') | (cur['STOCK_STATE'] == '禁止融资可融券')].copy()
                pre['key'] = pre['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'BOURSE'].str.strip()
                cur['key'] = cur['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'BOURSE'].str.strip()
                pre['pre_rate'] = pre['STOCK_RATIOS']
                cur['cur_rate'] = cur['STOCK_RATIOS']
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('光大证券',):
                pre = pre.loc[pre['融券标的'] == '是'].copy()
                cur = cur.loc[cur['融券标的'] == '是'].copy()
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                # 代码8、4开头，把市场修复为BJ
                pre['key'] = pre['key'].apply(lambda x: str(x)[:6] + '.BJ' if str(x)[:1] in ('8', '4') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                cur['key'] = cur['key'].apply(lambda x: str(x)[:6] + '.BJ' if str(x)[:1] in ('8', '4') else x)
                pre['pre_rate'] = 50
                cur['cur_rate'] = 50
            elif _data_source in ('海通证券',):
                pre = pre.loc[pre['ifBorrowStock'] == 'Y'].copy()
                cur = cur.loc[cur['ifBorrowStock'] == 'Y'].copy()
                pre['key'] = pre['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['marketCode'].map(
                                 lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                                     x) == '北交所' else str(x))
                cur['key'] = cur['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['marketCode'].map(
                                 lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                                     x) == '北交所' else str(x))
                pre['pre_rate'] = 50
                cur['cur_rate'] = 50
            elif _data_source in ('平安证券',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['融券比例'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['融券比例'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('中金公司',):
                pre = pre.loc[pre['iscreditsharestk'] == 'Y'].copy()
                cur = cur.loc[cur['iscreditsharestk'] == 'Y'].copy()

                pre['key'] = pre['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(
                        x) else str(x))
                cur['key'] = cur['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(
                        x) else str(x))
                pre['pre_rate'] = pre['csmarginrate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['csmarginrate'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('信达证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rqbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rqbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('东北证券',):
                pre = pre[:-1]
                cur = cur[:-1]
                pre['key'] = pre['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rq'].apply(lambda x: int(float(x)))
                cur['cur_rate'] = cur['rq'].apply(lambda x: int(float(x)))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('长江证券',):
                pre = pre.loc[pre['rqbd'] == '是'].copy()
                cur = cur.loc[cur['rqbd'] == '是'].copy()
                pre['key'] = pre['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                cur['key'] = cur['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                pre['pre_rate'] = pre['bail_ratio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['bail_ratio'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('东方财富',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                pre['sec_name'] = pre['证券简称']
                pre.sort_values(by=["key", "sec_name"], ascending=[True, True])
                pre.drop_duplicates(subset=["key", "sec_name"], keep='first', inplace=True, ignore_index=False)
                pre['pre_rate'] = pre['融券保证金比例'].apply(lambda x: float(str(x).replace('%', '')))

                cur['sec_name'] = cur['证券简称']
                cur.sort_values(by=["key", "sec_name"], ascending=[True, True])
                cur.drop_duplicates(subset=["key", "sec_name"], keep='first', inplace=True, ignore_index=False)
                cur['cur_rate'] = cur['融券保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            elif _data_source in ('东方证券',):
                pre = pre.loc[pre['financingtarget'] == '是'].copy()
                cur = cur.loc[cur['financingtarget'] == '是'].copy()
                pre['key'] = pre['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = 50
                cur['cur_rate'] = 50
            elif _data_source in ('方正证券',):
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['saleRate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['saleRate'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 50]
                cur = cur[cur['cur_rate'] >= 50]
            else:
                logger.warning(f"fix {_data_source}")
                continue
            _out = pre.loc[~pre['key'].isin(cur['key'].tolist())].copy()
            _in = cur.loc[~cur['key'].isin(pre['key'].tolist())].copy()
            _same = cur.merge(pre, on='key')
            _up = _same.loc[_same['pre_rate'] < _same['cur_rate']].copy()
            _down = _same.loc[_same['pre_rate'] > _same['cur_rate']].copy()
            temp_list = [rw['data_source'], '融券标的', biz_dt, '采集业务数据', _in.index.size, _out.index.size, _up.index.size,
                         _down.index.size]
            _message.append(temp_list)
            if not _out.empty:
                _out['adjust_type'] = 'out'
            if not _in.empty:
                _in['adjust_type'] = 'in'
            if not _up.empty:
                _up['adjust_type'] = 'up'
            if not _down.empty:
                _down['adjust_type'] = 'down'
        except Exception as err:
            logger.info(f"{rw['data_source']} ({rw['data_type']}) {err}")
    collect_df = pd.DataFrame(data=_message,
                              columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    return collect_df


def rz_handle(biz_dt, union):
    _message = []
    for idx, rw in union.iterrows():
        try:
            _data_source = rw['data_source']
            _data_type = int(rw['data_type'])
            if _data_type not in (3, 4, 99):
                continue
            pre = pd.read_csv(StringIO(rw['pre_data_text']), sep=",")
            cur = pd.read_csv(StringIO(rw['data_text']), sep=",")
            # 对比：每家不一样
            if _data_source in ('上海交易所', '深圳交易所', '北京交易所'):
                pre['证券代码'] = pre['证券代码'].astype('str')
                cur['证券代码'] = cur['证券代码'].astype('str')
                pre['key'] = pre['证券代码']
                cur['key'] = cur['证券代码']
                pre['pre_rate'] = 100
                cur['cur_rate'] = 100
            elif _data_source in ('华泰证券',):
                pre = pre.loc[pre['finStatus'] == 0].copy()
                cur = cur.loc[cur['finStatus'] == 0].copy()
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['exchangeType'].map(
                                 lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(
                                     x) == 9 else str(x))
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['exchangeType'].map(
                                 lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(
                                     x) == 9 else str(x))
                pre['pre_rate'] = pre['finRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['finRatio'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('国元证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rz_ratio'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rz_ratio'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('广发证券',):
                pre['key'] = pre['0'].apply(lambda x: (str(x))[-6:])
                cur['key'] = cur['0'].apply(lambda x: (str(x))[-6:])
                pre['pre_rate'] = pre['1'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['1'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('安信证券',):
                pre = pre.loc[pre['enableFi'] == '可以融资'].copy()
                cur = cur.loc[cur['enableFi'] == '可以融资'].copy()
                pre['key'] = pre['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['market'].map(
                                 lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(
                                     x) == '3' else str(x))
                cur['key'] = cur['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['market'].map(
                                 lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(
                                     x) == '3' else str(x))
                pre['pre_rate'] = pre['fiMarginRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['fiMarginRatio'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('中信证券',):
                pre = pre.loc[pre['rzFloag'] == 1].copy()
                cur = cur.loc[cur['rzFloag'] == 1].copy()
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['exchangeCode'].map(
                                 lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(
                                     x) == '北京' else str(x))
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['exchangeCode'].map(
                                 lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(
                                     x) == '北京' else str(x))
                pre['pre_rate'] = pre['rzPercent'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rzPercent'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('国泰君安',):
                pre = pre.loc[pre['type'] == 3].copy()
                cur = cur.loc[cur['type'] == 3].copy()
                pre['key'] = pre['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                    x) == '北交所' else str(x))
                cur['key'] = cur['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                    x) == '北交所' else str(x))
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('中国银河',):
                pre = pre.loc[pre['type'] == 'rz'].copy()
                cur = cur.loc[cur['type'] == 'rz'].copy()
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('招商证券',):
                pre = pre.loc[pre['creditfundctrl'] == 0].copy()
                cur = cur.loc[cur['creditfundctrl'] == 0].copy()
                pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(
                        x))
                cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(
                        x))
                pre['pre_rate'] = pre['marginratefund'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['marginratefund'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('中信建投',):
                pre = pre.loc[pre['fundctrlflag'] == 0].copy()
                cur = cur.loc[cur['fundctrlflag'] == 0].copy()
                pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) in (
                        '2', 'B') else str(
                        x))
                cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) in (
                        '2', 'B') else str(
                        x))
                pre['pre_rate'] = pre['marginratefund'].apply(lambda x: int(x))
                cur['cur_rate'] = cur['marginratefund'].apply(lambda x: int(x))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('国信证券',):
                pre['key'] = pre['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(
                        x))
                cur['key'] = cur['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(
                        x))
                pre['pre_rate'] = pre['rzbzjbl'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rzbzjbl'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('兴业证券',):
                pre['key'] = pre['证券代码'].str.upper()
                cur['key'] = cur['证券代码'].str.upper()
                pre['pre_rate'] = pre['融资保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
                cur['cur_rate'] = cur['融资保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('申万宏源',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                pre['pre_rate'] = pre['融资保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                cur['cur_rate'] = cur['融资保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('中金财富',):
                pre = pre.loc[pre['moneyTarget'] == 'Y'].copy()
                cur = cur.loc[cur['moneyTarget'] == 'Y'].copy()
                pre['key'] = pre['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                cur['key'] = cur['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                pre['pre_rate'] = pre['guaranteeMoney'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['guaranteeMoney'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('中泰证券',):
                pre = pre.loc[pre['STOCK_STATE'] == '可融资可融券'].copy()
                cur = cur.loc[cur['STOCK_STATE'] == '可融资可融券'].copy()
                pre['key'] = pre['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['BOURSE'].str.strip()
                cur['key'] = cur['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['BOURSE'].str.strip()
                pre['pre_rate'] = pre['FUND_RATIOS']
                cur['cur_rate'] = cur['FUND_RATIOS']
                pre['pre_rate'] = pre['pre_rate'].apply(lambda x: int(x))
                cur['cur_rate'] = cur['cur_rate'].apply(lambda x: int(x))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('光大证券',):
                pre = pre.loc[pre['融资标的'] == '是'].copy()
                cur = cur.loc[cur['融资标的'] == '是'].copy()
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                pre['key'] = pre['key'].apply(lambda x: str(x)[:6] + '.BJ' if str(x)[:1] in ('8', '4') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                # 代码8、4开头，把市场修复为BJ
                cur['key'] = cur['key'].apply(lambda x: str(x)[:6] + '.BJ' if str(x)[:1] in ('8', '4') else x)
                pre['pre_rate'] = 100
                cur['cur_rate'] = 100
            elif _data_source in ('海通证券',):
                pre = pre.loc[pre['ifFinancing'] == 'Y'].copy()
                cur = cur.loc[cur['ifFinancing'] == 'Y'].copy()
                pre['key'] = pre['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['marketCode'].map(
                                 lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                                     x) == '北交所' else str(x))
                cur['key'] = cur['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['marketCode'].map(
                                 lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                                     x) == '北交所' else str(x))
                pre['pre_rate'] = 100
                cur['cur_rate'] = 100
            elif _data_source in ('平安证券',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['融资比例'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['融资比例'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('中金公司',):
                pre = pre.loc[pre['iscreditcashstk'] == 'Y'].copy()
                cur = cur.loc[cur['iscreditcashstk'] == 'Y'].copy()

                pre['key'] = pre['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(
                        x) else str(x))
                cur['key'] = cur['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(
                        x) else str(x))
                pre['pre_rate'] = pre['ccmarginrate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['ccmarginrate'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('信达证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rzbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rzbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('东北证券',):
                pre = pre[:-1]
                cur = cur[:-1]
                pre['key'] = pre['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rz'].apply(lambda x: int(float(x)))
                cur['cur_rate'] = cur['rz'].apply(lambda x: int(float(x)))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('长江证券',):
                pre = pre.loc[pre['rzbd'] == '是'].copy()
                cur = cur.loc[cur['rzbd'] == '是'].copy()
                pre['key'] = pre['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                cur['key'] = cur['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                pre['pre_rate'] = pre['fin_ratio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['fin_ratio'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('东方财富',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                pre['sec_name'] = pre['证券简称']
                pre.sort_values(by=["key", "sec_name"], ascending=[True, True])
                pre.drop_duplicates(subset=["key", "sec_name"], keep='first', inplace=True, ignore_index=False)
                pre['pre_rate'] = pre['融资保证金比例'].apply(lambda x: float(str(x).replace('%', '')))

                cur['sec_name'] = cur['证券简称']
                cur.sort_values(by=["key", "sec_name"], ascending=[True, True])
                cur.drop_duplicates(subset=["key", "sec_name"], keep='first', inplace=True, ignore_index=False)
                cur['cur_rate'] = cur['融资保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            elif _data_source in ('东方证券',):
                pre = pre.loc[pre['margintradingtarget'] == '是'].copy()
                cur = cur.loc[cur['margintradingtarget'] == '是'].copy()
                pre['key'] = pre['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = 100
                cur['cur_rate'] = 100
            elif _data_source in ('方正证券',):
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['finRate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['finRate'].apply(lambda x: int(x * 100))
                pre = pre[pre['pre_rate'] >= 100]
                cur = cur[cur['cur_rate'] >= 100]
            else:
                logger.warning(f"fix {_data_source}")
                continue
            _out = pre.loc[~pre['key'].isin(cur['key'].tolist())].copy()
            _in = cur.loc[~cur['key'].isin(pre['key'].tolist())].copy()
            _same = cur.merge(pre, on='key')
            _up = _same.loc[_same['pre_rate'] < _same['cur_rate']].copy()
            _down = _same.loc[_same['pre_rate'] > _same['cur_rate']].copy()
            temp_list = [rw['data_source'], '融资标的', biz_dt, '采集业务数据', _in.index.size, _out.index.size, _up.index.size,
                         _down.index.size]
            _message.append(temp_list)
            if not _out.empty:
                _out['adjust_type'] = 'out'
            if not _in.empty:
                _in['adjust_type'] = 'in'
            if not _up.empty:
                _up['adjust_type'] = 'up'
            if not _down.empty:
                _down['adjust_type'] = 'down'
        except Exception as err:
            logger.info(f"{rw['data_source']} ({rw['data_type']}) {err}")
    collect_df = pd.DataFrame(data=_message,
                              columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    return collect_df


def db_handle(biz_dt, union):
    _message = []
    for idx, rw in union.iterrows():
        try:
            _data_source = rw['data_source']
            _data_type = int(rw['data_type'])
            if _data_type not in (2, 99):
                continue
            pre = pd.read_csv(StringIO(rw['pre_data_text']), sep=",")
            cur = pd.read_csv(StringIO(rw['data_text']), sep=",")
            # 对比：每家不一样
            if _data_source in ('上海交易所', '深圳交易所', '北京交易所'):
                market = ''
                if _data_source == '上海交易所':
                    market = 'SH'
                elif _data_source == '深圳交易所':
                    market = 'SZ'
                elif _data_source == '北京交易所':
                    market = 'BJ'
                pre['sec_code'] = pre['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):]) + '.' + market
                pre['sec_name'] = pre['证券简称'].str.replace(' ', '')
                pre['start_dt'] = None
                pre = code_ref_id(biz_dt, pre, _data_source, exchange=True)
                pre = get_exchange_discount_limit_rate(last_work_day(biz_dt), pre)
                pre = pre[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
                pre['key'] = pre['sec_code']
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(float(x)))
                cur['sec_code'] = cur['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):]) + '.' + market
                cur['sec_name'] = cur['证券简称'].str.replace(' ', '')
                cur['start_dt'] = None
                cur = code_ref_id(biz_dt, cur, _data_source, exchange=True)
                cur = get_exchange_discount_limit_rate(biz_dt, cur)
                cur = cur[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
                cur['key'] = cur['sec_code']
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(float(x)))
            elif _data_source in ('华泰证券',):
                pre = pre.loc[pre['assureStatus'] == 0].copy()
                cur = cur.loc[cur['assureStatus'] == 0].copy()
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['exchangeType'].map(
                                 lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(
                                     x) == 9 else str(x))
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['exchangeType'].map(
                                 lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(
                                     x) == 9 else str(x))
                pre['pre_rate'] = pre['assureRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['assureRatio'].apply(lambda x: int(x * 100))
            elif _data_source in ('国元证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['exchange_rate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['exchange_rate'].apply(lambda x: int(x * 100))
                pre['sec_code'] = pre['key']
                pre['sec_name'] = pre['secu_name']
                pre['market'] = pre['stkex'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
                pre = code_ref_id(biz_dt, pre, _data_source, exchange=True)
                pre['key'] = pre['sec_code']
                cur['sec_code'] = cur['key']
                cur['sec_name'] = cur['secu_name']
                cur['market'] = cur['stkex'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
                cur = code_ref_id(biz_dt, cur, _data_source, exchange=True)
                cur['key'] = cur['sec_code']
            elif _data_source in ('广发证券',):
                pre['key'] = pre['0'].apply(lambda x: (str(x))[-6:])
                cur['key'] = cur['0'].apply(lambda x: (str(x))[-6:])
                pre['pre_rate'] = pre['1'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['1'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('安信证券',):
                pre['key'] = pre['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['market'].map(
                                 lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(
                                     x) == '3' else str(x))
                cur['key'] = cur['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['market'].map(
                                 lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(
                                     x) == '3' else str(x))
                pre['pre_rate'] = pre['collatRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['collatRatio'].apply(lambda x: int(x * 100))
            elif _data_source in ('中信证券',):
                pre = pre.loc[pre['status'] == '正常'].copy()
                cur = cur.loc[cur['status'] == '正常'].copy()
                pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['exchangeCode'].map(
                                 lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(
                                     x) == '北京' else str(x))
                cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['exchangeCode'].map(
                                 lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(
                                     x) == '北京' else str(x))
                pre['pre_rate'] = pre['percent'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['percent'].apply(lambda x: int(x * 100))
            elif _data_source in ('国泰君安',):
                pre = pre.loc[pre['type'] == 1].copy()
                cur = cur.loc[cur['type'] == 1].copy()
                pre['key'] = pre['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                    x) == '北交所' else str(x))
                cur['key'] = cur['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                    x) == '北交所' else str(x))
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('中国银河',):
                pre = pre.loc[pre['type'] == 'db'].copy()
                cur = cur.loc[cur['type'] == 'db'].copy()
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(x * 100))
            elif _data_source in ('招商证券',):
                pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(
                        x))
                cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(
                        x))
                pre['pre_rate'] = pre['pledgerate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['pledgerate'].apply(lambda x: int(x * 100))
            elif _data_source in ('中信建投',):
                pre = pre.loc[~pre['pledgerate'].isna()].copy()
                cur = cur.loc[~cur['pledgerate'].isna()].copy()
                pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) in (
                        '2', 'B') else str(
                        x))
                cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'market'].map(
                    lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) in (
                        '2', 'B') else str(
                        x))
                pre['pre_rate'] = pre['pledgerate'].apply(lambda x: int(x))
                cur['cur_rate'] = cur['pledgerate'].apply(lambda x: int(x))
            elif _data_source in ('国信证券',):
                pre['key'] = pre['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['zsl'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['zsl'].apply(lambda x: int(x * 100))
            elif _data_source in ('兴业证券',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['折算率'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['折算率'].apply(lambda x: int(x * 100))
            elif _data_source in ('申万宏源',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                pre['pre_rate'] = pre['折算率'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                cur['cur_rate'] = cur['折算率'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
            elif _data_source in ('中金财富',):
                pre['key'] = pre['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                cur['key'] = cur['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                pre['pre_rate'] = pre['rate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(x * 100))
            elif _data_source in ('中泰证券',):
                pre['key'] = pre['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['BOURSE'].str.strip()
                cur['key'] = cur['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['BOURSE'].str.strip()
                pre['pre_rate'] = pre['REBATE']
                cur['cur_rate'] = cur['REBATE']
            elif _data_source in ('光大证券',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                pre['key'] = pre['key'].apply(lambda x: str(x)[:6] + '.BJ' if str(x)[:1] in ('8', '4') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                # 代码8、4开头，把市场修复为BJ
                cur['key'] = cur['key'].apply(lambda x: str(x)[:6] + '.BJ' if str(x)[:1] in ('8', '4') else x)
                pre['pre_rate'] = pre['调整后折算率'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['调整后折算率'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('海通证券',):
                pre = pre.loc[pre['ifGuarantee'] == 'Y'].copy()
                cur = cur.loc[cur['ifGuarantee'] == 'Y'].copy()
                pre['key'] = pre['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['marketCode'].map(
                                 lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                                     x) == '北交所' else str(x))
                cur['key'] = cur['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['marketCode'].map(
                                 lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(
                                     x) == '北交所' else str(x))
                pre['pre_rate'] = pre['stockConvertRate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['stockConvertRate'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('平安证券',):
                pre['sec_code'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['sec_name'] = pre['证券简称'].str.replace(' ', '')
                _pre = match_sid_by_code_and_name(next_trading_day(biz_dt), pre, _data_source)
                pre = pre.merge(_pre, on=['sec_code', 'sec_name'])
                pre.sort_values(by=["sec_code", "sec_name"], ascending=[True, True])
                pre.drop_duplicates(subset=["sec_code", "sec_name"], keep='first', inplace=True, ignore_index=False)
                pre['key'] = pre['sec_code']

                cur['sec_code'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['sec_name'] = cur['证券简称'].str.replace(' ', '')
                _cur = match_sid_by_code_and_name(next_trading_day(biz_dt), cur, _data_source)
                cur = cur.merge(_cur, on=['sec_code', 'sec_name'])
                cur.sort_values(by=["sec_code", "sec_name"], ascending=[True, True])
                cur.drop_duplicates(subset=["sec_code", "sec_name"], keep='first', inplace=True, ignore_index=False)
                cur['key'] = cur['sec_code']

                pre['pre_rate'] = pre['折算率'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['折算率'].apply(lambda x: int(x * 100))
            elif _data_source in ('中金公司',):
                pre = pre.loc[pre['iscmo'] == 'Y'].copy()
                cur = cur.loc[cur['iscmo'] == 'Y'].copy()
                pre['key'] = pre['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(x) else str(x))
                cur['key'] = cur['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(x) else str(x))
                pre['pre_rate'] = pre['cmorate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['cmorate'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('信达证券',):
                pre['sec_code'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['sec_name'] = pre['secu_name'].str.replace(' ', '')
                _pre = match_sid_by_code_and_name(next_trading_day(biz_dt), pre, _data_source)
                pre = pre.merge(_pre, on=['sec_code', 'sec_name'])
                pre['key'] = pre['scd']

                cur['sec_code'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['sec_name'] = cur['secu_name'].str.replace(' ', '')
                _cur = match_sid_by_code_and_name(next_trading_day(biz_dt), cur, _data_source)
                cur = cur.merge(_cur, on=['sec_code', 'sec_name'])
                cur['key'] = cur['scd']
                pre['pre_rate'] = pre['dbpzabl'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['dbpzabl'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('东北证券',):
                pre = pre[:-1]
                cur = cur[:-1]
                pre['key'] = pre['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                # not_six_digits = ~pre['bm'].apply(lambda x: bool(re.match('^\d{6}$', str(x))))
                # pre.drop(pre.loc[not_six_digits].index, inplace=True)
                # not_six_digits_ = ~cur['bm'].apply(lambda x: bool(re.match('^\d{6}$', str(x))))
                # cur.drop(cur.loc[not_six_digits_].index, inplace=True)
                pre['pre_rate'] = pre['zsl'].apply(lambda x: int(x))
                cur['cur_rate'] = cur['zsl'].apply(lambda x: int(x))
            elif _data_source in ('长江证券',):
                pre['key'] = pre['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                cur['key'] = cur['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                pre['pre_rate'] = pre['assure_ratio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['assure_ratio'].apply(lambda x: int(x * 100))
            elif _data_source in ('东方财富',):
                pre['sec_code'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(x))
                pre['sec_name'] = pre['证券简称']
                pre['start_dt'] = None
                pre['key'] = pre['sec_code']
                pre.sort_values(by=["key", "sec_name"], ascending=[True, True])
                pre.drop_duplicates(subset=["key", "sec_name"], keep='first', inplace=True, ignore_index=False)

                cur['sec_code'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(x))
                cur['sec_name'] = cur['证券简称']
                cur['start_dt'] = None
                cur['key'] = cur['sec_code']
                cur.sort_values(by=["key", "sec_name"], ascending=[True, True])
                cur.drop_duplicates(subset=["key", "sec_name"], keep='first', inplace=True, ignore_index=False)

                pre['pre_rate'] = pre['实际折算率'].apply(lambda x: float(str(x).replace('%', '')))
                cur['cur_rate'] = cur['实际折算率'].apply(lambda x: float(str(x).replace('%', '')))
            elif _data_source in ('东方证券',):
                pre['key'] = pre['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['convertrate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['convertrate'].apply(lambda x: int(x * 100))
            elif _data_source in ('方正证券',):
                pre['sec_code'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['sec_name'] = pre['stockName'].str.replace(' ', '')
                _pre = match_sid_by_code_and_name(next_trading_day(biz_dt), pre, _data_source)
                pre = pre.merge(_pre, on=['sec_code', 'sec_name'])
                pre['key'] = pre['sec_code']

                cur['sec_code'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['sec_name'] = cur['stockName'].str.replace(' ', '')
                _cur = match_sid_by_code_and_name(next_trading_day(biz_dt), cur, _data_source)
                cur = cur.merge(_cur, on=['sec_code', 'sec_name'])
                cur['key'] = cur['sec_code']

                pre['pre_rate'] = pre['rate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rate'].apply(lambda x: int(x * 100))
            else:
                logger.warning(f"如下证券无相关数据，无法进行数据监控，请检查！fix {_data_source}")
                continue
            _out = pre.loc[~pre['key'].isin(cur['key'].tolist())].copy()
            _in = cur.loc[~cur['key'].isin(pre['key'].tolist())].copy()
            _same = cur.merge(pre, on='key')
            _up = _same.loc[_same['pre_rate'] < _same['cur_rate']].copy()
            _down = _same.loc[_same['pre_rate'] > _same['cur_rate']].copy()
            temp_list = [rw['data_source'], '担保券', biz_dt, '采集业务数据', _in.index.size, _out.index.size, _up.index.size,
                         _down.index.size]
            _message.append(temp_list)
            if not _out.empty:
                _out['adjust_type'] = 'out'
            if not _in.empty:
                _in['adjust_type'] = 'in'
            if not _up.empty:
                _up['adjust_type'] = 'up'
            if not _down.empty:
                _down['adjust_type'] = 'down'
        except Exception as err:
            logger.info(f"{rw['data_source']} ({rw['data_type']}) {err}")
    collect_df = pd.DataFrame(data=_message,
                              columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    return collect_df


def last_work_day(search_date):
    import time, datetime  # 时间
    search_date = datetime.datetime.strptime(search_date,'%Y-%m-%d')
    date = datetime.datetime.combine(search_date, datetime.time())
    # date = datetime.datetime.today()  # 今天
    w = date.weekday() + 1
    # print(w) #周日到周六对应1-7
    if w == 1:  # 如果是周一，则返回上周五
        lastworkday = (date + datetime.timedelta(days=-3)).strftime("%Y-%m-%d")
    elif 1 < w < 7:  # 如果是周二到周五，则返回昨天
        lastworkday = (date + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    elif w == 7:  # 如果是周日
        lastworkday = (date + datetime.timedelta(days=-2)).strftime("%Y-%m-%d")

    return lastworkday


if __name__ == '__main__':
    cur_dt = datetime.now().strftime('%Y-%m-%d')
    # cur_dt = '2023-03-10'
    handle_cmp(cur_dt)
