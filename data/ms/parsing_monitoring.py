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
import sys
import traceback
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from config import Config
from database import MysqlClient
from util.logs_utils import logger


def pro_biz_db():
    global db_biz_pro
    if not db_biz_pro:
        db_biz_pro = MysqlClient(**cfg.get_content(f'pro_db_biz'))
    return db_biz_pro

def pro_raw_db():
    global db_raw_pro
    if not db_raw_pro:
        db_raw_pro = MysqlClient(**cfg.get_content(f'pro_db_raw'))
    return db_raw_pro

def raw_db():
    global raw_db
    if not raw_db:
        raw_db = MysqlClient(**cfg.get_content(f'{env}_db_raw'))
    return raw_db


def biz_db():
    global biz_db
    if not biz_db:
        biz_db = MysqlClient(**cfg.get_content(f'{env}_db_biz'))
    return biz_db


def get_adjust_data(db, biz_dt):
    sql = f"""
    SELECT a.broker_code, a.broker_name,b.* FROM t_security_broker a,
    (SELECT broker_id, 
    (case biz_type when 1 then '融资标的' when 2 then '融券标的' when 3 then '担保券' when 4 then '集中度' else biz_type end) as biz_type, 
    adjust_type,
    (case adjust_type when 1 then '调入' when 2 then '调出' when 3 then '调高' when 4 then '调低' else adjust_type end) as adjust_type_cn, 
    count(*) as adjust_num
    FROM t_broker_mt_business_security 
    WHERE data_status=1 and start_dt <= '{biz_dt} 00:00:00' and end_dt > '{biz_dt} 00:00:00'
     and biz_type in (1,2,3) and start_dt='{biz_dt} 00:00:00'
    GROUP BY broker_id, biz_type, adjust_type) b 
    where a.broker_id=b.broker_id
    ORDER BY b.broker_id, biz_type, adjust_type
    """
    return db.select(sql)


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
                      where biz_dt='{biz_dt}' 
                        and data_status=1
                      group by data_source, data_type)
    """
    return pro_raw_db().select(sql)


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
                      where biz_dt= (select max(biz_dt) from t_ndc_data_collect_log where data_status=1 and biz_dt < '{biz_dt}')
                        and data_status=1
                      group by data_source, data_type)
    """
    return pro_raw_db().select(sql)


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

    parsing_temp_list = [row['broker_name'], '担保券', biz_dt, '解析业务数据', _in_size, _out_size, _up_size, _down_size]
    return parsing_temp_list


def handle_cmp(biz_dt):
    pro_adjust = get_adjust_data(pro_biz_db(), biz_dt)
    pro_adjust['env'] = 'pro'
    pro_adjust.sort_values(by=['biz_type', 'broker_id', 'adjust_type'], inplace=True, ascending=True)
    clm = pro_adjust.columns.tolist()
    duplicate = pro_adjust[pro_adjust.duplicated(subset=clm, keep=False)].copy()
    duplicate.sort_values(by=['biz_type', 'broker_id', 'adjust_type'], inplace=True, ascending=True)
    diff = pro_adjust[~pro_adjust.duplicated(subset=clm, keep=False)].copy()
    diff.sort_values(by=['biz_type', 'broker_id', 'adjust_type'], inplace=True, ascending=True)

    _diff = diff.loc[diff['biz_type'] == '担保券'].copy()
    logger.info(f"{biz_dt} 不一致担保券({_diff.index.size}组)")
    _diff.drop_duplicates(['broker_id', 'biz_type'], inplace=True)

    _diff_rz = diff.loc[diff['biz_type'] == '融资标的'].copy()
    logger.info(f"{biz_dt} 不一致融资标的券({_diff_rz.index.size}组)")
    _diff.drop_duplicates(['broker_id', 'biz_type'], inplace=True)

    _diff_rq = diff.loc[diff['biz_type'] == '融券标的'].copy()
    logger.info(f"{biz_dt} 不一致融券标的券({_diff_rq.index.size}组)")
    _diff.drop_duplicates(['broker_id', 'biz_type'], inplace=True)

    logs = get_collected_data(biz_dt[:10])
    pre_logs = get_pre_collected_data(biz_dt[:10])
    if pre_logs.empty:
        logger.info(f"pre_logs is empty，biz_dt={biz_dt}")
    # 合并
    union = logs.merge(pre_logs, on=['data_source', 'data_type'])

    db_union = union.loc[(union['data_type'] == '2') | (union['data_type'] == '99')].copy()
    logger.info(f'开始核验担保券数据---')
    _all = pro_adjust.loc[pro_adjust['biz_type'] == '担保券'].drop_duplicates(['broker_id', 'biz_type'])
    db_df = db_handle(_all, biz_dt, pro_adjust, db_union)
    logger.info(f'担保券数据核验结束---')

    rz_union = union.loc[(union['data_type'] == '3') | (union['data_type'] == '4') | (union['data_type'] == '99')].copy()
    logger.info(f'开始核验融资标的券数据---')
    _all_rz = pro_adjust.loc[pro_adjust['biz_type'] == '融资标的'].drop_duplicates(['broker_id', 'biz_type'])
    rz_df = rz_handle(_all_rz, biz_dt, pro_adjust, rz_union)
    logger.info(f'融资标的券数据核验结束---')

    rq_union = union.loc[(union['data_type'] == '3') | (union['data_type'] == '5') | (union['data_type'] == '99')].copy()
    logger.info(f'开始核验融券标的券数据---')
    _all_rq = pro_adjust.loc[pro_adjust['biz_type'] == '融券标的'].drop_duplicates(['broker_id', 'biz_type'])
    rq_df = rq_handle(_all_rq, biz_dt, pro_adjust, rq_union)
    logger.info(f'融券标的券数据核验结束---')

    rzrz_df = pd.concat([rz_df, rq_df], ignore_index=False)
    result_df = pd.concat([db_df, rzrz_df], ignore_index=False)
    result_df.sort_values(by=['data_source'], ascending=False)
    result_df = result_df.reset_index()
    return result_df


def rq_handle(_all_rq, biz_dt, pro_adjust, union):
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
                pre['pre_rate'] = None
                cur['cur_rate'] = None
            elif _data_source in ('华泰证券',):
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
                pre['pre_rate'] = pre['slMarginRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['slMarginRatio'].apply(lambda x: int(x * 100))
            elif _data_source in ('中信证券',):
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
            elif _data_source in ('中国银河',):
                pre = pre.loc[pre['type'] == 'rq'].copy()
                cur = cur.loc[cur['type'] == 'rq'].copy()
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
                pre['pre_rate'] = pre['marginratestk'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['marginratestk'].apply(lambda x: int(x * 100))
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
            elif _data_source in ('国信证券',):
                pre['key'] = pre['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(x))
                cur['key'] = cur['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(x))
                pre['pre_rate'] = pre['rzbzjbl'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rzbzjbl'].apply(lambda x: int(x * 100))
            elif _data_source in ('兴业证券',):
                pre['key'] = pre['证券代码'].str.upper()
                cur['key'] = cur['证券代码'].str.upper()
                pre['pre_rate'] = pre['融券保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
                cur['cur_rate'] = cur['融券保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
            elif _data_source in ('申万宏源',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                pre['pre_rate'] = pre['融券保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                cur['cur_rate'] = cur['融券保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
            elif _data_source in ('中金财富',):
                pre['key'] = pre['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                cur['key'] = cur['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                pre['pre_rate'] = pre['guaranteeStock'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['guaranteeStock'].apply(lambda x: int(x * 100))
            elif _data_source in ('中泰证券',):
                pre['key'] = pre['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['BOURSE'].str.strip()
                cur['key'] = cur['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['BOURSE'].str.strip()
                pre['pre_rate'] = pre['STOCK_RATIOS']
                cur['cur_rate'] = cur['STOCK_RATIOS']
            elif _data_source in ('光大证券',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
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
            elif _data_source in ('信达证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rqbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rqbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('东北证券',):
                pre['key'] = pre['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rq'].apply(lambda x: int(x))
                cur['cur_rate'] = cur['rq'].apply(lambda x: int(x))
            elif _data_source in ('长江证券',):
                pre['key'] = pre['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                cur['key'] = cur['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                pre['pre_rate'] = pre['bail_ratio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['bail_ratio'].apply(lambda x: int(x * 100))
            elif _data_source in ('东方财富',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                pre['pre_rate'] = pre['融券保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
                cur['cur_rate'] = cur['融券保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
            elif _data_source in ('东方证券',):
                pre['key'] = pre['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = 50
                cur['cur_rate'] = 50
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
            if not _out.empty or not _in.empty or not _up.empty or not _down.empty:
                _out['adjust_type'] = 'out'
                _in['adjust_type'] = 'in'
                _up['adjust_type'] = 'up'
                _down['adjust_type'] = 'down'
        except Exception as err:
            logger.info(f"{rw['data_source']} ({rw['data_type']}) {err}")
    collect_df = pd.DataFrame(data=_message, columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    parsing_message = []
            
    for index, row in _all_rq.iterrows():
        _pro_adjust = pro_adjust.loc[((pro_adjust['broker_id'] == row['broker_id']) & (pro_adjust['biz_type'] == row['biz_type']))]
        parsing_message.append(get_message(_pro_adjust, row, biz_dt))

    parsing_df = pd.DataFrame(data=parsing_message, columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    rq_df = pd.concat([collect_df, parsing_df], ignore_index=False)
    return rq_df


def rz_handle(_all_rz, biz_dt, pro_adjust, union):
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
                pre['pre_rate'] = None
                cur['cur_rate'] = None
            elif _data_source in ('华泰证券',):
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
            elif _data_source in ('国元证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rz_ratio'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rz_ratio'].apply(lambda x: int(str(x).replace('%', '')))
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
                pre['pre_rate'] = pre['fiMarginRatio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['fiMarginRatio'].apply(lambda x: int(x * 100))
            elif _data_source in ('中信证券',):
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
            elif _data_source in ('中国银河',):
                pre = pre.loc[pre['type'] == 'rz'].copy()
                cur = cur.loc[cur['type'] == 'rz'].copy()
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
                pre['pre_rate'] = pre['marginratefund'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['marginratefund'].apply(lambda x: int(x * 100))
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
            elif _data_source in ('国信证券',):
                pre['key'] = pre['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(
                        x))
                cur['key'] = cur['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'sc'].map(
                    lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(
                        x))
                pre['pre_rate'] = pre['rzbzjbl'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['rzbzjbl'].apply(lambda x: int(x * 100))
            elif _data_source in ('兴业证券',):
                pre['key'] = pre['证券代码'].str.upper()
                cur['key'] = cur['证券代码'].str.upper()
                pre['pre_rate'] = pre['融资保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
                cur['cur_rate'] = cur['融资保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0')) * 100))
            elif _data_source in ('申万宏源',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x in ('北京', 'B') else x)
                pre['pre_rate'] = pre['融资保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                cur['cur_rate'] = cur['融资保证金比例'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
            elif _data_source in ('中金财富',):
                pre['key'] = pre['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                cur['key'] = cur['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
                pre['pre_rate'] = pre['guaranteeMoney'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['guaranteeMoney'].apply(lambda x: int(x * 100))
            elif _data_source in ('中泰证券',):
                pre['key'] = pre['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             pre['BOURSE'].str.strip()
                cur['key'] = cur['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + \
                             cur['BOURSE'].str.strip()
                pre['pre_rate'] = pre['FUND_RATIOS']
                cur['cur_rate'] = cur['FUND_RATIOS']
            elif _data_source in ('光大证券',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
                pre['pre_rate'] = 100
                cur['cur_rate'] = 100
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
                pre['pre_rate'] = 100
                cur['cur_rate'] = 100
            elif _data_source in ('平安证券',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['融资比例'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['融资比例'].apply(lambda x: int(x * 100))
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
            elif _data_source in ('信达证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rzbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['rzbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('东北证券',):
                pre['key'] = pre['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['rz'].apply(lambda x: int(x))
                cur['cur_rate'] = cur['rz'].apply(lambda x: int(x))
            elif _data_source in ('长江证券',):
                pre['key'] = pre['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                cur['key'] = cur['stock_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchange_type'].map(
                    lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '9' else str(x))
                pre['pre_rate'] = pre['fin_ratio'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['fin_ratio'].apply(lambda x: int(x * 100))
            elif _data_source in ('东方财富',):
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                pre['pre_rate'] = pre['融资保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
                cur['cur_rate'] = cur['融资保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
            elif _data_source in ('东方证券',):
                pre['key'] = pre['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = 100
                cur['cur_rate'] = 100
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
            if not _out.empty or not _in.empty or not _up.empty or not _down.empty:
                _out['adjust_type'] = 'out'
                _in['adjust_type'] = 'in'
                _up['adjust_type'] = 'up'
                _down['adjust_type'] = 'down'
        except Exception as err:
            logger.info(f"{rw['data_source']} ({rw['data_type']}) {err}")
    collect_df = pd.DataFrame(data=_message, columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    parsing_message = []

    for index, row in _all_rz.iterrows():
        _pro_adjust = pro_adjust.loc[((pro_adjust['broker_id'] == row['broker_id']) & (pro_adjust['biz_type'] == row['biz_type']))]
        parsing_message.append(get_message(_pro_adjust, row, biz_dt))

    parsing_df = pd.DataFrame(data=parsing_message, columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    rz_df = pd.concat([collect_df, parsing_df], ignore_index=False)
    return rz_df


def db_handle(_all, biz_dt, pro_adjust, union):
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
                pre['证券代码'] = pre['证券代码'].astype('str')
                cur['证券代码'] = cur['证券代码'].astype('str')
                pre['key'] = pre['证券代码']
                cur['key'] = cur['证券代码']
                pre['pre_rate'] = None
                cur['cur_rate'] = None
            elif _data_source in ('华泰证券',):
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
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(
                    x) == '北A' else str(x))
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
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['折算率'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['折算率'].apply(lambda x: int(x * 100))
            elif _data_source in ('中金公司',):
                pre['key'] = pre['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(x) else str(x))
                cur['key'] = cur['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    'exchname'].map(
                    lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(x) else str(x))
                pre['pre_rate'] = pre['cmorate'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['cmorate'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('信达证券',):
                pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['dbpzabl'].apply(lambda x: int(str(x).replace('%', '')))
                cur['cur_rate'] = cur['dbpzabl'].apply(lambda x: int(str(x).replace('%', '')))
            elif _data_source in ('东北证券',):
                pre = pre[:-1]
                cur = cur[:-1]
                pre['key'] = pre['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['bm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
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
                pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur[
                    '市场'].map(
                    lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(
                        x))
                pre['pre_rate'] = pre['实际折算率'].apply(lambda x: float(str(x).replace('%', '')))
                cur['cur_rate'] = cur['实际折算率'].apply(lambda x: float(str(x).replace('%', '')))
            elif _data_source in ('东方证券',):
                pre['key'] = pre['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                cur['key'] = cur['securitiescode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                pre['pre_rate'] = pre['convertrate'].apply(lambda x: int(x * 100))
                cur['cur_rate'] = cur['convertrate'].apply(lambda x: int(x * 100))
            else:
                logger.warning(f"fix {_data_source}")
                continue
            _out = pre.loc[~pre['key'].isin(cur['key'].tolist())].copy()
            _in = cur.loc[~cur['key'].isin(pre['key'].tolist())].copy()
            _same = cur.merge(pre, on='key')
            _up = _same.loc[_same['pre_rate'] < _same['cur_rate']].copy()
            _down = _same.loc[_same['pre_rate'] > _same['cur_rate']].copy()
            temp_list = [rw['data_source'], '担保券', biz_dt, '采集业务数据', _in.index.size, _out.index.size, _up.index.size,
                         _down.index.size]
            _message.append(temp_list)
            if not _out.empty or not _in.empty or not _up.empty or not _down.empty:
                _out['adjust_type'] = 'out'
                _in['adjust_type'] = 'in'
                _up['adjust_type'] = 'up'
                _down['adjust_type'] = 'down'
        except Exception as err:
            logger.info(f"{rw['data_source']} ({rw['data_type']}) {err}")
    collect_df = pd.DataFrame(data=_message, columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    parsing_message = []

    for index, row in _all.iterrows():
        _pro_adjust = pro_adjust.loc[((pro_adjust['broker_id'] == row['broker_id']) & (pro_adjust['biz_type'] == row['biz_type']))]
        parsing_message.append(get_message(_pro_adjust, row, biz_dt))

    parsing_df = pd.DataFrame(data=parsing_message, columns=['data_source', 'data_type', 'biz_dt', 'platform', 'in', 'out', 'up', 'down'])
    db_df = pd.concat([collect_df, parsing_df], ignore_index=False)
    return db_df


if __name__ == '__main__':
    try:
        __environments = ("loc", "dev", "fat", "uat", "pro")
        biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券",
                        4: "融资标的证券", 5: "融券标的证券", 99: "融资融券可充抵保证金证券和融资融券标的证券"}
        cfg = Config.get_cfg()
        if "environment" not in cfg.get_sections() or "env" not in cfg.get_options("environment"):
            raise Exception(f"请设置当前环境environment.env")
        env = cfg.get_content("environment").get("env")
        if env not in __environments:
            raise Exception(f"环境env只能是{__environments}范围内容")
        raw_db = MysqlClient(**cfg.get_content(f'{env}_db_raw'))
        biz_db = MysqlClient(**cfg.get_content(f'{env}_db_biz'))
        db_biz_pro = MysqlClient(**cfg.get_content(f'pro_db_biz'))
        db_raw_pro = MysqlClient(**cfg.get_content(f'pro_db_raw'))

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('max_colwidth', 200)
        pd.set_option('expand_frame_repr', False)

        cur_dt = datetime.now().strftime('%Y-%m-%d')
        handle_cmp(cur_dt)

    except Exception as e:
        logger.error(f"互联网数据解析服务启动异常: {e} =》{str(traceback.format_exc())}")
