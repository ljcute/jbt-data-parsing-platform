#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-11-04
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import re
import threading
import pandas as pd
from io import StringIO
from util.logs_utils import logger
from data.ms.fdb import get_ex_discount_limit_rate
from data.ms.register_center import search_bo_info
from data.ms.sec360 import get_sec360_sec_id_code, register_sec360_security
sz = r'(\d+)'
zm = r'[\u0041-\u005a|\u0061-\u007a]+'
zw = r'[\u4e00-\u9fa5]+'


def get_df_from_cdata(cdata):
    return pd.read_csv(StringIO(cdata['data_text'][0]), sep=",")


class Cache(object):

    _sic_df = pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec360_name', 'exchange_sec_name'])

    @classmethod
    def get_sic_df(cls):
        return Cache._sic_df.copy()

    @classmethod
    def set_sic_df(cls, sic_df):
        Cache._sic_df = sic_df
        return cls.get_sic_df()


def get_sic_df():
    return Cache.get_sic_df()


def set_sic_df(_df360, _df_exchange, exchange=False):
    if _df360.empty:
        return get_sic_df()
    mutex = threading.Lock()
    mutex.acquire(60)  # 里面可以加blocking(等待的时间)或者不加，不加就会一直等待(堵塞)
    _df = _df360.copy()
    if exchange:
        _df = _df.merge(_df_exchange[['sec_code', 'sec_name']], on='sec_code').copy()
        _df.rename(columns={'sec_name': 'exchange_sec_name'}, inplace=True)
        _df['exchange_sec_name'] = _df['exchange_sec_name'].str.replace(' ', '')
    else:
        _df['exchange_sec_name'] = None
    _df['sec360_name'] = _df['sec360_name'].str.replace(' ', '')
    df = Cache.set_sic_df(pd.concat([Cache.get_sic_df(), _df]).drop_duplicates())
    mutex.release()
    return df


def refresh_sic_df(_df, exchange=False):
    if _df.empty:
        return get_sic_df()
    # 证券360刷证券ID
    sec = get_sec360_sec_id_code(_df['sec_code'].tolist())
    sec['sec360_name'] = sec['sec360_name'].str.replace(' ', '')
    # 注册中心刷证券ID
    no_sec360 = _df.loc[~_df['sec_code'].isin(sec['sec_code'].tolist())][sec.columns.tolist() + ['sec_name']]
    no_sec360['sec_name'] = no_sec360['sec_name'].str.replace(' ', '')
    no_sec360['sec360_name'] = no_sec360['sec_name']
    no_sec360 = no_sec360[sec.columns.tolist()]
    if not no_sec360.empty:
        # 注册中心找对象
        for index, row in no_sec360.iterrows():
            bo = search_bo_info(row['sec_code'][:-3])
            bo['sec_name'] = bo['sec_name'].str.replace(' ', '')
            if bo.empty:
                continue
            # 代码全匹配
            _bo = bo.loc[bo['sec_code'] == row['sec_code']]
            if not _bo.empty:
                row['sec_type'] = _bo['sec_type'].tolist()[0]
                row['sec_id'] = _bo['sec_id'].tolist()[0]
                row['sec360_name'] = min(_bo['sec_name'].tolist(), key=len)
                sec = pd.concat([sec, row.to_frame().T])
                continue
            # 名称全匹配
            _bo = bo.loc[bo['sec_name'] == row['sec360_name']]
            if not _bo.empty:
                row['sec_type'] = _bo['sec_type'].tolist()[0]
                row['sec_id'] = _bo['sec_id'].tolist()[0]
                row['sec_code'] = _bo['sec_code'].tolist()[0]
                sec = pd.concat([sec, row.to_frame().T])
                continue
            # 名称包含匹配
            for idx, rw in bo.iterrows():
                if rw['sec_name'] in row['sec360_name'] or row['sec360_name'] in rw['sec_name']:
                    row['sec_type'] = rw['sec_type']
                    row['sec_id'] = rw['sec_id']
                    row['sec_code'] = rw['sec_code']
                    sec = pd.concat([sec, row.to_frame().T])
                    break
            # 名称拆解再模糊匹配
            sec_name = re.findall(sz, row['sec360_name']) + char_arr_split(re.findall(zm, row['sec360_name']), split_length=3) + char_arr_split(re.findall(zw, row['sec360_name']))
            for idx, rw in bo.iterrows():
                _sec_name = re.findall(sz, rw['sec_name']) + char_arr_split(re.findall(zm, rw['sec_name']), split_length=3) + char_arr_split(re.findall(zw, rw['sec_name']))
                if len(set(sec_name) & set(_sec_name)) > 0:
                    row['sec_type'] = rw['sec_type']
                    row['sec_id'] = rw['sec_id']
                    row['sec_code'] = rw['sec_code']
                    sec = pd.concat([sec, row.to_frame().T])
                    break
    return set_sic_df(sec, _df, exchange)


def register_sic_df(_df, exchange=False):
    if _df.empty:
        return get_sic_df()
    return set_sic_df(register_sec360_security(_df[['sec_type', 'sec_code', 'sec_name']]), _df, exchange)


def code_ref_id(_df, exchange=False):
    df1 = _df.merge(get_sic_df(), how='left', on='sec_code')
    no_sec_id_df = df1.loc[df1['sec_id'].isna()]
    if not no_sec_id_df.empty:
        # 刷证券ID
        df1 = _df.merge(refresh_sic_df(no_sec_id_df, exchange), how='left', on='sec_code')
        zc_sec_df = df1.loc[df1['sec_id'].isna()][['market', 'sec_code', 'sec_name']]
        if not zc_sec_df.empty:
            # 监控并Email关键词：本次需注册证券对象有
            logger.warn(f"本次需注册证券对象有({zc_sec_df.index.size}只)：\n{zc_sec_df.reset_index(drop=True)}")
    return df1.loc[~df1['sec_id'].isna()].copy()


def get_exchange_discount_limit_rate(biz_dt, df):
    if df.empty:
        return
    s_df = df.loc[df['sec_type'] == 'stock']
    s_rate = get_ex_discount_limit_rate(biz_dt, 'stock', s_df['sec_id'].tolist())
    b_df = df.loc[df['sec_type'] == 'bond']
    b_rate = get_ex_discount_limit_rate(biz_dt, 'bond', b_df['sec_id'].tolist())
    f_df = df.loc[df['sec_type'] == 'fund']
    f_rate = get_ex_discount_limit_rate(biz_dt, 'fund', f_df['sec_id'].tolist())
    # 根据返回内容组装结果返回
    sbf_rate = pd.concat([s_rate, b_rate, f_rate])
    _df = pd.merge(df, sbf_rate, how="left", on="sec_id")
    no_rate_df = _df.loc[_df['rate'].isna()][['sec_type', 'sec_id', 'sec_code', 'sec_name']]
    if not no_rate_df.empty:
        # 监控并Email关键词：如下证券缺少交易所折算率上限
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        logger.warn(f"如下证券缺少交易所折算率上限({no_rate_df.index.size}只)：\n{no_rate_df.reset_index(drop=True)}")
    return _df


def char_split(value, split_length=2):
    if isinstance(value, str):
        length = len(value)
        if length > split_length:
            _value = []
            for i in range(length - split_length + 1):
                _value.append(value[i:i+split_length])
            return _value
    return [value]


def char_arr_split(values, split_length=2):
    if isinstance(values, list):
        _values = []
        for value in values:
            _values += char_split(value, split_length)
        return _values
    return values


def match_sid_by_code_and_name(df):
    """"
    1、找出df中每个代码，在sid_df中只存在1个代码的代码，认为是相同代码
    2、再再出df中每个代码的前5位，在sid_df中只存在1个市场和证券类型的代码，认为是相同市场的相同证券类型
    """
    if df.empty:
        return df
    df1 = df[['sec_code', 'sec_name']].drop_duplicates()
    _sid_df = get_sic_df().rename(columns={'sec_type': 'stp', 'sec_id': 'sid', 'sec_code': 'scd'})
    _sid_df[['cd6', 'market']] = _sid_df['scd'].str.split('.', n=1, expand=True)
    target_columns = df1.columns.tolist() + ['stp', 'sid', 'scd']
    # 6位代码 + 名称完全匹配
    all_match1 = pd.merge(df1, _sid_df, left_on=['sec_code', 'sec_name'], right_on=['cd6', 'sec360_name'], how='inner')[target_columns]
    all_match2 = pd.merge(df1, _sid_df, left_on=['sec_code', 'sec_name'], right_on=['cd6', 'exchange_sec_name'], how='inner')[target_columns]
    all_match = pd.concat([all_match1, all_match2]).drop_duplicates()
    not_match = pd.concat([df1, all_match[df1.columns.tolist()]]).drop_duplicates(keep=False)
    # 6位代码 + 名称模糊匹配(包含关系)
    like_name = pd.merge(not_match, _sid_df, left_on='sec_code', right_on='cd6', how='left')
    like_name = like_name.loc[~like_name['sid'].isna()]
    _like_name = pd.DataFrame(columns=all_match.columns)
    for index, row in like_name.iterrows():
        if str(row['sec_name']) in str(row['sec360_name']) or str(row['sec_name']) in str(row['exchange_sec_name']) or str(row['sec360_name']) in str(row['sec_name']) or str(row['exchange_sec_name']) in str(row['sec_name']):
            _like_name = pd.concat([_like_name, row[all_match.columns.tolist()].to_frame().T])
    match = pd.concat([all_match, _like_name])
    # 未匹配证券
    no_match = pd.concat([df1, match[df1.columns.tolist()]]).drop_duplicates(keep=False)
    # 未匹配证券，通过注册中心寻找历史名称再识别
    no_match['stp'] = None
    no_match['sid'] = None
    no_match['scd'] = None
    for index, row in no_match.iterrows():
        sec = search_bo_info(row['sec_code'])
        _zc_sec = sec.loc[sec['sec_name'] == row['sec_name']]
        if not _zc_sec.empty:
            row['sid'] = _zc_sec['sec_id'].tolist()[0]
            row['stp'] = _zc_sec['sec_type'].tolist()[0]
            row['scd'] = _zc_sec['sec_code'].tolist()[0]
            continue
        sec_name = re.findall(sz, row['sec_name']) + char_arr_split(re.findall(zm, row['sec_name']), split_length=3) + char_arr_split(re.findall(zw, row['sec_name']))
        for idx, rw in sec.iterrows():
            _sec_name = re.findall(sz, rw['sec_name']) + char_arr_split(re.findall(zm, rw['sec_name']), split_length=3) + char_arr_split(re.findall(zw, rw['sec_name']))
            if len(set(sec_name) & set(_sec_name)) > 0:
                row['sid'] = rw['sec_id']
                row['stp'] = rw['sec_type']
                row['scd'] = rw['sec_code']
                break
    match = pd.concat([match, no_match.loc[~no_match['sid'].isna()]])
    no_match = df1.merge(match, how='left', on=['sec_code', 'sec_name'])
    no_match = no_match.loc[no_match['sid'].isna()]
    if not no_match.empty:
        # 监控并Email关键词：如下证券对象无法识别
        logger.warn(f"如下证券对象无法识别\n {no_match}")
    return match.rename(columns={'sid': 'sec_id', 'stp': 'sec_type'})
