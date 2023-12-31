#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/20 17:07
# @Site    : 
# @File    : db.py
# @Software: PyCharm
import pandas as pd
from data.ms.base_tools import match_sid_by_code_and_name, get_df_from_cdata, next_trading_day


def _get_format_df(cdata):
    data_source, df = get_df_from_cdata(cdata)
    df = df[:-1]
    biz_dt = cdata['biz_dt'].values[0]
    df['sec_code'] = df['bm'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_name'] = df['name'].str.replace(' ', '')
    biz_dt = next_trading_day(biz_dt)
    _df = match_sid_by_code_and_name(biz_dt, df, data_source)
    df = df.merge(_df, on=['sec_code', 'sec_name'])
    df['start_dt'] = None
    return biz_dt, df


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['zsl'].apply(lambda x: int(x))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['rz'].apply(lambda x: int(x))
    df['rq_rate'] = df['rq'].apply(lambda x: int(x))
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
