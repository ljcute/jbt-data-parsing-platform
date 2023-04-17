#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/16 17:22
# @Site    : 
# @File    : xd.py
# @Software: PyCharm
import datetime
import pandas as pd
from data.ms.base_tools import get_df_from_cdata, match_sid_by_code_and_name, next_trading_day


def _get_format_df(cdata, biz_type):
    data_source, df = get_df_from_cdata(cdata)
    biz_dt = df['creat_date'].values[0]
    df['sec_code'] = df['secu_code'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_name'] = df['secu_name']
    df['sec_name'] = df['sec_name'].str.replace(' ', '')
    biz_dt = next_trading_day(biz_dt)
    _df = match_sid_by_code_and_name(biz_dt, df, data_source)
    df = df.merge(_df, on=['sec_code', 'sec_name'])
    df['start_dt'] = None
    return biz_dt, df


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'dbq')
    df['rate'] = df['dbpzabl'].apply(lambda x: int(str(x).replace('%', '')))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rz_bdq')
    df['rz_rate'] = df['rzbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rq_bdq')
    df['rq_rate'] = df['rqbzjbl'].apply(lambda x: int(str(x).replace('%', '')))
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rq
