#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/12/21 9:40
# @Site    : 
# @File    : zj.py
# @Software: PyCharm
import pandas as pd
from data.ms.base_tools import get_df_from_cdata, match_sid_by_code_and_name


def _get_format_df(cdata):
    df = get_df_from_cdata(cdata)
    df['sec_code'] = df['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_name'] = df['证券名称']
    df['sec_name'] = df['sec_name'].str.replace(' ', '')
    _df = match_sid_by_code_and_name(df)
    df = df.merge(_df, on=['sec_code', 'sec_name'])
    df['sec_code'] = df['scd']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, df


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['中金折算率'].apply(lambda x: int(str(x).replace('%', '')))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['保证金比例'].apply(lambda x: int(str(x).replace('%', '')))
    rz = df.loc[df['是否融资标的物'] == '是'][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rq_rate'] = df['保证金比例'].apply(lambda x: int(str(x).replace('%', '')))
    rq = df.loc[df['是否融券标的物'] == '是'][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rq