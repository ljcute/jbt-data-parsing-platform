#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-10-31
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import pandas as pd
from data.ms.base_tools import code_ref_id, get_df_from_cdata, get_exchange_discount_limit_rate


def _get_format_df(cdata, market):
    df = get_df_from_cdata(cdata)
    df['market'] = market
    df['sec_code'] = df['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['证券简称']
    df['start_dt'] = None
    if market == 'SH':
        biz_dt = df['日期'].values[0]
    else:
        biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, code_ref_id(df, exchange=True)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, market)
    # 从FDB取交易所折算率上限
    df = get_exchange_discount_limit_rate(biz_dt, df)
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, market)
    df['rz_rate'] = 100
    df['rq_rate'] = 50
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    return biz_dt, rz, rq


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, market)
    df['rate'] = 100
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, market)
    df['rate'] = 50
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, rq
