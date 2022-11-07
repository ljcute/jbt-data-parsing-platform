#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-10-31
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import pandas as pd
from data.ms.base_tools import code_ref_id, get_df_from_cdata


def _get_format_df(cdata, biz_type):
    df = get_df_from_cdata(cdata)
    df['sec_code'] = df['0'].apply(lambda x: (str(x))[-6:])
    # TODO 无市场、无后缀
    # df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['0'].apply(lambda x: (str(x))[:-6])
    df['start_dt'] = None
    biz_dt = df['2'].values[0]
    return biz_dt, code_ref_id(df)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'dbq')
    df['rate'] = df['1'].apply(lambda x: int(str(x).replace('%', '')))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rz_bdq')
    df['rz_rate'] = df['1'].apply(lambda x: int(str(x).replace('%', '')))
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rq_bdq')
    df['rq_rate'] = df['1'].apply(lambda x: int(str(x).replace('%', '')))
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rq
