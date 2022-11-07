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
    if biz_type == 'dbq':
        df['market'] = df['market'].map(lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '3' else str(x))
    elif biz_type in ('rz_bdq', 'rq_bdq'):
        df['market'] = df['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
    df['sec_code'] = df['stkcode'].apply(lambda x: ('000000'+str(x))[-6:])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['stkname']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, code_ref_id(df)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'dbq')
    df['rate'] = df['pledgerate'].apply(lambda x: int(x*100))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rz_bdq')
    df['rz_rate'] = df['marginratefund'].apply(lambda x: int(x*100))
    rz = df.loc[df['creditfundctrl'] == 0][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rq_bdq')
    df['rq_rate'] = df['marginratestk'].apply(lambda x: int(x*100))
    rq = df.loc[df['creditstkctrl'] == 0][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rq
