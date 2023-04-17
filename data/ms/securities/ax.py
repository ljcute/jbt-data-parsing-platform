#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-10-31
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import pandas as pd
from data.ms.base_tools import code_ref_id, get_df_from_cdata, next_trading_day


def _get_format_df(cdata):
    data_source, df = get_df_from_cdata(cdata)
    df['market'] = df['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '3' else str(x))
    df['sec_code'] = df['secuCode'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['secuName']
    df['start_dt'] = None
    dt = df['effectiveDate'].values[0]
    if '-' in dt:
        biz_dt = dt
    else:
        biz_dt = str(dt)[:4] + '-' + str(dt)[4:6] + '-' + str(dt)[-2:]
    biz_dt = next_trading_day(biz_dt)
    return biz_dt, code_ref_id(biz_dt, df, data_source)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['collatRatio'].apply(lambda x: int(x*100))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    jzd = df[['sec_type', 'sec_id', 'sec_code', 'riskLvlDesc']].copy()
    jzd.rename(columns={'riskLvlDesc': 'rate'}, inplace=True)
    jzd['rate'] = jzd['rate'].apply(lambda x: 1 if str(x) == 'R1' else 2 if str(x) == 'R2' else 3 if str(x) == 'R3' else 4 if str(x) == 'R4' else 5 if str(x) == 'R5' else 6 if str(x) == 'R6' else str(x))
    return biz_dt, dbq, jzd


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['fiMarginRatio'].apply(lambda x: int(x*100))
    df['rq_rate'] = df['slMarginRatio'].apply(lambda x: int(x*100))
    rz = df.loc[df['enableFi'] == '可以融资'][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df.loc[df['enableSl'] == '可以融券'][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
