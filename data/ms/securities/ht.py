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


def _get_format_df(cdata):
    df = get_df_from_cdata(cdata)
    df['market'] = df['exchangeType'].map(lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(x) == 9 else str(x))
    df['sec_code'] = df['stockCode'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['stockName']
    df['start_dt'] = df['initDate'].apply(lambda x: str(x)[:4]+'-'+str(x)[4:6]+'-'+str(x)[-2:])
    dt = df['dataDt'].values[0]
    biz_dt = str(dt)[:4] + '-' + str(dt)[4:6] + '-' + str(dt)[-2:]
    return biz_dt, code_ref_id(df)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['assureRatio'].apply(lambda x: int(x*100))
    dbq = df.loc[df['assureStatus'] == 0][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    jzd = df.loc[df['assureStatus'] == 0][['sec_type', 'sec_id', 'sec_code', 'stockGroupName']].copy()
    jzd.rename(columns={'stockGroupName': 'rate'}, inplace=True)
    jzd['rate'] = jzd['rate'].apply(lambda x: 1 if str(x).upper() == 'A' else 2 if str(x).upper() == 'B' else 3 if str(x).upper() == 'C' else 4 if str(x).upper() == 'D' else 5 if str(x).upper() == 'E' else x)
    return biz_dt, dbq, jzd


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['finRatio'].apply(lambda x: int(x*100))
    df['rq_rate'] = df['sloRatio'].apply(lambda x: int(x*100))
    rz = df.loc[df['finStatus'] == 0][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df.loc[df['sloStatus'] == 0][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
