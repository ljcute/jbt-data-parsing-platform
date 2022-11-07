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
        df = df.loc[(df['isValid'] == 1) & (df['status'] == '正常')].copy()
    df['market'] = df['exchangeCode'].map(lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(x))
    df['sec_code'] = df['stockCode'].apply(lambda x: ('000000'+str(x))[-6:])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['stockName']
    df['start_dt'] = None
    dt = df['dataDate'].values[0]
    biz_dt = str(dt)[:4] + '-' + str(dt)[4:6] + '-' + str(dt)[-2:]
    return biz_dt, code_ref_id(df)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'dbq')
    df['rate'] = df['percent'].apply(lambda x: int(x*100))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'bdq')
    df['rz_rate'] = df['rzPercent'].apply(lambda x: int(x*100))
    df['rq_rate'] = df['rqPercent'].apply(lambda x: int(x*100))
    rz = df.loc[df['rzFloag'] == 1][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df.loc[df['rqFloag'] == 1][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
