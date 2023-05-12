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
    df = df.loc[df['state'] == 1].copy()
    df = df[:-1]
    df['stockId'] = df['stockId'].apply(lambda x: int(x))
    df['market'] = df['exchange'].map(lambda x: 'BJ' if str(x) in ('1', 'BJ') else str(x))
    df['sec_code'] = df['stockId'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['stockName']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    biz_dt = next_trading_day(biz_dt)
    return biz_dt, code_ref_id(biz_dt, df, data_source)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['rate'].apply(lambda x: int(x*100))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['guaranteeMoney'].apply(lambda x: int(x*100))
    df['rq_rate'] = df['guaranteeStock'].apply(lambda x: int(x*100))
    rz = df.loc[df['moneyTarget'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df.loc[df['stockTarget'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
