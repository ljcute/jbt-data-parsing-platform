#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-10-31
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import pandas as pd
from data.ms.base_tools import code_ref_id, get_df_from_cdata, match_sid_by_code_and_name


def _get_format_df(cdata, biz_type):
    df = get_df_from_cdata(cdata)
    df['sec_name'] = df['证券简称']
    df['sec_name'] = df['sec_name'].str.replace(' ', '')
    if biz_type == 'dbq':
        df['sec_code'] = df['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    elif biz_type == 'bdq':
        df['sec_code'] = df['证券代码'].str.upper()

    _df = match_sid_by_code_and_name(df)
    df = df.merge(_df, on=['sec_code', 'sec_name'])
    df['sec_code'] = df['scd']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, df


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'dbq')
    df['rate'] = df['折算率'].apply(lambda x: int(x*100))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'bdq')
    df['rz_rate'] = df['融资保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0'))*100))
    df['rq_rate'] = df['融券保证金比例'].apply(lambda x: int(float(str(x).replace('/', '0'))*100))
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
