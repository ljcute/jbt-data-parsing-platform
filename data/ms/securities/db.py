#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/2/20 17:07
# @Site    : 
# @File    : db.py
# @Software: PyCharm
import pandas as pd
from data.ms.base_tools import code_ref_id, get_df_from_cdata


def _get_format_df(cdata):
    df = get_df_from_cdata(cdata)
    df['market'] = df['jys'].map(lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(x))
    df['sec_code'] = df['bm'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['name']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, code_ref_id(df)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['zsl'].apply(lambda x: int(x))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['rz'].apply(lambda x: int(x))
    df['rq_rate'] = df['rq'].apply(lambda x: int(x))
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
