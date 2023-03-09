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
    data_source, df = get_df_from_cdata(cdata)
    df['market'] = df['市场'].map(lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(x))
    df['sec_code'] = df['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['证券简称']
    df['start_dt'] = None
    df.sort_values(by=["sec_code", "sec_name"], ascending=[True, True])
    df.drop_duplicates(subset=["sec_code", "sec_name"], keep='first', inplace=True, ignore_index=False)
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, code_ref_id(df, data_source)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['实际折算率'].apply(lambda x: float(str(x).replace('%', '')))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    jzd = df[['sec_type', 'sec_id', 'sec_code', '组别']].copy()
    jzd.rename(columns={'组别': 'rate'}, inplace=True)
    jzd['rate'] = jzd['rate'].apply(lambda x: 1 if str(x) == 'A组' else 2 if str(x) == 'B组' else 3 if str(x) == 'C组' else 4 if str(x) == 'D组' else 5 if str(x) == 'E组' else 6 if str(x) == 'F组' else 7 if str(x) == '关注类' else 8 if str(x) == '北交所' else str(x))
    return biz_dt, dbq, jzd


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['融资保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
    df['rq_rate'] = df['融券保证金比例'].apply(lambda x: float(str(x).replace('%', '')))
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq
