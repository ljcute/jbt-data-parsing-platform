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
    data_source, df = get_df_from_cdata(cdata)
    df['sec_code'] = df['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
    df['sec_name'] = df['zqmc']
    df['sec_name'] = df['sec_name'].str.replace(' ', '')
    biz_dt = df['rq'].values[0]
    if biz_type == 'dbq':
        _df = match_sid_by_code_and_name(biz_dt, df, data_source)
        df = df.merge(_df, on=['sec_code', 'sec_name'])
    elif biz_type in ('rz_bdq', 'rq_bdq'):
        df['market'] = df['sc'].map(lambda x: 'SZ' if str(x) == '1' else 'SH' if str(x) == '0' else 'BJ' if str(x) == '3' else str(x))
        df['sec_code'] = df['sec_code'] + '.' + df['market']
        df = code_ref_id(biz_dt, df, data_source)
    df['start_dt'] = None
    return biz_dt, df


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'dbq')
    df['rate'] = df['zsl'].apply(lambda x: int(x*100))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    jzd = df[['sec_type', 'sec_id', 'sec_code', 'jzd']].copy()
    jzd.rename(columns={'jzd': 'rate'}, inplace=True)
    jzd['rate'] = jzd['rate'].apply(lambda x: -1 if str(x).strip() == '非法字符' else 1 if str(x).strip() == 'A' else 2 if str(x).strip() == 'B' else 3 if str(x).strip() == 'C' else 4 if str(x).strip() == 'D' else 5 if str(x).strip() == 'E' else 6 if str(x).strip() == 'F' else x)
    return biz_dt, dbq, jzd


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rz_bdq')
    df['rz_rate'] = df['rzbzjbl'].apply(lambda x: int(x*100))
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rz = rz[rz['rate'] >= 100]
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'rq_bdq')
    df['rq_rate'] = df['rzbzjbl'].apply(lambda x: int(x*100))
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rq
