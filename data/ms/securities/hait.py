#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/9/22 18:38
# @Site    :
# @File    : ax_securities_parsing.py
# @Software: PyCharm

import pandas as pd
from data.ms.base_tools import get_df_from_cdata, code_ref_id


def _get_format_df(cdata, biz_type):
    data_source, df = get_df_from_cdata(cdata)
    df['market'] = df['marketCode'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(x) == '北交所' else str(x))
    df['sec_code'] = df['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    # 代码8、4开头，把市场修复为BJ
    df['sec_code'] = df['sec_code'].apply(lambda x: str(x)[:6] + '.BJ' if str(x)[:1] in ('8', '4') else x)
    df['sec_name'] = df['productName']
    df['start_dt'] = None
    dt = str(df['etlDate'].values[0])
    if '-' in dt:
        biz_dt = dt
    else:
        biz_dt = str(dt)[:4] + '-' + str(dt)[4:6] + '-' + str(dt)[-2:]
    return biz_dt, code_ref_id(biz_dt, df, data_source)


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata, 'dbq')
    df['rate'] = df['stockConvertRate'].apply(lambda x: int(str(x).replace('%', '')))
    dbq = df.loc[df['ifGuarantee'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, market)
    df['rz_rate'] = 100
    df['rq_rate'] = 50
    rz = df.loc[df['ifFinancing'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df.loc[df['ifBorrowStock'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    return biz_dt, rz, rq


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, market)
    df['rate'] = 100
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata, market)
    df['rate'] = 50
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, rq
