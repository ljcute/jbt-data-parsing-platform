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
    df['market'] = df['branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(x) == '北交所' else str(x))
    df['sec_code'] = df['secCode'].apply(lambda x: ('000000'+str(x))[-6:])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['secAbbr']
    df['start_dt'] = None
    biz_dt = str(df['stamp'].values[0])[:10]
    return biz_dt, code_ref_id(df)


def _format_db_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['rate'].apply(lambda x: int(str(x).replace('%', '')))
    dbq = df.loc[df['type'] == 1][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    rz = df.loc[df['type'] == 2][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    rq = df.loc[df['type'] == 3][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame(), rz, rq
