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
    df['sec_code'] = df['证券代码'].apply(lambda x: ('000000'+str(x))[-6:])
    # TODO 无市场、无后缀
    # df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['证券简称']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, code_ref_id(df)


def _format_db_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['rate'].apply(lambda x: int(x*100))
    dbq = df.loc[df['type'] == 'db'][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    rz = df.loc[df['type'] == 'rz'][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    rq = df.loc[df['type'] == 'rq'][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame(), rz, rq
