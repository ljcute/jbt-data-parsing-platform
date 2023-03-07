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
    df['market'] = df['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) in ('2', 'B') else str(x))
    df['sec_code'] = df['stkcode'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['stkname']
    df['start_dt'] = None
    biz_dt = str(df['updatedate'].values[0])[:10]
    return biz_dt, code_ref_id(df, data_source)


def _format_db_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    dbq = df.loc[~df['pledgerate'].isna()][['sec_type', 'sec_id', 'sec_code', 'pledgerate']].copy()
    dbq['pledgerate'] = dbq['pledgerate'].apply(lambda x: int(x))
    dbq.rename(columns={'pledgerate': 'rate'}, inplace=True)
    rz = df.loc[df['fundctrlflag'] == 0][['sec_type', 'sec_id', 'sec_code', 'marginratefund']].copy()
    rz['marginratefund'] = rz['marginratefund'].apply(lambda x: int(x))
    rz.rename(columns={'marginratefund': 'rate'}, inplace=True)
    rq = df.loc[df['stkctrlflag'] == 0][['sec_type', 'sec_id', 'sec_code', 'marginratestk']].copy()
    rq['marginratestk'] = rq['marginratestk'].apply(lambda x: int(x))
    rq.rename(columns={'marginratestk': 'rate'}, inplace=True)
    jzd = df.loc[~df['pledgerate'].isna()][['sec_type', 'sec_id', 'sec_code', 'stkclasses']].copy()
    jzd.rename(columns={'stkclasses': 'rate'}, inplace=True)
    jzd['rate'] = jzd['rate'].apply(lambda x: 1 if str(x).upper() == 'A类' else 2 if str(x).upper() == 'B类' else 3 if str(x).upper() == 'C类' else 4 if str(x).upper() == 'D类' else 5 if str(x).upper() == 'E类' else 0)
    return biz_dt, dbq, jzd, rz, rq
