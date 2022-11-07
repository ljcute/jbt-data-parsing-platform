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
    df['market'] = df['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
    df['sec_code'] = df['stkCode'].apply(lambda x: ('000000'+str(x))[-6:])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['stkName']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, code_ref_id(df)


def _format_db_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    dbq = df.loc[~df['pledgerate'].isna()][['sec_type', 'sec_id', 'sec_code', 'pledgerate']].copy()
    dbq['pledgerate'] = dbq['pledgerate'].apply(lambda x: int(x*100))
    dbq.rename(columns={'pledgerate': 'rate'}, inplace=True)
    rz = df.loc[df['fundctrlflag'] == 0][['sec_type', 'sec_id', 'sec_code', 'marginratefund']].copy()
    rz['marginratefund'] = rz['marginratefund'].apply(lambda x: int(x*100))
    rz.rename(columns={'marginratefund': 'rate'}, inplace=True)
    rq = df.loc[df['stkctrlflag'] == 0][['sec_type', 'sec_id', 'sec_code', 'marginratestk']].copy()
    rq['marginratestk'] = rq['marginratestk'].apply(lambda x: int(x*100))
    rq.rename(columns={'marginratestk': 'rate'}, inplace=True)
    return biz_dt, dbq, pd.DataFrame(), rz, rq
