#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/12/21 9:40
# @Site    : 
# @File    : zj.py
# @Software: PyCharm
import math

import pandas as pd
from data.ms.base_tools import get_df_from_cdata, code_ref_id


def _get_format_df(cdata):
    df = get_df_from_cdata(cdata)
    df['market'] = df['exchname'].map(
        lambda x: 'SZ' if str(x) == '深市' else 'SH' if str(x) == '沪市' else 'BJ' if math.isnan(x) else str(x))
    df['sec_code'] = df['stkid'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['stkname']
    df['start_dt'] = None
    biz_dt = cdata['biz_dt'].values[0]
    return biz_dt, code_ref_id(df)


def _format_db_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    dbq = df.loc[df['iscmo'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'cmorate']].copy()
    dbq['cmorate'] = dbq['cmorate'].apply(lambda x: int(str(x).replace('%', '')))
    dbq.rename(columns={'cmorate': 'rate'}, inplace=True)
    rz = df.loc[df['iscreditcashstk'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'ccmarginrate']].copy()
    rz['ccmarginrate'] = rz['ccmarginrate'].apply(lambda x: int(str(x).replace('%', '')))
    rz.rename(columns={'ccmarginrate': 'rate'}, inplace=True)
    rq = df.loc[df['iscreditsharestk'] == 'Y'][['sec_type', 'sec_id', 'sec_code', 'csmarginrate']].copy()
    rq['csmarginrate'] = rq['csmarginrate'].apply(lambda x: int(str(x).replace('%', '')))
    rq.rename(columns={'csmarginrate': 'rate'}, inplace=True)

    jzd = df.loc[~df['cmorate'].isna()][['sec_type', 'sec_id', 'sec_code', 'groupid']].copy()
    jzd.rename(columns={'groupid': 'rate'}, inplace=True)
    jzd['rate'] = jzd['rate'].apply(lambda x: 1 if str(x).upper() == 'A' else 2 if str(x).upper() == 'B' else 3 if str(x).upper() == 'C' else 4 if str(x).upper() == 'D' else 5 if str(x).upper() == 'E' else 0)
    return biz_dt, dbq, jzd, rz, rq
