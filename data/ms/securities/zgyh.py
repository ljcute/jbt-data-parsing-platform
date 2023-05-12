#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-10-31
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import pandas as pd
from data.ms.base_tools import get_df_from_cdata, match_sid_by_code_and_name, next_trading_day


def _get_format_df(cdata):
    data_source, df = get_df_from_cdata(cdata)
    biz_dt = cdata['biz_dt'].values[0]
    df['sec_code'] = df['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_name'] = df['证券简称']
    df['sec_name'] = df['sec_name'].str.replace(' ', '')
    t_df = df.copy()
    df.sort_values(by=["sec_code", "sec_name"], ascending=[True, True])
    dep_data = df.duplicated(["sec_code", "sec_name"]).sum()
    dep_line = df[df.duplicated(["sec_code", "sec_name"], keep='last')]  # 查看删除重复的行
    dep_list = dep_line.values.tolist()
    df.drop_duplicates(subset=["sec_code", "sec_name"], keep='first', inplace=True, ignore_index=False)
    biz_dt = next_trading_day(biz_dt)
    _df = match_sid_by_code_and_name(biz_dt, df, data_source)
    t_df['sec_id'] = t_df['sec_code'].apply(lambda x: (_df[_df['sec_code'] == x])['sec_id'].tolist()[0])
    t_df['sec_type'] = t_df['sec_code'].apply(lambda x: (_df[_df['sec_code'] == x])['sec_type'].tolist()[0])
    t_df['start_dt'] = None
    return biz_dt, t_df


def _format_db_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['rate'].apply(lambda x: int(x*100))
    dbq = df.loc[df['type'] == 'db'][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    rz = df.loc[df['type'] == 'rz'][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    rq = df.loc[df['type'] == 'rq'][['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame(), rz, rq
