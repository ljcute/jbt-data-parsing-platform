#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-10-31
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import pandas as pd
from data.ms.base_tools import match_sid_by_code_and_name, get_df_from_cdata, next_trading_day


def _get_format_df(cdata):
    data_source, df = get_df_from_cdata(cdata)
    biz_dt = cdata['biz_dt'].values[0]
    df['sec_code'] = df['securitiescode'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    df['sec_name'] = df['securitieshort'].str.replace(' ', '')
    biz_dt = next_trading_day(biz_dt)
    _df = match_sid_by_code_and_name(biz_dt, df, data_source)
    df = df.merge(_df, on=['sec_code', 'sec_name'])
    df['start_dt'] = None
    return biz_dt, df


def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['convertrate'].apply(lambda x: int(x*100))
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, dbq, pd.DataFrame()


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = 100
    df['rq_rate'] = 50
    rz = df.loc[df['margintradingtarget'] == '是'][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df.loc[df['financingtarget'] == '是'][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    return biz_dt, rz, rq


def _format_rz_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = 100
    rz = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, rz


def _format_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = 50
    rq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    return biz_dt, rq
