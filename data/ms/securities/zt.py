#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/12/6 14:00
# @Site    : 
# @File    : zt.py
# @Software: PyCharm
import time

import pandas as pd
from data.ms.base_tools import get_df_from_cdata, code_ref_id, next_trading_day
from util.logs_utils import logger


def _get_format_df(cdata):
    data_source, df = get_df_from_cdata(cdata)
    df['market'] = df['BOURSE'].str.strip()
    df['sec_code'] = df['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
    df['sec_code'] = df['sec_code'] + '.' + df['market']
    df['sec_name'] = df['STOCK_NAME']
    df['start_dt'] = None
    biz_dt = timeStamp(int(df['CREATE_TIME'].values[0][6:len(df['CREATE_TIME'].values[0]) - 2]))[:10]
    biz_dt = next_trading_day(biz_dt)
    return biz_dt, code_ref_id(biz_dt, df, data_source)


def timeStamp(timeNum):
    timeStamp = float(timeNum / 1000)
    timeArray = time.localtime(timeStamp)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return otherStyleTime

def _format_dbq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rate'] = df['REBATE']
    dbq = df[['sec_type', 'sec_id', 'sec_code', 'rate']].copy()
    jzd = df[['sec_type', 'sec_id', 'sec_code', 'OWN_GROUP_NAME']].copy()
    jzd.rename(columns={'OWN_GROUP_NAME': 'rate'}, inplace=True)
    jzd['rate'] = jzd['rate'].apply(lambda x: 1 if str(x) == 'A' else 2 if str(x) == 'B' else 3 if str(x) == 'C' else 4 if str(x) == 'D' else 5 if str(x) == 'E' else 6 if str(x) == 'F' else str(x))
    return biz_dt, dbq, jzd


def _format_rz_rq_bdq(cdata, market):
    biz_dt, df = _get_format_df(cdata)
    df['rz_rate'] = df['FUND_RATIOS']
    df['rq_rate'] = df['STOCK_RATIOS']
    rz = df.loc[df['STOCK_STATE'] == '可融资可融券'][['sec_type', 'sec_id', 'sec_code', 'rz_rate']].copy()
    rz.rename(columns={'rz_rate': 'rate'}, inplace=True)
    rq = df.loc[(df['STOCK_STATE'] == '可融资可融券') | (df['STOCK_STATE'] == '禁止融资可融券')][['sec_type', 'sec_id', 'sec_code', 'rq_rate']].copy()
    rq.rename(columns={'rq_rate': 'rate'}, inplace=True)
    rz['rate'] = rz['rate'].apply(lambda x: int(x))
    rz = rz[rz['rate'] >= 100]
    rq = rq[rq['rate'] >= 50]
    return biz_dt, rz, rq

