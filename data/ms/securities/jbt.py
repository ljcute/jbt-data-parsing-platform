#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2023/4/19 13:19
# @Site    : 
# @File    : jbt.py
# @Software: PyCharm
import pandas as pd

from data.ms.base_tools import get_exchange_sec_data_for_jbt
from database import MysqlClient
from data.ms.InternetBizDataParsingApp import cfg

_factor_db = MysqlClient(**cfg.get_content('pro_db_factor'))


def factor_db():
    global _factor_db
    if not _factor_db:
        _factor_db = MysqlClient(**cfg.get_content('pro_db_factor'))
    return _factor_db


def _get_format_df(biz_dt):
    e_sec_df = get_exchange_sec_data_for_jbt(biz_dt)
    jbt_model_rate_df = get_model_jbt_mt_conversion_rate_data(biz_dt)
    jbt_model_rate_df.rename(columns={'object_id': 'bo_id'}, inplace=True)
    jbt_model_group_df = get_model_jbt_mt_concentration_group(biz_dt)
    jbt_model_group_df.rename(columns={'object_id': 'bo_id'}, inplace=True)
    rate_df = pd.merge(e_sec_df[['bo_id','bo_id_type', 'bo_id_code']], jbt_model_rate_df[['bo_id', 'cur_score']], on='bo_id', how='right')
    rate_df.rename(columns={'bo_id': 'sec_id', 'bo_id_type': 'sec_type', 'bo_id_code': 'sec_code', 'cur_score': 'rate'}, inplace=True)
    rate_df = rate_df.loc[~rate_df['rate'].isna()]
    group_df = pd.merge(e_sec_df[['bo_id','bo_id_type', 'bo_id_code']], jbt_model_group_df[['bo_id', 'cur_score']], on='bo_id', how='right')
    group_df.rename(columns={'bo_id': 'sec_id', 'bo_id_type': 'sec_type', 'bo_id_code': 'sec_code', 'cur_score': 'rate'}, inplace=True)
    group_df['rate'] = group_df['rate'].apply(lambda x: 1 if str(x).upper() == 'A' else 2 if str(x).upper() == 'B' else 3 if str(x).upper() == 'C' else 4 if str(x).upper() == 'D' else 5 if str(x).upper() == 'E' else 6 if str(x).upper() == 'F' else x)
    group_df = group_df.loc[~group_df['rate'].isna()]
    rate_df['rate'] = rate_df['rate'].apply(lambda x: float(x))
    group_df['rate'] = group_df['rate'].apply(lambda x: float(x))
    return biz_dt, rate_df, group_df


def _format_dbq(cdata, market):
    return _get_format_df(cdata['biz_dt'][0])


def get_model_jbt_mt_conversion_rate_data(biz_dt):
    sql = f"""
    select *
      from t_objeva_model_jbt_mt_conversion_rate 
      where status_type = 1
      and start_dt <= '{biz_dt} 23:59:59'
      and end_dt > '{biz_dt} 00:00:00' 
    """
    return factor_db().select(sql)


def get_model_jbt_mt_concentration_group(biz_dt):
    sql = f"""
    select *
      from t_objeva_model_jbt_mt_concentration_group 
      where status_type = 1
      and start_dt <= '{biz_dt} 23:59:59'
      and end_dt > '{biz_dt} 00:00:00' 
    """
    return factor_db().select(sql)


