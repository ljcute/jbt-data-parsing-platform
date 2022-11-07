#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-11-04
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import threading
import pandas as pd
from io import StringIO
from util.logs_utils import logger
from data.ms.fdb import get_ex_discount_limit_rate
from data.ms.sec360 import get_sec360_sec_id_code, register_sec360_security


def get_df_from_cdata(cdata):
    return pd.read_csv(StringIO(cdata['data_text'][0]), sep=",")


class Cache(object):

    _sic_df = pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code'])

    @classmethod
    def get_sic_df(cls):
        return Cache._sic_df

    @classmethod
    def set_sic_df(cls, sic_df):
        Cache._sic_df = sic_df
        return Cache._sic_df


def get_sic_df():
    return Cache.get_sic_df()


def set_sic_df(_df):
    if _df.empty:
        return get_sic_df()
    mutex = threading.Lock()
    mutex.acquire(60)  # 里面可以加blocking(等待的时间)或者不加，不加就会一直等待(堵塞)
    df = Cache.set_sic_df(pd.concat([Cache.get_sic_df(), _df]).drop_duplicates())
    mutex.release()
    return df


def refresh_sic_df(_df):
    if _df.empty:
        return get_sic_df()
    return set_sic_df(get_sec360_sec_id_code(_df['sec_code'].tolist()))


def register_sic_df(_df):
    if _df.empty:
        return get_sic_df()
    return set_sic_df(register_sec360_security(_df['sec_code'].tolist()))


def code_ref_id(_df):
    df1 = _df.merge(get_sic_df(), how='left', on='sec_code')
    no_sec_id_df = df1.loc[df1['sec_id'].isna()]
    if not no_sec_id_df.empty:
        df1 = _df.merge(refresh_sic_df(no_sec_id_df), how='left', on='sec_code')
        # df有sec_code但是sic_df无，则注册对象
        zc_sec_df = df1.loc[df1['sec_id'].isna()]
        if not zc_sec_df.empty:
            # TODO Email
            logger.warn(f"本次注册对象有{zc_sec_df}")
            # 注册后用inner，目的是如果存在注册失败，这边照常解析入库，仅注册失败券受影响
            df1 = _df.merge(register_sic_df(zc_sec_df), on='sec_code')
    return df1


def get_exchange_discount_limit_rate(biz_dt, df):
    if df.empty:
        return
    s_df = df.loc[df['sec_type'] == 'stock']
    s_rate = get_ex_discount_limit_rate(biz_dt, 'stock', s_df['sec_id'].tolist())
    b_df = df.loc[df['sec_type'] == 'bond']
    b_rate = get_ex_discount_limit_rate(biz_dt, 'bond', b_df['sec_id'].tolist())
    f_df = df.loc[df['sec_type'] == 'fund']
    f_rate = get_ex_discount_limit_rate(biz_dt, 'fund', f_df['sec_id'].tolist())
    # 根据返回内容组装结果返回
    sbf_rate = pd.concat([s_rate, b_rate, f_rate])
    _df = pd.merge(df, sbf_rate, how="left", on="sec_id")
    no_rate_df = _df.loc[_df['rate'].isna()][['sec_type', 'sec_id', 'sec_code', 'sec_name']]
    if not no_rate_df.empty:
        # TODO Email
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        logger.warn(f"如下证券缺少交易所折算率上限({no_rate_df.index.size}只)：\n{no_rate_df.reset_index(drop=True)}")
    return _df
