#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-11-04
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import re
import threading
from datetime import datetime

import pandas as pd
from io import StringIO

from data.ms.InternetBizDataParsingApp import cfg, get_collect_data
from database import MysqlClient
from util.logs_utils import logger
from data.ms.fdb import get_ex_discount_limit_rate
sz = r'(\d+)'
zm = r'[\u0041-\u005a|\u0061-\u007a]+'
zw = r'[\u4e00-\u9fa5]+'

_sec_db = MysqlClient(**cfg.get_content('pro_db_sec'))

db_biz_pro = MysqlClient(**cfg.get_content(f'pro_db_biz'))


def sec_db():
    global _sec_db
    if not _sec_db:
        _sec_db = MysqlClient(**cfg.get_content('pro_db_sec'))
    return _sec_db


def get_df_from_cdata(cdata):
    return cdata['data_source'].to_list()[0], pd.read_csv(StringIO(cdata['data_text'][0]), sep=",")


class Cache(object):

    _sic_df = pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec360_name', 'exchange_sec_name'])

    @classmethod
    def get_sic_df(cls):
        return Cache._sic_df.copy()

    @classmethod
    def set_sic_df(cls, sic_df):
        Cache._sic_df = sic_df
        return cls.get_sic_df()


def code_ref_id(biz_dt, _df, data_source, exchange=False):
    exchange_sec = get_exchange_sec_data(biz_dt)
    exchange_sec.rename(columns={'bo_id': 'sec_id', 'bo_id_type': 'sec_type', 'bo_id_code': 'sec_code'}, inplace=True)
    temp_df = pd.merge(_df[['sec_code']].astype(str), exchange_sec.astype(str), on='sec_code', how='left')
    df_ = pd.merge(_df, temp_df, on='sec_code', how='left')
    no_sec_id_df = df_.loc[df_['sec_id'].isna()]
    if not no_sec_id_df.empty:
        logger.warn(f"业务解析日期：{biz_dt}，{data_source}中本次需注册证券对象有({no_sec_id_df.index.size}只)：\n{no_sec_id_df[['market', 'sec_code', 'sec_name']].reset_index(drop=True)}")
    return df_.loc[~df_['sec_id'].isna()].copy()
    # df1 = _df.merge(get_sic_df(), how='left', on='sec_code')
    # no_sec_id_df = df1.loc[df1['sec_id'].isna()]
    # if not no_sec_id_df.empty:
    #     # 刷证券ID
    #     df1 = _df.merge(refresh_sic_df(no_sec_id_df, exchange), how='left', on='sec_code')
    #     zc_sec_df = df1.loc[df1['sec_id'].isna()][['market', 'sec_code', 'sec_name']]
    #     if not zc_sec_df.empty:
    #         # 监控并Email关键词：本次需注册证券对象有
    #         logger.warn(f"{data_source}中本次需注册证券对象有({zc_sec_df.index.size}只)：\n{zc_sec_df.reset_index(drop=True)}")
    # return df1.loc[~df1['sec_id'].isna()].copy()


def get_exchange_sec_data(biz_dt):
    sql = f"""
    select bo_id, bo_id_type, bo_id_code, bo_name, start_dt, end_dt
      from t_jbt_exchange_sec_bo 
      where data_status = 1 
      and start_dt <= '{biz_dt} 00:00:00'
    """
    return sec_db().select(sql)


def get_exchange_sec_data_for_jbt(biz_dt):
    sql = f"""
    select bo_id, bo_id_type, bo_id_code, bo_name, start_dt, end_dt
      from t_jbt_exchange_sec_bo 
      where data_status = 1 
      and start_dt <= '{biz_dt} 00:00:00'
      and end_dt > '{biz_dt} 00:00:00'
    """
    return sec_db().select(sql)


def get_special_sec_bo():
    sql = f"""
    select bo_id, bo_id_type, bo_id_code, special_sec_code, special_sec_name
    from t_parsing_special_sec_bo 
    where bo_status = 1 
    and data_status = 1
    """
    return db_biz_pro.select(sql)

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
    _df = pd.merge(df.astype(str), sbf_rate.astype(str), how="left", on="sec_id")
    no_rate_df = _df.loc[_df['rate'].isna()][['sec_type', 'sec_id', 'sec_code', 'sec_name']]
    if not no_rate_df.empty:
        # 监控并Email关键词：如下证券缺少交易所折算率上限
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        logger.warn(f"如下证券缺少交易所折算率上限({no_rate_df.index.size}只)：\n{no_rate_df.reset_index(drop=True)}")
    return _df


def char_split(value, split_length=2):
    if isinstance(value, str):
        length = len(value)
        if length > split_length:
            _value = []
            for i in range(length - split_length + 1):
                _value.append(value[i:i+split_length])
            return _value
    return [value]


def char_arr_split(values, split_length=2):
    if isinstance(values, list):
        _values = []
        for value in values:
            _values += char_split(value, split_length)
        return _values
    return values


def next_trading_day(date):
    import datetime
    """
    判断日期是否为周末，如果是周末，则将其转为下一个交易日在返回
    """
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    elif not isinstance(date, datetime.date):
        raise ValueError('日期格式不正确')
    if date.weekday() >= 5:
        date += datetime.timedelta(days=7 - date.weekday())
    return date


def match_sid_by_code_and_name(biz_dt, df, data_source):
    """"
    1、找出df中每个代码，在sid_df中只存在1个代码的代码，认为是相同代码
    2、再再出df中每个代码的前5位，在sid_df中只存在1个市场和证券类型的代码，认为是相同市场的相同证券类型
    """
    df = df[['sec_code', 'sec_name']]
    exchange_sec = get_exchange_sec_data(biz_dt)
    # 按照end_date排序，最新的在前面
    exchange_sec = exchange_sec.sort_values('end_dt', ascending=False)
    # 按照bo_id_code去重，保留最新的一行
    exchange_sec = exchange_sec.drop_duplicates(subset=['bo_id_code'], keep='first')
    exchange_sec = exchange_sec.rename(columns={'bo_id': 'sid', 'bo_id_type': 'stp', 'bo_id_code': 'scd', 'bo_name': 'scn'})
    exchange_sec[['sec_code', 'market']] = exchange_sec['scd'].str.split('.', n=1, expand=True)
    target_columns = df.columns.tolist() + ['stp', 'sid', 'scd']
    merged_df = pd.merge(df, exchange_sec, on='sec_code', how='left')
    # 不带市场后缀代码唯一匹配
    sec_id_df = merged_df.drop_duplicates(subset=['sec_code'], keep=False)
    duplicated_df = merged_df[merged_df.duplicated(subset=['sec_code'], keep='first')]
    sec_id_df = sec_id_df[['sec_code', 'sec_name', 'sid', 'stp', 'market']]
    # 已获取到sec_id的对象
    sec_id_df = sec_id_df.rename(columns={'sid': 'sec_id', 'stp': 'sec_type'})
    # duplicated_df = duplicated_df[['sec_code', 'sec_name']]
    merged_df = merged_df[['sec_code', 'scn', 'sid', 'stp', 'market', 'end_dt']]
    temp_df = pd.merge(duplicated_df[['sec_code', 'sec_name']], merged_df, on=['sec_code'], how='left')
    # 1对多的时候，通过end_dt过滤掉退市的数据，直接匹配剩下的
    mask = temp_df['sec_name'] != temp_df['scn']
    name_all_match_df = temp_df.drop(temp_df[mask].index)
    name_like_diff_df = duplicated_df[~duplicated_df['sec_code'].isin(name_all_match_df['sec_code'])]
    name_like_df = pd.merge(name_like_diff_df[['sec_code']], temp_df, on=['sec_code'], how='left')
    # 第一次模糊匹配后获取到sec_id的对象
    _like_name = pd.DataFrame(columns=name_all_match_df.columns)
    for index, row in name_like_df.iterrows():
        if str(row['sec_name']) in str(row['scn']) or str(row['scn']) in str(row['sec_name']):
            _like_name = pd.concat([_like_name, row[name_all_match_df.columns.tolist()].to_frame().T])

    sec_id_matched = pd.concat([name_all_match_df, _like_name])
    sec_id_matched.rename(columns={'sid': 'sec_id', 'stp': 'sec_type'}, inplace=True)
    sec_id_matched = sec_id_matched.drop(columns=['scn'])
    sec_id_matched = sec_id_matched.drop(columns=['end_dt'])
    # 模糊匹配第一次后，仍然无法获取sec_id的对象
    not_even_matched_df = name_like_diff_df[~name_like_diff_df['sec_code'].isin(_like_name['sec_code'])]
    not_even_matched = pd.merge(not_even_matched_df[['sec_code']], temp_df, on=['sec_code'], how='left')

    # 第二次模糊匹配后获取到sec_id的对象
    _like_name_df = pd.DataFrame(columns=name_all_match_df.columns)
    for index, row in not_even_matched_df.iterrows():
        sec = not_even_matched[not_even_matched['sec_code'] == row['sec_code']]
        sec_name = re.findall(sz, row['sec_name']) + char_arr_split(re.findall(zm, row['sec_name']),split_length=3) + char_arr_split(re.findall(zw, row['sec_name']))
        for idx, rw in sec.iterrows():
            _sec_name = re.findall(sz, rw['scn']) + char_arr_split(re.findall(zm, rw['scn']), split_length=3) + char_arr_split(re.findall(zw, rw['scn']))
            if len(set(sec_name) & set(_sec_name)) > 0:
                _like_name_df = pd.concat([_like_name_df, rw[name_all_match_df.columns.tolist()].to_frame().T])
                break

    # 模糊匹配第二次后，仍然无法获取sec_id的对象，直接异常告警处理
    not_even_matched_df_ = not_even_matched_df[~not_even_matched_df['sec_code'].isin(_like_name_df['sec_code'])]
    # 通过退市状态过滤
    not_matched_df = pd.merge(not_even_matched_df_[['sec_code']], temp_df, on=['sec_code'], how='left')
    not_matched_df['end_dt'] = not_matched_df['end_dt'].apply(lambda x: x.date())
    filtered_df = not_matched_df[(not_matched_df['end_dt'] > biz_dt) & (not_matched_df.duplicated(['sec_code'], keep=False))]
    filtered_df_ = filtered_df.drop_duplicates(subset='sec_code',keep=False)
    filtered_df_.rename(columns={'sid':'sec_id', 'stp':'sec_type'}, inplace=True)
    filtered_df_ = filtered_df_.drop(columns=['scn'])
    filtered_df_ = filtered_df_.drop(columns=['end_dt'])
    last_not_matched_df = filtered_df[~filtered_df['sec_code'].isin(filtered_df_['sec_code'])]
    last_not_matched_df = last_not_matched_df.drop_duplicates(subset='sec_code')
    if not last_not_matched_df.empty:
        s_df = get_data_log(biz_dt, last_not_matched_df)
        s_df['scd'] = s_df['sec_code']
        s_df = s_df.drop(columns=['sec_code'])
        find_df = pd.merge(s_df, exchange_sec, on=['scd'], how='left')
        find_df = find_df[['sec_code', 'sec_name', 'sid', 'stp', 'market']]
        find_df.rename(columns={'sid': 'sec_id', 'stp': 'sec_type'}, inplace=True)
        filtered_df_ = pd.concat([filtered_df_, find_df])
    # if not last_not_matched_df.empty:
    #     # 监控并Email关键词：如下证券对象无法识别
    #     logger.warn(f"解析业务日期:{biz_dt},{data_source}中如下证券对象无法识别\n {last_not_matched_df[['sec_code', 'sec_name']]}")
    _like_name.rename(columns={'sid': 'sec_id', 'stp': 'sec_type'}, inplace=True)
    _like_name = _like_name.drop(columns=['scn'])
    _like_name = _like_name.drop(columns=['end_dt'])
    _like_name_df.rename(columns={'sid':'sec_id', 'stp':'sec_type'}, inplace=True)
    _like_name_df = _like_name_df.drop(columns=['scn'])
    _like_name_df = _like_name_df.drop(columns=['end_dt'])

    data_df = pd.concat([sec_id_df, sec_id_matched, _like_name_df, filtered_df_])
    not_six_digits = ~data_df['sec_code'].apply(lambda x: bool(re.match('^\d{6}$', str(x))))
    data_df.drop(data_df.loc[not_six_digits].index, inplace=True)
    never_sec_id_df = data_df.loc[data_df['sec_id'].isna()]
    if not never_sec_id_df.empty:
        special_seb_df = get_special_sec_bo()
        special_seb_df.rename(columns={'special_sec_name': 'sec_name'}, inplace=True)
        special_seb_df[['sec_code', 'market']] = special_seb_df['bo_id_code'].str.split('.', n=1, expand=True)
        special_seb_df['sec_code'] = special_seb_df['sec_code'].apply(lambda x: int(x))
        spe_bo = pd.merge(never_sec_id_df, special_seb_df, on=['sec_code', 'sec_name'], how='left')
        spe_bo['sec_id'] = spe_bo['bo_id']
        spe_bo['sec_type'] = spe_bo['bo_id_type']
        spe_bo['market_x'] = spe_bo['market_y']
        spe_bo.rename(columns={'market_x': 'market'}, inplace=True)
        spe_bo = spe_bo[['sec_code', 'sec_name', 'sec_id', 'sec_type', 'market']]
        data_df = pd.concat([data_df, spe_bo])
    last_no_sec_id_df = data_df.loc[data_df['sec_id'].isna()]
    if not last_no_sec_id_df.empty:
        logger.warn(f"解析业务日期:{biz_dt},{data_source}证券对象无法识别,或需要注册证券对象有({last_no_sec_id_df.index.size}只)：\n{last_no_sec_id_df[['market', 'sec_code', 'sec_name']].reset_index(drop=True)}")
    return data_df


def get_data_log(dt, df):
    df = df[['sec_code', 'sec_name']]
    szdata = pd.read_csv(StringIO(get_collect_data('深圳交易所', 2, dt)['data_text'][0]), sep=",")
    szdata['sec_code'] = szdata['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    sz_ = pd.merge(df, szdata, on='sec_code', how='left')
    sz_['市场'] = 'SZ'
    bjdata = pd.read_csv(StringIO(get_collect_data('北京交易所', 2, dt)['data_text'][0]), sep=",")
    bjdata['sec_code'] = bjdata['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    bj_ = pd.merge(df, bjdata, on='sec_code', how='left')
    bj_ = bj_.drop(columns=['日期'])
    bj_['市场'] = 'BJ'
    shdata = pd.read_csv(StringIO(get_collect_data('上海交易所', 2, dt)['data_text'][0]), sep=",")
    shdata['sec_code'] = shdata['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
    sh_ = pd.merge(df, shdata, on='sec_code', how='left')
    sh_ = sh_.drop(columns=['日期'])
    sh_['市场'] = 'SH'
    dd = pd.concat([sh_, bj_ ,sz_])
    _like_name = pd.DataFrame(columns=df.columns)
    for index, row in dd.iterrows():
        if str(row['sec_name']) in str(row['证券简称']) or str(row['证券简称']) in str(row['sec_name']):
            _like_name = pd.concat([_like_name,row[dd.columns.tolist()].to_frame().T])
    if not _like_name.empty:
        _like_name['sec_code'] = _like_name['sec_code'] + '.' + _like_name['市场']
        _like_name = _like_name[['sec_code', 'sec_name']]
    return _like_name
