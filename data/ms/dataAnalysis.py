#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
@ss          :互联网业务数据解析APP
"""

import os
import sys
import traceback
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from config import Config
from database import MysqlClient
from util.logs_utils import logger


def pro_biz_db():
    global _db_biz_pro
    if not _db_biz_pro:
        _db_biz_pro = MysqlClient(**cfg.get_content(f'pro_db_biz'))
    return _db_biz_pro


def raw_db():
    global _raw_db
    if not _raw_db:
        _raw_db = MysqlClient(**cfg.get_content(f'{env}_db_raw'))
    return _raw_db


def biz_db():
    global _biz_db
    if not _biz_db:
        _biz_db = MysqlClient(**cfg.get_content(f'{env}_db_biz'))
    return _biz_db


def get_adjust_data(db, biz_dt):
    sql = f"""
    SELECT a.broker_code, a.broker_name,b.* FROM t_security_broker a,
    (SELECT broker_id, 
    (case biz_type when 1 then '融资标的' when 2 then '融券标的' when 3 then '担保券' when 4 then '集中度' else biz_type end) as biz_type, 
    adjust_type,
    (case adjust_type when 1 then '调入' when 2 then '调出' when 3 then '调高' when 4 then '调低' else adjust_type end) as adjust_type_cn, 
    count(*) as adjust_num
    FROM t_broker_mt_business_security 
    WHERE data_status=1 and start_dt <= '{biz_dt} 00:00:00' and end_dt > '{biz_dt} 00:00:00'
     and biz_type in (1,2,3) and start_dt='{biz_dt} 00:00:00'
    GROUP BY broker_id, biz_type, adjust_type) b 
    where a.broker_id=b.broker_id
    ORDER BY b.broker_id, biz_type, adjust_type
    """
    return db.select(sql)


def get_collected_data(data_source, biz_dt):
    if data_source == '交易所':
        data_source_str = f" in ('深圳交易所', '上海交易所', '北京交易所')"
    else:
        data_source_str = f" = '{data_source}'"
    sql = f"""
    select data_source, data_type, data_text, biz_dt, log_id
      from t_ndc_data_collect_log 
     where (data_source, data_type, log_id) in (select data_source, data_type, max(log_id) 
                       from t_ndc_data_collect_log 
                      where data_source {data_source_str}
                        and biz_dt='{biz_dt}' 
                        and data_status=1
                      group by data_source, data_type)
    """
    return raw_db().select(sql)


def get_pre_collected_data(data_source, biz_dt):
    if data_source == '交易所':
        data_source_str = f" in ('深圳交易所', '上海交易所', '北京交易所')"
    else:
        data_source_str = f" = '{data_source}'"
    sql = f"""
    select data_source, data_type, data_text as pre_data_text, biz_dt as pre_biz_dt, log_id as pre_log_id
      from t_ndc_data_collect_log 
     where (data_source, data_type, log_id) in (select data_source, data_type, max(log_id) 
                       from t_ndc_data_collect_log 
                      where data_source {data_source_str}
                        and biz_dt= (select max(biz_dt) from t_ndc_data_collect_log where data_source {data_source_str} and data_status=1 and biz_dt < '{biz_dt}')
                        and data_status=1
                      group by data_source, data_type)
    """
    return raw_db().select(sql)

def get_message(_adjust, row, rw, env, biz_dt):
    _in = _adjust.loc[_adjust['adjust_type'] == 1]
    _out = _adjust.loc[_adjust['adjust_type'] == 2]
    _up = _adjust.loc[_adjust['adjust_type'] == 3]
    _down = _adjust.loc[_adjust['adjust_type'] == 4]
    _in_size = 0
    _out_size = 0
    _up_size = 0
    _down_size = 0
    if not _in.empty:
        _in_size = _in['adjust_num'].tolist()[0]
    if not _out.empty:
        _out_size = _out['adjust_num'].tolist()[0]
    if not _up.empty:
        _up_size = _up['adjust_num'].tolist()[0]
    if not _down.empty:
        _down_size = _down['adjust_num'].tolist()[0]
    _message = f"{rw['data_source']} {row['biz_type']}    {biz_dt} {env} 业务数据： 调入{_in_size} 调出{_out_size} 调高{_up_size} 调低{_down_size} "
    # logger.info(_message)
    return _message


def handle_cmp(biz_dt):
    adjust = get_adjust_data(biz_db(), biz_dt)
    adjust['env'] = env
    adjust.sort_values(by=['biz_type', 'broker_id', 'adjust_type'], inplace=True, ascending=True)
    # logger.info(f"{biz_dt} {env}环境-调整数据({adjust.index.size}组)\n {adjust}")
    clm = adjust.columns.tolist()
    pro_adjust = get_adjust_data(pro_biz_db(), biz_dt)
    pro_adjust['env'] = 'pro'
    pro_adjust.sort_values(by=['biz_type', 'broker_id', 'adjust_type'], inplace=True, ascending=True)
    # logger.info(f"{biz_dt} PRO环境-调整数据({pro_adjust.index.size}组)\n {pro_adjust}")
    un = pd.concat([adjust, pro_adjust])
    duplicate = un[un.duplicated(subset=clm, keep=False)].copy()
    duplicate.sort_values(by=['biz_type', 'broker_id', 'adjust_type'], inplace=True, ascending=True)
    diff = un[~un.duplicated(subset=clm, keep=False)].copy()
    diff.sort_values(by=['biz_type', 'broker_id', 'adjust_type'], inplace=True, ascending=True)
    # logger.info(f"{biz_dt} 一致数据({duplicate.index.size}组)\n {duplicate}")
    # logger.info(f"{biz_dt} 不一致数据({diff.index.size}组)\n {diff}")
    # 仅对比担保券
    _diff = diff.loc[diff['biz_type'] == '担保券'].copy()
    # logger.info(f"{biz_dt} 不一致担保券({_diff.index.size}组)\n {_diff}")
    _diff.drop_duplicates(['broker_id', 'biz_type'], inplace=True)
    # logger.info(f"{biz_dt} 校验数据({_diff.index.size}组)\n {_diff}")
    _message = []
    _all = adjust.loc[adjust['biz_type'] == '担保券'].drop_duplicates(['broker_id', 'biz_type'])
    for index, row in _all.iterrows():
        if row['biz_type'] != '担保券':
            continue
        logs = get_collected_data(row['broker_name'], biz_dt[:10])
        if logs.empty:
            continue
        pre_logs = get_pre_collected_data(row['broker_name'], biz_dt[:10])
        if pre_logs.empty:
            logger.info(f"pre_logs is empty: broker_name={row['broker_name']}, biz_dt={biz_dt}")
            continue
        # 合并
        union = logs.merge(pre_logs, on=['data_source', 'data_type'])
        for idx, rw in union.iterrows():
            try:
                _data_source = rw['data_source']
                _data_type = int(rw['data_type'])
                if _data_type not in (2, 99):
                    continue
                pre = pd.read_csv(StringIO(rw['pre_data_text']), sep=",")
                cur = pd.read_csv(StringIO(rw['data_text']), sep=",")
                # 对比：每家不一样
                if _data_source in ('上海交易所', '深圳交易所'):
                    pre['证券代码'] = pre['证券代码'].astype('str')
                    cur['证券代码'] = cur['证券代码'].astype('str')
                    pre['key'] = pre['证券代码']
                    cur['key'] = cur['证券代码']
                    pre['pre_rate'] = None
                    cur['cur_rate'] = None
                elif _data_source in ('华泰证券',):
                    pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['exchangeType'].map(lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(x) == 3 else str(x))
                    cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['exchangeType'].map(lambda x: 'SZ' if int(x) == 2 else 'SH' if int(x) == 1 else 'BJ' if int(x) == 3 else str(x))
                    pre['pre_rate'] = pre['assureRatio'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['assureRatio'].apply(lambda x: int(x * 100))
                elif _data_source in ('国元证券',):
                    pre['key'] = pre['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                    cur['key'] = cur['secu_code'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                    pre['pre_rate'] = pre['exchange_rate'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['exchange_rate'].apply(lambda x: int(x * 100))
                elif _data_source in ('广发证券',):
                    pre['key'] = pre['0'].apply(lambda x: (str(x))[-6:])
                    cur['key'] = cur['0'].apply(lambda x: (str(x))[-6:])
                    pre['pre_rate'] = pre['1'].apply(lambda x: int(str(x).replace('%', '')))
                    cur['cur_rate'] = cur['1'].apply(lambda x: int(str(x).replace('%', '')))
                elif _data_source in ('安信证券',):
                    pre['key'] = pre['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
                    cur['key'] = cur['secuCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
                    pre['pre_rate'] = pre['collatRatio'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['collatRatio'].apply(lambda x: int(x * 100))
                elif _data_source in ('中信证券',):
                    pre['key'] = pre['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['exchangeCode'].map(lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(x))
                    cur['key'] = cur['stockCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['exchangeCode'].map(lambda x: 'SZ' if str(x) == '深圳' else 'SH' if str(x) == '上海' else 'BJ' if str(x) == '北京' else str(x))
                    pre['pre_rate'] = pre['percent'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['percent'].apply(lambda x: int(x * 100))
                elif _data_source in ('国泰君安',):
                    pre = pre.loc[pre['type'] == 1].copy()
                    cur = cur.loc[cur['type'] == 1].copy()
                    pre['key'] = pre['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(x) == '北交所' else str(x))
                    cur['key'] = cur['secCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['branch'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(x) == '北交所' else str(x))
                    pre['pre_rate'] = pre['rate'].apply(lambda x: int(str(x).replace('%', '')))
                    cur['cur_rate'] = cur['rate'].apply(lambda x: int(str(x).replace('%', '')))
                elif _data_source in ('中国银河',):
                    pre = pre.loc[pre['type'] == 'db'].copy()
                    cur = cur.loc[cur['type'] == 'db'].copy()
                    pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                    cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                    pre['pre_rate'] = pre['rate'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['rate'].apply(lambda x: int(x * 100))
                elif _data_source in ('招商证券',):
                    pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['market'].map(lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '3' else str(x))
                    cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['market'].map(lambda x: 'SZ' if str(x) == '2' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '3' else str(x))
                    pre['pre_rate'] = pre['pledgerate'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['pledgerate'].apply(lambda x: int(x * 100))
                elif _data_source in ('中信建投',):
                    pre = pre.loc[~pre['pledgerate'].isna()].copy()
                    cur = cur.loc[~cur['pledgerate'].isna()].copy()
                    pre['key'] = pre['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
                    cur['key'] = cur['stkcode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['market'].map(lambda x: 'SZ' if str(x) == '0' else 'SH' if str(x) == '1' else 'BJ' if str(x) == '2' else str(x))
                    pre['pre_rate'] = pre['pledgerate'].apply(lambda x: int(x))
                    cur['cur_rate'] = cur['pledgerate'].apply(lambda x: int(x))
                elif _data_source in ('国信证券',):
                    pre['key'] = pre['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                    cur['key'] = cur['zqdm'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):])
                    pre['pre_rate'] = pre['zsl'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['zsl'].apply(lambda x: int(x * 100))
                elif _data_source in ('兴业证券',):
                    pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['exchange'].str.upper()
                    cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['exchange'].str.upper()
                    pre['pre_rate'] = pre['折算率'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['折算率'].apply(lambda x: int(x * 100))
                elif _data_source in ('申万宏源',):
                    pre['key'] = pre['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['市场'].map(lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x == '北京' else x)
                    cur['key'] = cur['证券代码'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['市场'].map(lambda x: 'SZ' if x == '深圳' else 'SH' if x == '上海' else 'BJ' if x == '北京' else x)
                    pre['pre_rate'] = pre['折算率'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                    cur['cur_rate'] = cur['折算率'].apply(lambda x: int(str(x).replace('%', '').replace('-', '0')))
                elif _data_source in ('中金财富',):
                    pre['key'] = pre['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['exchange']
                    cur['key'] = cur['stockId'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['exchange']
                    pre['pre_rate'] = pre['rate'].apply(lambda x: int(x * 100))
                    cur['cur_rate'] = cur['rate'].apply(lambda x: int(x * 100))
                elif _data_source in ('中泰证券',):
                    pre['key'] = pre['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['BOURSE']
                    cur['key'] = cur['STOCK_CODE'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['BOURSE']
                    pre['pre_rate'] = pre['REBATE']
                    cur['cur_rate'] = cur['REBATE']
                elif _data_source in ('光大证券',):
                    pre['key'] = pre['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):]) + '.' + pre['证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(x) == '北A' else str(x))
                    cur['key'] = cur['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):]) + '.' + cur['证券市场'].map(lambda x: 'SZ' if str(x) == '深A' else 'SH' if str(x) == '沪A' else 'BJ' if str(x) == '北A' else str(x))
                    pre['pre_rate'] = pre['调整后折算率'].apply(lambda x: int(str(x).replace('%', '')))
                    cur['cur_rate'] = cur['调整后折算率'].apply(lambda x: int(str(x).replace('%', '')))
                elif _data_source in ('海通证券',):
                    pre = pre.loc[pre['ifGuarantee'] == 'Y'].copy()
                    cur = cur.loc[cur['ifGuarantee'] == 'Y'].copy()
                    pre['key'] = pre['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + pre['marketCode'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(x) == '北交所' else str(x))
                    cur['key'] = cur['productCode'].apply(lambda x: ('000000' + str(x))[-max(6, len(str(x))):]) + '.' + cur['marketCode'].map(lambda x: 'SZ' if str(x) == '深交所' else 'SH' if str(x) == '上交所' else 'BJ' if str(x) == '北交所' else str(x))
                    pre['pre_rate'] = pre['stockConvertRate'].apply(lambda x: int(str(x).replace('%', '')))
                    cur['cur_rate'] = cur['stockConvertRate'].apply(lambda x: int(str(x).replace('%', '')))
                elif _data_source in ('平安证券',):
                    pre['key'] = pre['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
                    cur['key'] = cur['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
                    pre['pre_rate'] = pre['折算率'].apply(lambda x: int(x*100))
                    cur['cur_rate'] = cur['折算率'].apply(lambda x: int(x*100))
                elif _data_source in ('中金公司',):
                    pre['key'] = pre['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
                    cur['key'] = cur['证券代码'].apply(lambda x: ('000000'+str(x))[-max(6, len(str(x))):])
                    pre['pre_rate'] = pre['中金折算率'].apply(lambda x: int(str(x).replace('%', '')))
                    cur['cur_rate'] = cur['中金折算率'].apply(lambda x: int(str(x).replace('%', '')))
                else:
                    logger.warning(f"fix {_data_source}")
                    continue
                _out = pre.loc[~pre['key'].isin(cur['key'].tolist())].copy()
                _in = cur.loc[~cur['key'].isin(pre['key'].tolist())].copy()
                _same = cur.merge(pre, on='key')
                _up = _same.loc[_same['pre_rate'] < _same['cur_rate']].copy()
                _down = _same.loc[_same['pre_rate'] > _same['cur_rate']].copy()
                _message.append(f"{rw['data_source']} {row['biz_type']}({rw['data_type']}) {biz_dt}    程序测试： 调入{_in.index.size} 调出{_out.index.size} 调高{_up.index.size} 调低{_down.index.size} ")
                # logger.info(_message[-1])
                file_name = f"{rw['data_source']}_{biz_type_map.get(int(rw['data_type']))}({rw['data_type']})_{biz_dt[:10]}"
                if not _out.empty or not _in.empty or not _up.empty or not _down.empty:
                    _out['adjust_type'] = 'out'
                    _in['adjust_type'] = 'in'
                    _up['adjust_type'] = 'up'
                    _down['adjust_type'] = 'down'
                    pd.concat([_in, _out, _up, _down]).to_csv(f"excel/{file_name}_adjust.csv", encoding="GBK")
                    # with pd.ExcelWriter('test.xlsx') as writer:
                    #     data.to_excel(writer, sheet_name='data')
            except Exception as err:
                logger.info(f"{rw['data_source']} ({rw['data_type']}) {err}")
        _adjust = adjust.loc[((adjust['broker_id'] == row['broker_id']) & (adjust['biz_type'] == row['biz_type']))]
        _message.append(get_message(_adjust, row, rw, env, biz_dt))
        _pro_adjust = pro_adjust.loc[((pro_adjust['broker_id'] == row['broker_id']) & (pro_adjust['biz_type'] == row['biz_type']))]
        _message.append(get_message(_pro_adjust, row, rw, 'pro', biz_dt))
    return _message


if __name__ == '__main__':
    try:
        __environments = ("loc", "dev", "fat", "uat", "pro")
        biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券",
                        4: "融资标的证券", 5: "融券标的证券", 7: "单一股票担保物比例信息", 99: "融资融券可充抵保证金证券和融资融券标的证券"}
        cfg = Config.get_cfg()
        if "environment" not in cfg.get_sections() or "env" not in cfg.get_options("environment"):
            raise Exception(f"请设置当前环境environment.env")
        env = cfg.get_content("environment").get("env")
        if env not in __environments:
            raise Exception(f"环境env只能是{__environments}范围内容")
        _raw_db = MysqlClient(**cfg.get_content(f'{env}_db_raw'))
        _biz_db = MysqlClient(**cfg.get_content(f'{env}_db_biz'))
        _db_biz_pro = MysqlClient(**cfg.get_content(f'pro_db_biz'))

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('max_colwidth', 200)
        pd.set_option('expand_frame_repr', False)

        # _start_dt = datetime.strptime('2022-11-21', '%Y-%m-%d')
        # _start_dt = datetime.strptime('2022-11-28', '%Y-%m-%d')
        _start_dt = datetime.strptime('2022-12-22', '%Y-%m-%d')

        # _end_dt = datetime.strptime('2022-11-06', '%Y-%m-%d')
        _end_dt = datetime.strptime('2022-12-22', '%Y-%m-%d')
        message = []
        for i in range((_end_dt - _start_dt).days + 1):
            dt = _start_dt + timedelta(days=i)
            message += handle_cmp(str(dt))
        logger.info('\n\n\n')
        for m in message:
            logger.info(m)
    except Exception as e:
        logger.error(f"互联网数据解析服务启动异常: {e} =》{str(traceback.format_exc())}")
