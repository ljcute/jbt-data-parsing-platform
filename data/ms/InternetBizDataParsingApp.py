#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
@ss          :互联网业务数据解析APP
"""

__author__ = 'Eagle (liuzh@igoldenbeta.com)'

import math
import os
import sys
import time
import traceback
import numpy as np
import pandas as pd
from decimal import Decimal
from kafka import KafkaConsumer
from datetime import datetime, date, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from config import Config
from database import MysqlClient
from util.logs_utils import logger


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


def std_dt(dt):
    if isinstance(dt, str):
        if len(dt) == 8:
            return datetime.strptime(dt, '%Y%m%d')
        if len(dt) == 10:
            if dt.find('-') > 0:
                return datetime.strptime(dt, '%Y-%m-%d')
            elif dt.find('/') > 0:
                return datetime.strptime(dt, '%Y/%m/%d')
        elif len(dt) == 19:
            if dt.find('-') > 0:
                return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            elif dt.find('/') > 0:
                return datetime.strptime(dt, '%Y/%m/%d %H:%M:%S')


def get_dt(dt):
    if dt is None or (isinstance(dt, str) and len(dt) == 0):
        return date.today()
    elif isinstance(dt, str) and len(dt) >= 8:
        return std_dt(dt)
    if not type(dt) in (datetime, date):
        msg = f'时间格式错误: {dt}'
        logger.error(msg)
        raise Exception(msg)
    return dt


def get_last_exchange_collect_date(days):
    sql = f"""
    select data_source, data_type, biz_dt
      from t_ndc_data_collect_log 
     where data_source in ('上海交易所', '深圳交易所')
       and data_type = 2
       and data_status = 1
  group by data_source, data_type, biz_dt
  order by biz_dt desc
  limit {2*days}
    """
    return raw_db().select(sql)


def get_collect_data(data_source, data_type, biz_dt):
    sql = f"""
    select * 
      from t_ndc_data_collect_log 
     where log_id = (select max(log_id) 
                       from t_ndc_data_collect_log 
                      where data_source='{data_source}'
                        and data_type={data_type}
                        and biz_dt='{biz_dt}' 
                        and data_status=1)
    """
    return raw_db().select(sql)


def get_brokers():
    sql = f"""
    select * 
      from t_security_broker
    """
    return biz_db().select(sql)


def market_sql_handle(market, sql):
    if isinstance(market, str):
        if market.upper() == 'SZ':
            sql += f" and data_desc = 1"
        elif market.upper() == 'SH':
            sql += f" and data_desc = 2"
        elif market.upper() == 'BJ':
            sql += f" and data_desc = 3"
    return sql


def get_broker_biz_data(broker_id, biz_dt, biz_type, market):
    sql = f"""
    select row_id, secu_id, secu_type, adjust_type, pre_value, cur_value, start_dt, end_dt
      from t_broker_mt_business_security
     where broker_id = {broker_id}
       and biz_type = {biz_type}
       and data_status = 1
       and start_dt <= '{biz_dt} 00:00:00'
       and end_dt > '{biz_dt} 00:00:00'
    """
    return biz_db().select(market_sql_handle(market, sql))


def persist_data(broker_id, biz_dt, biz_type, lgc_del, recovery, invalid, ist_df, market):
    # T日重采处理：逻辑删除错采数据
    if not lgc_del.empty:
        if lgc_del.index.size == 1:
            lgc_del_sub_sql = f" = {lgc_del['row_id'].values[0]}"
        else:
            lgc_del_sub_sql = f" in {tuple(lgc_del['row_id'].tolist())}"
        lgc_del_sql = f"""
            update t_broker_mt_business_security
               set data_status = 0,
                   update_dt = NOW()
             where row_id {lgc_del_sub_sql} 
               and broker_id = {broker_id}
               and biz_type = {biz_type}
            """
    # T日重采处理：恢复错采时失效的当前数据
    if not recovery.empty:
        if recovery.index.size == 1:
            recovery_sub_sql = f" = {recovery['secu_id'].values[0]}"
        else:
            recovery_sub_sql = f" in {tuple(recovery['secu_id'].tolist())}"
        recovery_sql = f"""
            update t_broker_mt_business_security
               set end_dt = '2999-12-31 23:59:59',
                   update_dt = NOW()
             where secu_id {recovery_sub_sql} 
               and broker_id = {broker_id}
               and biz_type = {biz_type}
               and start_dt <= DATE_ADD('{biz_dt} 00:00:00', INTERVAL -1 DAY)
               and end_dt > DATE_ADD('{biz_dt} 00:00:00', INTERVAL -1 DAY)
               and data_status = 1
            """
    if not invalid.empty:
        if invalid.index.size == 1:
            invalid_sub_sql = f" = {invalid['row_id'].values[0]}"
        else:
            invalid_sub_sql = f" in {tuple(invalid['row_id'].tolist())}"
        invalid_sql = f"""
            update t_broker_mt_business_security
               set end_dt = '{biz_dt} 00:00:00',
                   update_dt= NOW()
             where row_id {invalid_sub_sql} 
               and broker_id = {broker_id}
               and biz_type = {biz_type}
            """
    if not ist_df.empty:
        ist_sql = f"""
            INSERT INTO t_broker_mt_business_security(broker_id, secu_id, secu_type, biz_type, pre_value,
                        cur_value, adjust_type, data_status, biz_status, start_dt, end_dt, data_desc, create_dt, update_dt) 
            VALUES ({broker_id}, %s, %s, {biz_type}, %s, %s, %s, 1, 1, '{biz_dt} 00:00:00', '2999-12-31 23:59:59', {1 if market == 'SZ' else 2 if market == 'SH' else 3 if market == 'BJ' else 'NULL'}, now(), now())
            """
    cnx = None
    try:
        cnx = biz_db().get_cnx()
        # T日重采处理：逻辑删除错采数据
        if not lgc_del.empty:
            # 逻辑删除错采数据
            biz_db().execute_uncommit(cnx, lgc_del_sql)
        # T日重采处理：恢复错采时失效的当前数据
        if not recovery.empty:
            # 恢复错采时失效的当前数据
            biz_db().execute_uncommit(cnx, recovery_sql)
        if not invalid.empty:
            # 失效被调整记录
            biz_db().execute_uncommit(cnx, invalid_sql)
        if not ist_df.empty:
            # 插入调整数据
            data = np.where(ist_df.isna(), None, ist_df.values).tolist()
            biz_db().execute_uncommit(cnx, ist_sql, data)
        if cnx:
            cnx.commit()
    except Exception as err:
        logger.error(f"互联网数据解析异常：{err} =》{str(traceback.format_exc())}")
        if cnx:
            cnx.rollback()
    finally:
        if cnx:
            cnx.close()


def handle_range_collected_data(data_source, data_type, start_dt=None, end_dt=None, persist_flag=True):
    try:
        _start_dt = get_dt(start_dt)
        if isinstance(_start_dt, datetime):
            _start_dt = _start_dt.date()
        if end_dt is None:
            _end_dt = _start_dt
        else:
            _end_dt = get_dt(end_dt)
            if isinstance(_end_dt, datetime):
                _end_dt = _end_dt.date()
        global brokers
        brokers = get_brokers()
        for i in range((_end_dt - _start_dt).days + 1):
            dt = _start_dt + timedelta(days=i)
            logger.info(f"开始计算data_source={data_source} data_type={data_type} {dt}")
            # 取biz_dt日，最新数据
            cdata = get_collect_data(data_source, data_type, dt)
            # 如果没, 则不解析（continue）
            if cdata.empty:
                continue
            # 如果有则解析
            else:
                handle_collected_data(cdata, dt, persist_flag)
            logger.info(f"结束计算data_source={data_source} data_type={data_type} {dt}")
    except Exception as e:
        logger.error(f"互联网数据解析服务解析异常data_source={data_source} data_type={data_type} {dt}: {e} =》{str(traceback.format_exc())}")


def check_biz_dt(dt, biz_dt):
    if f"{dt}" != f"{biz_dt}":
        logger.info(f"collect date={dt}, data date is {biz_dt}")


def handle_collected_data(cdata, dt, persist_flag=True):
    data_source = cdata['data_source'][0]
    data_type = int(cdata['data_type'][0])
    global brokers
    broker = brokers.loc[(brokers['broker_name'] == data_source) | (brokers['broker_name'] == data_source[2:])]
    broker_id = broker['broker_id'].values[0]
    market = None
    if data_source[2:] == '交易所':
        if data_source[:2] == '深圳':
            market = 'SZ'
        elif data_source[:2] == '上海':
            market = 'SH'
        elif data_source[:2] == '北京':
            market = 'BJ'

    if data_type == 0:
        print(0)
    elif data_type == 1:
        print(1)
    elif data_type == 2:
        biz_dt, dbq, jzd = format_dbq(broker, cdata, market)
        check_biz_dt(dt, biz_dt)
        handle_dbq(broker_id, biz_dt, dbq, market, persist_flag)
        handle_dbq_jzd(broker_id, biz_dt, jzd, market, persist_flag)
    elif data_type == 3:
        biz_dt, rz_bdq, rq_bdq = format_rz_rq_bdq(broker, cdata, market)
        check_biz_dt(dt, biz_dt)
        handle_rz_bdq(broker_id, biz_dt, rz_bdq, market, persist_flag)
        handle_rq_bdq(broker_id, biz_dt, rq_bdq, market, persist_flag)
    elif data_type == 4:
        biz_dt, rz_bdq = format_rz_bdq(broker, cdata, market)
        check_biz_dt(dt, biz_dt)
        handle_rz_bdq(broker_id, biz_dt, rz_bdq, market, persist_flag)
    elif data_type == 5:
        biz_dt, rq_bdq = format_rq_bdq(broker, cdata, market)
        check_biz_dt(dt, biz_dt)
        handle_rq_bdq(broker_id, biz_dt, rq_bdq, market, persist_flag)
    elif data_type == 99:
        biz_dt, dbq, jzd, rz_bdq, rq_bdq = format_db_rz_rq_bdq(broker, cdata, market)
        check_biz_dt(dt, biz_dt)
        handle_dbq(broker_id, biz_dt, dbq, market, persist_flag)
        handle_dbq_jzd(broker_id, biz_dt, jzd, market, persist_flag)
        handle_rz_bdq(broker_id, biz_dt, rz_bdq, market, persist_flag)
        handle_rq_bdq(broker_id, biz_dt, rq_bdq, market, persist_flag)


def format_dbq(broker, cdata, market):
    exec(f"from data.ms.securities.{broker['broker_code'].values[0].lower()} import _format_dbq")
    return eval(f"_format_dbq(cdata, market)")


def format_rz_bdq(broker, cdata, market):
    exec(f"from data.ms.securities.{broker['broker_code'].values[0].lower()} import _format_rz_bdq")
    return eval(f"_format_rz_bdq(cdata, market)")


def format_rq_bdq(broker, cdata, market):
    exec(f"from data.ms.securities.{broker['broker_code'].values[0].lower()} import _format_rq_bdq")
    return eval(f"_format_rq_bdq(cdata, market)")


def format_rz_rq_bdq(broker, cdata, market):
    exec(f"from data.ms.securities.{broker['broker_code'].values[0].lower()} import _format_rz_rq_bdq")
    return eval(f"_format_rz_rq_bdq(cdata, market)")


def format_db_rz_rq_bdq(broker, cdata, market):
    exec(f"from data.ms.securities.{broker['broker_code'].values[0].lower()} import _format_db_rz_rq_bdq")
    return eval(f"_format_db_rz_rq_bdq(cdata, market)")


def handle_dbq(_broker_id, _biz_dt, _dbq, market, persist_flag=True):
    biz_type = 3
    handle_data(_broker_id, _biz_dt, biz_type, _dbq, market, persist_flag)


def handle_rz_bdq(_broker_id, _biz_dt, _rz_bdq, market, persist_flag=True):
    biz_type = 1
    handle_data(_broker_id, _biz_dt, biz_type, _rz_bdq, market, persist_flag)


def handle_rq_bdq(_broker_id, _biz_dt, _rq_bdq, market, persist_flag=True):
    biz_type = 2
    handle_data(_broker_id, _biz_dt, biz_type, _rq_bdq, market, persist_flag)


def handle_dbq_jzd(_broker_id, _biz_dt, _rq_bdq, market, persist_flag=True):
    biz_type = 4
    handle_data(_broker_id, _biz_dt, biz_type, _rq_bdq, market, persist_flag)


def handle_duplicate_data(broker_id, biz_type, data):
    if data.empty:
        return False
    # 处理数据库中可能存在的重复数据(问题数据)：重复，保留start_dt最早记录
    data.sort_values(by=['secu_id', 'start_dt', 'adjust_type', 'cur_value'], inplace=True, ascending=True)
    duplicate = data[data.duplicated(subset=['secu_id'])]
    # 删除冗余记录
    if duplicate.empty:
        return False
    if duplicate.index.size == 1:
        duplicate_sub_sql = f" = {duplicate['row_id'].values[0]}"
    else:
        duplicate_sub_sql = f" in {tuple(duplicate['row_id'].tolist())}"
    duplicate_sql = f"""
            update t_broker_mt_business_security
               set data_status = 0,
                   update_dt = NOW()
             where row_id {duplicate_sub_sql} 
               and broker_id = {broker_id}
               and biz_type = {biz_type}
            """
    # 逻辑删除重复数据
    biz_db().update(duplicate_sql)
    return True


def delete_after_dt_data(broker_id, biz_dt, biz_type, market):
    # T日之后所有日已采有效数据做逻辑删除处理
    t1_del_sql = f"""
            update t_broker_mt_business_security
                   set data_status = 0,
                       update_dt = NOW()
                 where broker_id = {broker_id}
                   and biz_type = {biz_type}
                   and data_status = 1
                   and start_dt > '{biz_dt} 00:00:00'
        """
    # T日有效数据，失效时间统一处理成'2999-12-31 23:59:59'
    t_valid_sql = f"""
            update t_broker_mt_business_security
                   set end_dt = '2999-12-31 23:59:59',
                       update_dt = NOW()
                 where broker_id = {broker_id}
                   and biz_type = {biz_type}
                   and data_status = 1
                   and start_dt <= '{biz_dt} 00:00:00'
                   and end_dt > '{biz_dt} 00:00:00'
                   and end_dt != '2999-12-31 23:59:59'
        """
    try:
        cnx = biz_db().get_cnx()
        # T日之后所有日已采有效数据做逻辑删除处理
        biz_db().execute_uncommit(cnx, market_sql_handle(market, t1_del_sql))
        # T日有效数据，失效时间统一处理成'2999-12-31 23:59:59'
        biz_db().execute_uncommit(cnx, market_sql_handle(market, t_valid_sql))
        if cnx:
            cnx.commit()
    except Exception as err:
        logger.error(f"互联网数据解析异常：{err} =》{str(traceback.format_exc())}")
        if cnx:
            cnx.rollback()
    finally:
        if cnx:
            cnx.close()


def fix_dt_data(lgc_del, forever, fix_df, broker_id, biz_type, biz_dt):
    if len(lgc_del) > 0:
        if len(lgc_del) == 1:
            lgc_del_sub_sql = f" = {lgc_del[0]}"
        else:
            lgc_del_sub_sql = f" in {tuple(lgc_del)}"
        lgc_del_sql = f"""
            update t_broker_mt_business_security
               set data_status = 0,
                   update_dt= NOW()
             where row_id {lgc_del_sub_sql} 
               and broker_id = {broker_id}
               and biz_type = {biz_type}
               and start_dt = '{biz_dt} 00:00:00'
            """
    if len(forever) > 0:
        if len(forever) == 1:
            forever_sub_sql = f" = {forever[0]}"
        else:
            forever_sub_sql = f" in {tuple(forever)}"
        forever_sql = f"""
            update t_broker_mt_business_security
               set end_dt = '2999-12-31 23:59:59',
                   update_dt = NOW()
             where row_id {forever_sub_sql} 
               and broker_id = {broker_id}
               and biz_type = {biz_type}
               and end_dt = '{biz_dt} 00:00:00'
            """
    if not fix_df.empty:
        fix_sql = f"""
            update t_broker_mt_business_security
               set adjust_type = %s,
                   pre_value = %s,
                   cur_value = %s,
                   update_dt = NOW()
             where row_id = %s
               and broker_id = {broker_id}
               and biz_type = {biz_type}
               and start_dt = '{biz_dt} 00:00:00'
            """
    try:
        cnx = biz_db().get_cnx()
        # T日之后所有日已采有效数据做逻辑删除处理
        if len(lgc_del) > 0:
            biz_db().execute_uncommit(cnx, lgc_del_sql)
        if len(forever) > 0:
            biz_db().execute_uncommit(cnx, forever_sql)
        if not fix_df.empty:
            data = np.where(fix_df.isna(), None, fix_df.values).tolist()
            biz_db().execute_uncommit(cnx, fix_sql, data)
        if cnx:
            cnx.commit()
    except Exception as err:
        logger.error(f"互联网数据解析异常：{err} =》{str(traceback.format_exc())}")
        if cnx:
            cnx.rollback()
    finally:
        if cnx:
            cnx.close()


def get_and_fix_cur_db_data(_broker_id, _biz_dt, _biz_type, market, data):
    # 删除_biz_dt之后的数据
    delete_after_dt_data(_broker_id, _biz_dt, _biz_type, market)
    while True:
        # 取当前数据
        cur_data = get_broker_biz_data(_broker_id, _biz_dt, _biz_type, market)
        # 修复当前重复数据
        cur_data_has_duplicate = handle_duplicate_data(_broker_id, _biz_type, cur_data)
        # 前日
        _pre_biz_dt = (datetime.strptime(str(_biz_dt), '%Y-%m-%d').date() + timedelta(days=-1)).strftime("%Y-%m-%d")
        # 取前日数据
        pre_data = get_broker_biz_data(_broker_id, _pre_biz_dt, _biz_type, market)
        # 修复前日重复数据
        pre_data_has_duplicate = handle_duplicate_data(_broker_id, _biz_type, pre_data)
        if not cur_data_has_duplicate and not pre_data_has_duplicate:
            if not pre_data.empty:
                pre_data['row_id'] = pre_data['row_id'].astype('str')
                pre_data['secu_id'] = pre_data['secu_id'].astype('str')
            if cur_data.empty:
                return cur_data
            cur_data['row_id'] = cur_data['row_id'].astype('str')
            cur_data['secu_id'] = cur_data['secu_id'].astype('str')
            # 提取当日新增数据
            cur_add_data = cur_data.loc[cur_data['start_dt'] == f'{_biz_dt} 00:00:00']
            # 当日无新增数据，无需处理错误数据，直接返回
            if cur_add_data.empty:
                return cur_data
            # 修复当日数据前值和调整类型
            pre_data = pre_data.rename(columns={'secu_id': 'pre_secu_id', 'row_id': 'pre_row_id', 'adjust_type': 'pre_adjust_type', 'cur_value': 'pre_cur_value'})[['pre_secu_id', 'pre_row_id', 'pre_adjust_type', 'pre_cur_value']]
            # 前与已有
            cur_add_data = cur_add_data.merge(pre_data, how='left', left_on='secu_id', right_on='pre_secu_id')
            # 前与已有 + 目标值
            cur_add_data = cur_add_data.merge(data, how='left', left_on='secu_id', right_on='sec_id')
            # 遍历比较
            cur_add_data['rate'] = cur_add_data['rate'].apply(lambda x: Decimal(str(x)))
            n_one = Decimal('-1')
            cur_add_data['rate'].fillna(n_one, inplace=True)
            cur_add_data['cur_value'].fillna(n_one, inplace=True)
            cur_add_data['pre_value'].fillna(n_one, inplace=True)
            cur_add_data['pre_cur_value'].fillna(n_one, inplace=True)
            lgc_del = []
            forever = []
            fix_df = pd.DataFrame()
            for idx, rw in cur_add_data.iterrows():
                fix_flag = False
                # 从无
                if isinstance(rw['pre_secu_id'], float) and math.isnan(rw['pre_secu_id']):
                    # 到无，中间记录多余，逻辑删除
                    if isinstance(rw['sec_id'], float) and math.isnan(rw['sec_id']):
                        lgc_del.append(rw['row_id'])
                    # 到有，为调入、前值为空、当前值有
                    else:
                        # 调整类型错误
                        if rw['adjust_type'] != 1:
                            rw['adjust_type'] = 1
                            fix_flag = True
                        # 前值错误
                        if rw['pre_value'] != n_one:
                            rw['pre_value'] = None
                            fix_flag = True
                        # 当前值不准，则修复
                        if rw['cur_value'] != rw['rate']:
                            rw['cur_value'] = rw['rate']
                            fix_flag = True
                # 从有
                else:
                    # 到无，为调出、前值有、当前值为空
                    if isinstance(rw['sec_id'], float) and math.isnan(rw['sec_id']):
                        # 调整类型错误
                        if rw['adjust_type'] != 2:
                            rw['adjust_type'] = 2
                            fix_flag = True
                        # 前值错误
                        if rw['pre_value'] != rw['pre_cur_value']:
                            rw['pre_value'] = rw['pre_cur_value']
                            fix_flag = True
                        # 当前值错误
                        if rw['cur_value'] != n_one:
                            rw['cur_value'] = None
                            fix_flag = True
                    # 到有
                    else:
                        # 前后值一致，中间记录多余，逻辑删除；前记录延伸到永久
                        if rw['pre_cur_value'] == rw['rate']:
                            lgc_del.append(rw['row_id'])
                            forever.append(rw['pre_row_id'])
                        else:
                            # 前后值不一致，调高
                            if rw['pre_cur_value'] < rw['rate']:
                                # 调整类型错误
                                if rw['adjust_type'] != 3:
                                    rw['adjust_type'] = 3
                                    fix_flag = True
                            # 前后值不一致，调低
                            elif rw['pre_cur_value'] > rw['rate']:
                                # 调整类型错误
                                if rw['adjust_type'] != 4:
                                    rw['adjust_type'] = 4
                                    fix_flag = True
                            # 前值错误
                            if rw['pre_value'] != rw['pre_cur_value']:
                                rw['pre_value'] = rw['pre_cur_value']
                                fix_flag = True
                            # 当前值不准，则修复
                            if rw['cur_value'] != rw['rate']:
                                rw['cur_value'] = rw['rate']
                                fix_flag = True
                if fix_flag:
                    fix_df = pd.concat([fix_df, rw.to_frame().T])
            if len(lgc_del) > 0 or len(forever) > 0 or not fix_df.empty:
                if not fix_df.empty:
                    fix_df = fix_df[['adjust_type', 'pre_value', 'cur_value', 'row_id']].copy()
                    fix_df = fix_df.replace(n_one, None)
                # 修复数据
                fix_dt_data(lgc_del, forever, fix_df, _broker_id, _biz_type, _biz_dt)
            else:
                return cur_data


def handle_data(_broker_id, _biz_dt, _biz_type, _data, market, persist_flag=True):
    """
    数据持久化逻辑
    """
    if not persist_flag:
        return
    logger.info(f"begin handle_data {_broker_id} {_biz_dt} {_biz_type} data size({_data.index.size}) market({market}) {persist_flag}")
    data = pd.DataFrame(columns=['sec_id', 'sec_type', 'cur_value', 'rate'])
    if not _data.empty:
        data = _data.copy()
        # 去重, 取控制严格数据
        if _biz_type in (1, 2, 4):
            # 去重,取最大，即倒序
            data.sort_values(by=['sec_id', 'rate'], inplace=True, ascending=False)
        else:
            # 去重, 取最小，即升序
            data.sort_values(by=['sec_id', 'rate'], inplace=True, ascending=True)
        data.drop_duplicates(['sec_id'], inplace=True)
        data['sec_id'] = data['sec_id'].astype('str')
    # 查询和修复数据库数据
    cur_data = get_and_fix_cur_db_data(_broker_id, _biz_dt, _biz_type, market, data)
    if data.empty:
        return
    # 对比数据 adjust_type: 调入1, 调出2, 调高3, 调低4
    if cur_data.empty:
        lgc_del = pd.DataFrame()
        recovery = lgc_del
        invalid = lgc_del
        data['cur_value'] = None
        ist_df = data[['sec_id', 'sec_type', 'cur_value', 'rate']]
        ist_df = ist_df.rename(columns={'sec_id': 'secu_id'})
        ist_df['adjust_type'] = 1
    else:
        # 数据关联
        _df = cur_data.merge(data, how='outer', left_on='secu_id', right_on='sec_id')
        # 过滤掉相同调出[含当日调出]
        _df = _df.loc[~((_df['adjust_type'] == 2) & (_df['sec_id'].isna()))]
        # 过滤掉相同值[含当日调入]
        diff_df = _df.loc[~((_df['adjust_type'].isin([1, 3, 4])) & ((_df['rate'] == _df['cur_value']) | (~_df['sec_id'].isna() & ~_df['secu_id'].isna() & _df['rate'].isna() & _df['cur_value'].isna())))]
        # 新增调入
        in_df = diff_df.loc[((diff_df['adjust_type'] == 2) & (~diff_df['sec_id'].isna())) | diff_df['row_id'].isna()]
        # 过滤掉新增券
        diff_df = diff_df.loc[~diff_df['row_id'].isna()]
        # 新增调出
        out_df = diff_df.loc[(diff_df['adjust_type'].isin([1, 3, 4])) & (diff_df['sec_id'].isna())]
        # 新增调高调低
        ud_df = diff_df.loc[(~diff_df['row_id'].isin(out_df['row_id'].tolist())) & (~diff_df['sec_id'].isin(in_df['sec_id'].tolist()))]
        # 构造持久化数据
        # 调出中逻辑删除的
        lgc_del1 = out_df.loc[out_df['start_dt'] == f'{_biz_dt} 00:00:00']
        # 逻辑删除中调入的，把先前的调出延长结束时间到永久
        recovery1 = out_df.loc[(out_df['start_dt'] == f'{_biz_dt} 00:00:00') & out_df['adjust_type'] == 1][['secu_id']]
        # recovery1['adjust_type'] = 2
        # 真实新增调出(排除逻辑删除恢复先前已是调出状态的)
        out_df = out_df.loc[~((out_df['start_dt'] == f'{_biz_dt} 00:00:00') & out_df['adjust_type'] == 1)]
        # 调高调低中逻辑删除的
        lgc_del2 = diff_df.loc[diff_df['start_dt'] == f'{_biz_dt} 00:00:00']
        # 需逻辑删除的
        lgc_del = pd.concat([lgc_del1, lgc_del2])
        # 逻辑删除中调高调低的，比较调整前值，如果与当前新值一致，则把先前的延长结束时间到永久
        recovery2 = diff_df.loc[(diff_df['start_dt'] == f'{_biz_dt} 00:00:00') & ((diff_df['pre_value'] == diff_df['rate']) | (diff_df['pre_value'].isna() & diff_df['rate'].isna()))][['secu_id']]
        # 真实新增调高调低(排除逻辑删除恢复先前已是有效状态的)
        diff_df = diff_df.loc[~((diff_df['start_dt'] == f'{_biz_dt} 00:00:00') & ((diff_df['pre_value'] == diff_df['rate']) | (diff_df['pre_value'].isna() & diff_df['rate'].isna())))]
        # 需恢复拉链的
        recovery = pd.concat([recovery1, recovery2])
        # 需切断拉链失效的
        invalid1 = out_df.loc[out_df['start_dt'] != f'{_biz_dt} 00:00:00'][['row_id']]
        invalid2 = diff_df.loc[diff_df['start_dt'] != f'{_biz_dt} 00:00:00'][['row_id']]
        invalid = pd.concat([invalid1, invalid2])
        # 新插入新的：调入调出调高调低
        ist_out = out_df[['secu_id', 'secu_type', 'cur_value', 'rate']].copy()
        ist_out['adjust_type'] = 2
        ist_in = in_df[['sec_id', 'sec_type', 'cur_value', 'rate']].copy()
        ist_in.rename(columns={'sec_id': 'secu_id', 'sec_type': 'secu_type'}, inplace=True)
        ist_in['adjust_type'] = 1
        ist_up = ud_df.loc[((ud_df['cur_value'] < ud_df['rate']) | (ud_df['cur_value'].isna() & ~ud_df['rate'].isna()))][['secu_id', 'secu_type', 'cur_value', 'rate']].copy()
        ist_up['adjust_type'] = 3
        ist_down = ud_df.loc[((ud_df['cur_value'] > ud_df['rate']) | (~ud_df['cur_value'].isna() & ud_df['rate'].isna()))][['secu_id', 'secu_type', 'cur_value', 'rate']].copy()
        ist_down['adjust_type'] = 4
        ist_df = pd.concat([ist_out, ist_in, ist_up, ist_down])
    persist_data(_broker_id, _biz_dt, _biz_type, lgc_del, recovery, invalid, ist_df, market)


def kafka_mq_consumer():
    """
    从mq接收消息
    """
    consumer = KafkaConsumer(
        cfg.get_content("kafka").get("topic"), bootstrap_servers=cfg.get_content("kafka")['kafkalist'],
        auto_offset_reset='earliest', group_id=cfg.get_content("kafka").get("group"),
        consumer_timeout_ms=1000, enable_auto_commit=False, max_poll_interval_ms=86400000
    )
    logger.info(f'============================')
    logger.info(f'数据解析开始......')
    while True:
        for msg in consumer:
            try:
                recv = msg.value.decode('unicode_escape')
                recv = recv[1:-1]
                recv_ = eval(recv)
                mq_content = {'biz_dt': recv_['biz_dt'], 'data_type': recv_['data_type'],
                              'data_source': recv_['data_source'], 'message': recv_['message']}
                data_source_info = recv_['data_source']
                data_type_info = recv_['data_type']
                biz_dt_info = recv_['biz_dt']
                logger.info(f'此次消费的消息内容为：{mq_content}')
                if recv_:
                    handle_range_collected_data(data_source_info, data_type_info, biz_dt_info, biz_dt_info)
                else:
                    logger.error(f'消费消息失败，请检查{mq_content}')
                consumer.commit()
                logger.info('此次消息已消费完成!')
                time.sleep(1)
            except Exception as es:
                logger.error(f'{data_source_info}的{biz_type_map.get(data_type_info)}解析任务失败，biz_dt为：{biz_dt_info}，请检查！具体异常信息为：{traceback.format_exc()}，Exception:{es}')


if __name__ == '__main__':
    try:
        __environments = ("loc", "dev", "fat", "uat", "pro")
        biz_type_map = {0: "交易所交易总量", 1: "交易所交易明细", 2: "融资融券可充抵保证金证券", 3: "融资融券标的证券",
                        4: "融资标的证券", 5: "融券标的证券", 99: "融资融券可充抵保证金证券和融资融券标的证券"}
        cfg = Config.get_cfg()
        if "environment" not in cfg.get_sections() or "env" not in cfg.get_options("environment"):
            raise Exception(f"请设置当前环境environment.env")
        env = cfg.get_content("environment").get("env")
        if env not in __environments:
            raise Exception(f"环境env只能是{__environments}范围内容")
        _raw_db = MysqlClient(**cfg.get_content(f'{env}_db_raw'))
        _biz_db = MysqlClient(**cfg.get_content(f'{env}_db_biz'))
        brokers = get_brokers()
        # 启动应用跑交易所数据，填充共享内存证券代码与对象ID内容
        exchange_df = get_last_exchange_collect_date(2).sort_values(by='biz_dt', axis=0, ascending=True)
        for index, row in exchange_df.iterrows():
            handle_range_collected_data(row['data_source'], row['data_type'], row['biz_dt'], persist_flag=False)
        kafka_mq_consumer()
    except Exception as e:
        logger.error(f"互联网数据解析服务启动异常: {e} =》{str(traceback.format_exc())}")
