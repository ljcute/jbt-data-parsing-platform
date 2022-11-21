#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import requests
import pandas as pd
from config import Config
from util.logs_utils import logger


def get_sec360_sec_id_code(sec_codes):
    """
    POST JSON请求指定服务
    :param sec_codes> 证券代码
    :return :<dict> 响应一个字典对象
    """
    if len(sec_codes) == 0:
        return pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec360_name'])
    parm = {
        "module": "pysec.etl.sec360.api.sec_api",
        "method": "query_sec",
        "args": sec_codes
    }
    url = Config.get_cfg().get_content("sec360").get("api_url") + '/api/gateway'
    res = requests.post(url=url, json=parm)
    if res.status_code != 200:
        logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}', exc_info=True)
        raise Exception(res.text)
    if res.text:
        try:
            df = pd.DataFrame(res.json()['data'])
            df['sec_type'] = df['sec_category'].apply(lambda x: str(x)[4:])
            df.rename(columns={'sec_code_market': 'sec_code', 'sec_name': 'sec360_name'}, inplace=True)
            return df[['sec_type', 'sec_id', 'sec_code', 'sec360_name']]
        except Exception as ex:
            logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}，error={ex}', exc_info=True)
            raise ex
    else:
        return pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec360_name'])


def name_format(name):
    name = str(name).replace(' ', '').replace('Ａ', 'A').replace('⑽', '(10)')
    name = name.replace('⑴', '(1)').replace('⑵', '(2)').replace('⑶', '(3)')
    name = name.replace('⑷', '(4)').replace('⑸', '(5)').replace('⑹', '(6)')
    name = name.replace('⑺', '(7)').replace('⑻', '(8)').replace('⑼', '(9)')
    return name


def register_sec360_security(df):
    """
    POST JSON请求指定服务
    :param df> df['sec_type', 'sec_code', 'sec_name']
    :return :
    """
    if df.empty:
        return
    _df = df.copy()
    _df['sec_name'] = _df['sec_name'].apply(lambda x: name_format(x))
    _df['sec_type'] = _df['sec_type'].apply(lambda x: 'AS' if x == 'stock' else 'B' if x == 'bond' else 'OF' if x == 'fund' else None)
    _df['update_flag'] = 1
    _df.rename(columns={'sec_code': 'sec_code_market'}, inplace=True)
    args_ = _df.to_dict('records')
    parm = {
        "module": "pysec.etl.sec360.api.sec_api",
        "method": "sync_sec",
        "args": args_
    }
    url = Config.get_cfg().get_content("sec360").get("api_url") + '/api/gateway'
    res = requests.post(url=url, json=parm)
    if res.status_code != 200:
        logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}', exc_info=True)
        raise Exception(res.text)
    if res.text:
        try:
            zc_result = res.json()
        except Exception as ex:
            logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}，error={ex}', exc_info=True)
            raise ex
    else:
        return None
    data_rs = zc_result['data']
    if data_rs:
        secu_id = data_rs[0]['sec_id']
        secu_type = get_secu_type(data_rs[0]['sec_category'])
        ax_.append(secu_id)
        ax_.append(secu_type)
        logger.info(f'该条已注册到360{ax_}')
    else:
        logger.error(f'匹配到现有规则的证券代码{ax_}注册到360失败！请检查')


