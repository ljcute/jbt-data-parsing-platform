#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-9-19
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import math

import requests
import pandas as pd
from config import Config
from util.logs_utils import logger


def get_sec360_sec_id_code(sec_codes):
    _df = pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec360_name'])
    if len(sec_codes) == 0:
        return _df
    pages = math.ceil(len(sec_codes)/5000)
    # 适配证券360分页取数
    for i in range(pages):
        _df = pd.concat([_df, _get_sec360_sec_id_code(sec_codes[i*5000: (i+1)*5000])])
    return _df


def _get_sec360_sec_id_code(sec_codes):
    """
    POST JSON请求指定服务
    :param sec_codes> 证券代码
    :return :<dict> 响应一个字典对象
    """
    _df = pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec360_name'])
    if len(sec_codes) == 0:
        return _df
    parm = {
        "fields": ["secCategory", "secId", "secCodeMarket", "secName"],
        "secCodeMarkets": sec_codes
    }
    url = Config.get_cfg().get_content("sec360").get("api_url") + '/sec360/ms/sec/info/query_by_code'
    res = requests.post(url=url, json=parm)
    if res.status_code != 200:
        logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}', exc_info=True)
        raise Exception(res.text)
    if res.text:
        try:
            df = pd.DataFrame(columns=res.json()['columns'], data=res.json()['data'])
            if df.empty:
                return _df
            df['sec_type'] = df['secCategory'].apply(lambda x: str(x)[4:])
            df.rename(columns={'secType': 'sec_type', 'secId': 'sec_id', 'secCodeMarket': 'sec_code', 'secName': 'sec360_name'}, inplace=True)
            return df[['sec_type', 'sec_id', 'sec_code', 'sec360_name']]
        except Exception as ex:
            logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}，error={ex}', exc_info=True)
            raise ex
    else:
        return _df


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
    # if data_rs:
    #     secu_id = data_rs[0]['sec_id']
    #     secu_type = get_secu_type(data_rs[0]['sec_category'])
    #     ax_.append(secu_id)
    #     ax_.append(secu_type)
    #     logger.info(f'该条已注册到360{ax_}')
    # else:
    #     logger.error(f'匹配到现有规则的证券代码{ax_}注册到360失败！请检查')


if __name__ == '__main__':
    get_sec360_sec_id_code(['561330.SH', '561330.SZ'])
