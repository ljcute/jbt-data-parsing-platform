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
        return pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code'])
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
            df.rename(columns={'sec_code_market': 'sec_code'}, inplace=True)
            return df[['sec_type', 'sec_id', 'sec_code']]
        except Exception as ex:
            logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}，error={ex}', exc_info=True)
            raise ex
    else:
        return pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code'])


def register_sec360_security(sec_codes):
    """
    POST JSON请求指定服务
    :param sec_codes> 证券代码
    :return :<dict> 响应一个字典对象
    """
    print(f"TODO fix register_sec360_security")
    print(111)
    return None
