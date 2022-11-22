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


def get_ex_discount_limit_rate(biz_dt, sec_type, sec_ids):
    """
    POST JSON请求指定服务
    :param biz_dt> 业务日期
    :param sec_type> 证券类型
    :param sec_ids> 证券对象ID
    :return :<dict> 响应一个字典对象
    """
    if len(sec_ids) == 0:
        return pd.DataFrame(columns=['sec_id', 'rate'])
    query_data = {
        "module": "server.fdb.factor.api.factor_api",
        "method": "get_factor_data_cube",
        "kwargs": {
            "dt": f"{biz_dt}",
            "obj_type": sec_type,
            "object_ids": sec_ids,
            "factors": f"{sec_type[:1]}_ex_discount_rate"
        }
    }
    url = Config.get_cfg().get_content("fdb").get("api_url") + '/api/gateway'
    res = requests.post(url=url, json=query_data)
    if res.status_code != 200:
        logger.error(f'请求服务异常，uri={url}，json={query_data}，response={res.text}', exc_info=True)
        raise Exception(res.text)
    if res.text:
        try:
            dct = res.json()['data']
            df = pd.DataFrame(columns=dct['columns'], data=dct['data'])
            df.rename(columns={"object_id": "sec_id", f"{sec_type[:1]}_ex_discount_rate": "rate"}, inplace=True)
            df.sec_id = df.sec_id.astype('int64')
            return df
        except Exception as ex:
            logger.error(f'请求服务异常，uri={url}，json={query_data}，response={res.text}，error={ex}', exc_info=True)
            raise ex
    else:
        return pd.DataFrame(columns=['sec_id', 'rate'])

