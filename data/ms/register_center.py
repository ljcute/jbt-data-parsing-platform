#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@Date        : 2022-11-18
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""
import requests
import pandas as pd
from config import Config
from util.logs_utils import logger


def search_bo_info(sec_codes):
    """
    POST JSON请求指定服务
    :param sec_codes:<str> 证券代码
    :return :<dict> 响应字典对象，如只传证券代码，则可能会有多个返回结果，如都传，则可能匹配不到
    """
    _df = pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec_name'])
    if len(sec_codes) == 0:
        return _df
    parm = {"boCode": sec_codes}
    url = Config.get_cfg().get_content("register_center").get("api_url") + '/bo/search'
    res = requests.post(url=url, json=parm)
    if res.status_code != 200:
        logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}', exc_info=True)
        raise Exception(res.text)
    if res.text:
        try:
            dct = res.json()
            if len(dct) == 0:
                return _df
            _df = pd.DataFrame(res.json())[['boIdType', 'boId', 'boIdCode', 'boName']]
            _df = _df.loc[(_df['boIdType'].isin(['stock', 'bond', 'fund']) & _df['boIdCode'].str[-2:].isin(['SZ', 'SH', 'BJ']))].copy()
            _df.rename(columns={'boId': 'sec_id', 'boIdType': 'sec_type', 'boIdCode': 'sec_code', 'boName': 'sec_name'}, inplace=True)
            return _df
        except Exception as ex:
            logger.error(f'请求服务异常，uri={url}，json={parm}，response={res.text}，error={ex}', exc_info=True)
            raise ex
    else:
        return _df


if __name__ == '__main__':
    data = search_bo_info('160921')
    logger.info(f"{data}")