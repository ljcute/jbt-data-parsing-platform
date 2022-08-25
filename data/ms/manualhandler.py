#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/10 9:42
# @Site    : 
# @File    : manualhandler.py
# @Software: PyCharm
import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)
from data.ms.genralhandler import *
'''
手工修复数据，把正确的证券数据注册到注册中心，用于后续查询sec_id
'''


class ManualHandler(object):
    @classmethod
    def manual_handle_stock(cls):
        # df = pd.read_excel("D:\jbt-data-parsing-platform\data\ms\担保证券-交易所-股票-20220810.xlsx")
        df = pd.read_excel("担保证券-交易所-股票-20220810.xlsx")
        list = df.values.tolist()
        del list[0]
        args = []
        for i in list:
            sec_code = i[0]
            sec_name = i[1]
            arg_dict = {
                "sec_code_market": sec_code,
                "sec_type": "AS",
                "sec_name": sec_name,
                "update_flag": 1
            }
            args.append(arg_dict)
            logger.info(arg_dict)

        etl_param = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "sync_sec",
            "args": args
        }
        etl_result = cls.post_data_job(etl_param)

    @classmethod
    def manual_handle_bond(cls):
        # df = pd.read_excel("D:\jbt-data-parsing-platform\data\ms\担保证券-交易所-债券-20220810.xlsx")
        df = pd.read_excel("担保证券-交易所-债券-20220810.xlsx")
        list = df.values.tolist()
        del list[0]
        args = []
        for i in list:
            sec_code = i[0]
            sec_name = i[1]
            arg_dict = {
                "sec_code_market": sec_code,
                "sec_type": "B",
                "sec_name": sec_name,
                "update_flag": 1
            }
            args.append(arg_dict)
            logger.info(arg_dict)

        etl_param = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "sync_sec",
            "args": args
        }
        etl_result = cls.post_data_job(etl_param)

    @classmethod
    def manual_handle_fund(cls):
        # df = pd.read_excel("D:\jbt-data-parsing-platform\data\ms\担保证券-交易所-基金-20220810.xlsx")
        df = pd.read_excel("担保证券-交易所-基金-20220810.xlsx")
        list = df.values.tolist()
        del list[0]
        args = []
        for i in list:
            sec_code = i[0]
            sec_name = i[1]
            arg_dict = {
                "sec_code_market": sec_code,
                "sec_type": "OF",
                "sec_name": sec_name,
                "update_flag": 1
            }
            args.append(arg_dict)
            logger.info(arg_dict)

        etl_param = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "sync_sec",
            "args": args
        }
        etl_result = cls.post_data_job(etl_param)

    # 调用360etl接口
    @classmethod
    def post_data_job(cls, data):
        """
        POST JSON请求指定服务
        :param url:<str> 请求资源URI
        :param data:<obj> 请求对象
        :return :<dict> 响应一个字典对象
        """

        url = request_url + '/api/gateway'
        res = requests.post(url=url, json=data)
        if res.status_code != 200:
            logger.error(f'请求服务异常，uri={url}，json={data}，response={res.text}', exc_info=True)
            raise Exception(res.text)
        if res.text:
            try:
                return res.json()
            except Exception as ex:
                logger.error(f'请求服务异常，uri={url}，json={data}，response={res.text}，error={ex}', exc_info=True)
                raise ex
        else:
            return None


if __name__ == '__main__':
    mh = ManualHandler()
    logger.info(f'股票解析开始...')
    mh.manual_handle_stock()
    logger.info(f'股票解析完成...')
    time.sleep(5)

    logger.info(f'债券解析开始...')
    mh.manual_handle_bond()
    logger.info(f'债券解析完成...')
    time.sleep(5)

    logger.info(f'基金解析开始...')
    mh.manual_handle_fund()
    logger.info(f'基金解析完成...')

