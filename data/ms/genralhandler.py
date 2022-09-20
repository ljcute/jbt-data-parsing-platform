#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author  : yanpan
# @Time    : 2022/8/18 11:51
# @Site    : 
# @File    : genralhandler.py
# @Software: PyCharm
import re

import requests
from utils.logs_utils import logger
from constants import *
from decimal import Decimal
from data.dao.biz.biz_data_deal import *

base_dir = os.path.dirname(os.path.abspath(__file__))
full_path = os.path.join(base_dir, '../../config/config.ini')
cf = ConfigParser()
cf.read(full_path, encoding='utf-8')
request_url = cf.get('360etl-url', 'url')
request_url_zc_center = cf.get('zc-center-url', 'url')


# 比较调整前，调整后的rate值
def get_adjust_status_by_two_rate(pre_value, current_rate):
    if current_rate is None or current_rate == '-' or current_rate == '--':  # 当前rate是'-'表示调出
        return adjust_status_out

    if pre_value is None and current_rate is None:
        return adjust_status_invariant

    if pre_value is None and current_rate is not None:
        return adjust_status_in

    if pre_value is not None and current_rate is not None:
        pre_value2 = Decimal(pre_value).quantize(Decimal('0.00'))
        current_value2 = Decimal(current_rate).quantize(Decimal('0.00'))
        result = pre_value2.compare(current_value2)
        if result == 1:
            return adjust_status_low
        elif result == -1:
            return adjust_status_high
        return adjust_status_invariant  # 不变


# 判断df中是否存在某个字段
def df_exists_index(df, sec_code, sec_id):
    try:
        df_record = df[(df.secu_id == sec_id) & (sec_code is not None)]
        if not df_record.empty:
            for i, row in df_record.iterrows():
                return row
    except Exception as es:
        logger.error(es)
    return None


# 判断券商状态设置
def rate_is_normal_one(rate):
    if '作废' in str(rate) or '禁用' in str(rate) or '限制' in str(rate):
        right_rate = None
    else:
        right_rate = round(float(str(rate)) * 100, 3)

    return right_rate


def rate_is_normal_two(rate):
    if '作废' in str(rate) or '禁用' in str(rate) or '限制' in str(rate):
        right_rate = None
    else:
        right_rate = round(float(str(rate).strip('%')), 3)

    return right_rate


# 证券代码规则匹配
def sec_code_rules_match(code):
    # 处理是否带后缀
    code = code.upper()
    if '.' in code:
        rs_list = code.split('.')
        if len(rs_list[0]) == 6:
            rs = match_rules(rs_list[0])
            if rs:
                rs_code = rs['code']
                if code == rs_code:
                    return rs
                else:
                    logger.error(f'该带后缀的证券代码{code}匹配现有规则失败，后缀错误，请检查！')
            else:
                return None
    else:
        if len(str(code)) == 6:
            return match_rules(code)


def match_rules(code):
    sz_bond_4 = ['1003', '1004', '1006', '1008', '1009', '1019', '1111', '1119', '1181', '1182', '1183', '1184', '1185',
                 '1186', '1187', '1188', '1189', '1215', '1263', '1266', '1267', '1281', '1282', '1290', '1330', '1331',
                 '1333', '1351', '1372', '1373', '1374', '1378', '1379', '1395', '1396', '1397', '1398', '1399', '1930',
                 '10020', '10021', '10050', '10051', '10070', '10071', '10998', '10999', '11101', '11102', '11103',
                 '11104', '11105', '11106', '11107', '11108', '11109', '11802', '11803', '11804', '11805', '11806',
                 '11807', '11808', '11809', '12311', '12312', '12313', '12314', '12315', '12801', '12802', '12803',
                 '12804', '12805', '12806', '12807', '12808', '12809', '13320', '13321', '13322', '13323', '13324',
                 '13326', '13327', '13328', '13329', '13716', '13717', '13718', '13719', '13949', '14995']

    sh_bond_4 = ['1000', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1232', '1233', '1234', '1235', '1236',
                 '1237', '1238', '1239', '1241', '1242', '1243', '1244', '1245', '1246', '1247', '1248', '1249', '1251',
                 '1252', '1271', '1272', '1273', '1274', '1275', '1276', '1277', '1278', '1279', '1296', '1297', '1298',
                 '1299', '1355', '1356', '1357', '1358', '1500', '1501', '1502', '1503', '1504', '1505', '1506', '1507',
                 '1508', '1509', '1591', '1592', '1593', '1594', '1595', '1600', '1628', '1629', '1636', '1637', '1650',
                 '1651', '1652', '1654', '1656', '1657', '1658', '1659', '1662', '1663', '1665', '1666', '1667', '1668',
                 '1669', '1671', '1672', '1674', '1676', '1677', '1678', '1679', '1685', '1686', '1688', '1689', '1690',
                 '1693', '1694', '1695', '1696', '1697', '1698', '1699', '1800', '1806', '1807', '1840', '1841', '1842',
                 '1843', '1844', '1845', '1947', '1948', '1949', '10017', '10019', '10022', '10023', '10056', '10079',
                 '10105', '10106', '10107', '10108', '10109', '10113', '10114', '10115', '10116', '10117', '10118',
                 '10119', '10123', '10124', '10125', '10126', '10127', '10128', '10129', '10133', '10134', '10135',
                 '10136', '10137', '10138', '10139', '10144', '10145', '10146', '10147', '10148', '10149', '10153',
                 '10154', '10155', '10156', '10157', '10158', '10159', '10163', '10164', '10165', '10166', '10167',
                 '10168', '10169', '10173', '10174', '10175', '10176', '10177', '10178', '10183', '10184', '10185',
                 '10186', '10187', '10188', '10189', '11100', '12101', '12102', '12403', '12404', '12405', '12406',
                 '12407', '12408', '12409', '12501', '12503', '12504', '12505', '12507', '12509', '12531', '12532',
                 '12533', '12534', '12535', '12536', '12537', '12538', '12539', '12540', '12541', '12542', '12543',
                 '12544', '12545', '12546', '12547', '12549', '12550', '12551', '12553', '12554', '12555', '12556',
                 '12560', '12561', '12564', '12565', '12566', '12567', '12568', '12569', '12574', '12575', '12576',
                 '12577', '12578', '12579', '12580', '12581', '12583', '12584', '12585', '12586', '12587', '12590',
                 '12591', '12592', '12597', '12598', '12599', '12601', '12708', '12709', '13541', '13544', '13545',
                 '13546', '13547', '13548', '13549', '15901', '15902', '15903', '15904', '15905', '15906', '15907',
                 '15908', '15909', '15965', '15966', '15967', '15968', '15969', '16015', '16016', '16017', '16018',
                 '16019', '16020', '16023', '16024', '16025', '16026', '16027', '16028', '16029', '16030', '16033',
                 '16034', '16035', '16036', '16037', '16038', '16039', '16040', '16043', '16044', '16045', '16046',
                 '16047', '16048', '16049', '16053', '16054', '16055', '16056', '16057', '16058', '16059', '16065',
                 '16066', '16067', '16068', '16069', '16073', '16074', '16075', '16076', '16077', '16078', '16079',
                 '16082', '16083', '16084', '16085', '16086', '16087', '16088', '16089', '16090', '16093', '16094',
                 '16095', '16096', '16097', '16098', '16099', '16201', '16202', '16203', '16204', '16205', '16206',
                 '16207', '16208', '16209', '16211', '16212', '16213', '16214', '16215', '16216', '16217', '16218',
                 '16219', '16222', '16223', '16224', '16225', '16226', '16227', '16228', '16229', '16231', '16232',
                 '16233', '16234', '16235', '16236', '16237', '16238', '16239', '16240', '16242', '16243', '16244',
                 '16245', '16246', '16247', '16248', '16249', '16252', '16253', '16254', '16255', '16256', '16257',
                 '16258', '16259', '16261', '16262', '16263', '16264', '16265', '16266', '16267', '16268', '16269',
                 '16273', '16274', '16275', '16276', '16277', '16278', '16279', '16301', '16302', '16303', '16304',
                 '16305', '16306', '16307', '16308', '16309', '16312', '16313', '16314', '16315', '16316', '16317',
                 '16318', '16319', '16322', '16323', '16324', '16325', '16326', '16327', '16328', '16329', '16331',
                 '16332', '16333', '16334', '16335', '16336', '16337', '16338', '16339', '16342', '16343', '16344',
                 '16345', '16346', '16347', '16348', '16349', '16351', '16352', '16353', '16354', '16355', '16356',
                 '16357', '16358', '16359', '16383', '16384', '16385', '16386', '16387', '16388', '16389', '16391',
                 '16392', '16393', '16394', '16395', '16396', '16397', '16398', '16399', '16532', '16533', '16534',
                 '16535', '16536', '16537', '16538', '16539', '16553', '16554', '16555', '16556', '16557', '16558',
                 '16559', '16603', '16604', '16605', '16606', '16607', '16608', '16609', '16611', '16612', '16613',
                 '16614', '16615', '16616', '16617', '16618', '16619', '16641', '16642', '16643', '16644', '16645',
                 '16646', '16647', '16648', '16649', '16701', '16702', '16703', '16704', '16705', '16706', '16707',
                 '16708', '16709', '16731', '16732', '16733', '16734', '16735', '16736', '16737', '16738', '16739',
                 '16751', '16752', '16753', '16754', '16755', '16756', '16757', '16758', '16759', '16801', '16802',
                 '16803', '16804', '16805', '16806', '16807', '16808', '16809', '16811', '16812', '16813', '16814',
                 '16815', '16816', '16817', '16818', '16819', '16821', '16822', '16823', '16824', '16825', '16826',
                 '16827', '16828', '16829', '16831', '16832', '16833', '16834', '16835', '16836', '16837', '16838',
                 '16839', '16841', '16842', '16843', '16844', '16845', '16846', '16847', '16848', '16849', '16871',
                 '16872', '16873', '16874', '16875', '16876', '16877', '16878', '16879', '16911', '16912', '16913',
                 '16914', '16915', '16916', '16917', '16918', '16919', '16921', '16922', '16923', '16924', '16925',
                 '16926', '16927', '16928', '16929', '18011', '18012', '18013', '18014', '18015', '18016', '18017',
                 '18018', '18019', '18021', '18023', '18024', '18025', '18026', '18027', '18028', '18029', '18031',
                 '18032', '18033', '18034', '18035', '18036', '18037', '18038', '18039', '18041', '18042', '18043',
                 '18044', '18045', '18046', '18047', '18048', '18049', '18051', '18052', '18053', '18054', '18055',
                 '18056', '18057', '18058', '18059', '18081', '18083', '18084', '18085', '19464', '19465', '19466',
                 '19467', '19468', '19469']

    if str(code).startswith('2', 0, 1) or str(code).startswith('3', 0, 1) or str(code).startswith('00', 0, 2):
        code = code + '.SZ'
        type = 'stock'
        return {'code': code, 'type': type}
    elif str(code).startswith('102', 0, 3) or str(code).startswith('104', 0, 3) or str(code).startswith('105', 0,
                                                                                                        3) or str(
        code).startswith('106', 0, 3) \
            or str(code).startswith('107', 0, 3) or str(code).startswith('108', 0, 3) or str(code).startswith('112', 0,
                                                                                                              3) or str(
        code).startswith('114', 0, 3) \
            or str(code).startswith('115', 0, 3) or str(code).startswith('116', 0, 3) or str(code).startswith('117', 0,
                                                                                                              3) or str(
        code).startswith('119', 0, 3) \
            or str(code).startswith('138', 0, 3) or str(code).startswith('148', 0, 3) or str(code).startswith('190', 0,
                                                                                                              3) or str(
        code).startswith('191', 0, 3) \
            or str(code).startswith('192', 0, 3) or str(code).startswith('198', 0, 3) or str(code).startswith(
        tuple(sz_bond_4)):
        code = code + '.SZ'
        type = 'bond'
        return {'code': code, 'type': type}
    elif str(code).startswith('161', 0, 3) or str(code).startswith('164', 0, 3) or str(code).startswith('1848', 0, 4):
        code = code + '.SZ'
        type = 'fund'
        return {'code': code, 'type': type}
    elif str(code).startswith('6', 0, 1) or str(code).startswith('90', 0, 2):
        code = code + '.SH'
        type = 'stock'
        return {'code': code, 'type': type}
    elif str(code).startswith('01', 0, 2) or str(code).startswith('02', 0, 2) or str(code).startswith('17', 0,
                                                                                                      2) or str(
        code).startswith('95', 0, 2) or str(code).startswith('110', 0, 3) or str(code).startswith('113', 0,
                                                                                                  3) or str(
        code).startswith('122', 0, 3) or str(code).startswith('130', 0, 3) \
            or str(code).startswith('131', 0, 3) or str(code).startswith('132', 0, 3) or str(code).startswith('140', 0,
                                                                                                              3) or str(
        code).startswith('142', 0, 3) \
            or str(code).startswith('143', 0, 3) or str(code).startswith('145', 0, 3) or str(code).startswith('146', 0,
                                                                                                              3) or str(
        code).startswith('147', 0, 3) \
            or str(code).startswith('151', 0, 3) or str(code).startswith('152', 0, 3) or str(code).startswith('155', 0,
                                                                                                              3) or str(
        code).startswith('156', 0, 3) \
            or str(code).startswith('157', 0, 3) or str(code).startswith('182', 0, 3) or str(code).startswith('183', 0,
                                                                                                              3) or str(
        code).startswith('185', 0, 3) \
            or str(code).startswith('186', 0, 3) or str(code).startswith('188', 0, 3) or str(code).startswith('189', 0,
                                                                                                              3) or str(
        code).startswith('196', 0, 3) \
            or str(code).startswith('197', 0, 3) or str(code).startswith(tuple(sh_bond_4)):
        code = code + '.SH'
        type = 'bond'
        return {'code': code, 'type': type}
    elif str(code).startswith('5', 0, 1):
        code = code + '.SH'
        type = 'fund'
        return {'code': code, 'type': type}
    elif str(code).startswith('4', 0, 1) or str(code).startswith('83', 0, 2) or str(code).startswith('87', 0, 1):
        code = code + '.BJ'
        type = 'stock'
        return {'code': code, 'type': type}
    elif str(code).startswith('80', 0, 2) or str(code).startswith('8122', 0, 4):
        code = code + '.BJ'
        type = 'bond'
        return {'code': code, 'type': type}
    else:
        return None


# 调用360etl接口
def post_data_job(data):
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


# 调用注册中心接口，通过证券代码，证券简称查询类型，code
def get_securities_type_job(data):
    """
        POST JSON请求指定服务
        :param data: boCode,boName
        :param boCode:<str> 证券代码
        :param boName:<str> 证券简称
        :return :<dict> 响应字典对象，如只传证券代码，则可能会有多个返回结果，如都传，则可能匹配不到
        """

    url = request_url_zc_center + '/bo/search'
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


# 融资融券组合数据获取证券id公共方法-- 融资融券合并,不带市场market_flag为False，无后缀
def securities_normal_parsing_data(data):
    return temp_deal(data, False)


# 融资融券组合数据获取证券id公共方法-- 融资融券分开,不带市场market_flag为False，无后缀
def securities_normal_parsing_data_no_market(data):
    return temp_deal(data, False)


# 可充抵保证金rate解析公共方法--带市场的处理，market_flag为True，无后缀
def securities_bzj_parsing_data(rs, biz_type, data_):
    real_data = temp_deal(data_, True)

    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), biz_type, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in real_data:
            if len(i) == 6:
                if i[3] is not None:
                    rate = int(i[3])
                    if i[5] == 'stock':
                        if 0 <= rate <= 70:
                            insert_data_list.append(
                                [broker_id, i[4], i[5], biz_type, adjust_status_in, None, rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt,
                                 None])
                        else:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                    elif i[5] == 'fund':
                        if 0 <= rate <= 95:
                            insert_data_list.append(
                                [broker_id, i[4], i[5], biz_type, adjust_status_in, None, rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt,
                                 None])
                        else:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                    elif i[5] == 'bond':
                        if 0 <= rate <= 95:
                            insert_data_list.append(
                                [broker_id, i[4], i[5], biz_type, adjust_status_in, None, rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt,
                                 None])
                        else:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                else:
                    insert_data_list.append([broker_id, i[4], i[5], biz_type, adjust_status_out, None, None, 1, 1,
                         datetime.datetime.now(), forever_end_dt,None])
            else:
                invalid_data_list.append(i)
        if invalid_data_list:
            logger.error(f'{rs[3]}，如下数据无证券id，请检查!{invalid_data_list}')
        if insert_data_list:
            logger.info(f'业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list)
            logger.info(f'业务数据入库结束,共{len(insert_data_list)}条')
    else:
        logger.info(f'进入不为空的判断')
        # 先把本次查询到的数据中有的 但是数据库中没有的更新为失效
        haved_list = []
        query_list = []
        for col in result.values.tolist():
            haved_list.append(col[2])

        for row_ in real_data:
            if len(row_) == 6:
                query_list.append(row_[4])

        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            insert_data_list_new = []
            for b in s_list:
                for temp_data in real_data:
                    if b == temp_data[4] and temp_data[3] is not None:
                        secu_type = temp_data[5]
                        if secu_type == 'stock':
                            if 0 <= int(temp_data[3]) <= 70:
                                insert_data_list_new.append(
                                    [broker_id, b, secu_type, biz_type, adjust_status_in, None,
                                     temp_data[3], 1, 1, datetime.datetime.now(), forever_end_dt, None])
                            else:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                        elif secu_type == 'fund':
                            if 0 <= int(temp_data[3]) <= 95:
                                insert_data_list_new.append(
                                    [broker_id, b, secu_type, biz_type, adjust_status_in, None,
                                     temp_data[3], 1, 1, datetime.datetime.now(), forever_end_dt, None])
                            else:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                        elif secu_type == 'bond':
                            if 0 <= int(temp_data[3]) <= 95:
                                insert_data_list_new.append(
                                    [broker_id, b, secu_type, biz_type, adjust_status_in, None,
                                     temp_data[3], 1, 1, datetime.datetime.now(), forever_end_dt, None])
                            else:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                    elif b == temp_data[4] and temp_data[3] is None:
                        insert_data_list_new.append([broker_id, b, temp_data[5], biz_type, adjust_status_out, None, None, 1, 1,
                                                 datetime.datetime.now(), forever_end_dt, None])

            if insert_data_list_new:
                logger.info(f'业务数据入库开始,t_broker_mt_business_security...')
                insert_broker_mt_business_security(insert_data_list_new)
                logger.info(f'业务数据入库完成，共{len(insert_data_list_new)}条')

        b_list = list(set(haved_list).difference(set(query_list)))
        if b_list:
            for s in b_list:
                update_business_security((str(rs[1])).replace('-', ''), s, broker_id, biz_type)

        insert_data_list_noempty = []
        for row in real_data:
            if len(row) == 6:
                if row[3] is not None:
                    sec_code = row[1]
                    sec_id = row[4]
                    secu_type = row[5]
                    round_rate = row[3]

                    db_record = df_exists_index(result, sec_code, sec_id)
                    if db_record is not None:
                        old_rate = db_record[7]
                        adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                        if adjust_status != adjust_status_invariant:
                            if adjust_status == adjust_status_in:
                                # 调入 新增记录,更新之前一条位失效（仅限同一天内）
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                if secu_type == 'stock':
                                    if 0 <= int(round_rate) <= 70:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_in, old_rate,
                                             round_rate,
                                             1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'fund':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_in, old_rate,
                                             round_rate,
                                             1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'bond':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_in, old_rate,
                                             round_rate,
                                             1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                            elif adjust_status == adjust_status_high:
                                # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                if secu_type == 'stock':
                                    if 0 <= int(round_rate) <= 70:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate,
                                             round_rate, 1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'fund':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate,
                                             round_rate, 1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'bond':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate,
                                             round_rate, 1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                            elif adjust_status == adjust_status_low:
                                # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                if secu_type == 'stock':
                                    if 0 <= int(round_rate) <= 70:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate,
                                             round_rate, 1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'fund':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate,
                                             round_rate, 1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'bond':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate,
                                             round_rate, 1, 1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                            elif adjust_status == adjust_status_out:
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                # 调出 更新记录，rate置为空，新增一条调处记录，更新其他字段,
                                insert_data_list_noempty.append(
                                    [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, None, 1, 1,
                                     datetime.datetime.now(), forever_end_dt, None])
                else:
                    insert_data_list_noempty.append([broker_id, row[4], row[5], biz_type, adjust_status_out, None, None, 1, 1,
                         datetime.datetime.now(), forever_end_dt,None])
            else:
                invalid_data_list.append(row)
        if invalid_data_list:
            logger.error(f'{rs[3]}，如下数据无证券id，请检查!{invalid_data_list}')
        if insert_data_list_noempty:
            logger.info(f'业务数据入库开始,t_broker_mt_business_security...')
            insert_broker_mt_business_security(insert_data_list_noempty)
            logger.info(f'业务数据入库完成，共{len(insert_data_list_noempty)}条')


# 可充抵保证金rate解析公共方法--不带市场的处理，无后缀，不带市场,market_flag为False
def securities_bzj_parsing_data_no_market(rs, data_):
    real_data = temp_deal(data_, False)

    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), 3, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in real_data:
            if len(i) == 5:
                if i[2] is not None:
                    rate = int(i[2])
                    if i[4] == 'stock':
                        if 0 <= rate <= 70:
                            insert_data_list.append(
                                [broker_id, i[3], i[4], 3, adjust_status_in, None, i[2], 1, 1, datetime.datetime.now(),
                                 forever_end_dt, None])
                        else:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                    elif i[4] == 'fund':
                        if 0 <= rate <= 95:
                            insert_data_list.append(
                                [broker_id, i[3], i[4], 3, adjust_status_in, None, i[2], 1, 1, datetime.datetime.now(),
                                 forever_end_dt, None])
                        else:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                    elif i[4] == 'bond':
                        if 0 <= rate <= 95:
                            insert_data_list.append(
                                [broker_id, i[3], i[4], 3, adjust_status_in, None, i[2], 1, 1, datetime.datetime.now(),
                                 forever_end_dt, None])
                        else:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                else:
                    insert_data_list.append([broker_id, i[3], i[4], 3, adjust_status_out, None, None, 1, 1, datetime.datetime.now(),
                         forever_end_dt, None])
            else:
                invalid_data_list.append(i)
        if invalid_data_list:
            logger.error(f'{rs[3]}，如下数据无证券id，请检查!{invalid_data_list}')
        if insert_data_list:
            logger.info(f'业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list)
            logger.info(f'业务数据入库结束，共{len(insert_data_list)}条')
    else:
        logger.info(f'进入不为空的判断')
        # 先把本次查询到的数据中有的 但是数据库中没有的更新为失效
        haved_list = []
        query_list = []
        for col in result.values.tolist():
            haved_list.append(col[2])

        for row_ in real_data:
            if len(row_) == 5:
                query_list.append(row_[3])

        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            insert_data_list_new = []
            for b in s_list:
                for temp_data in real_data:
                    if b == temp_data[3] and temp_data[2] is not None:
                        secu_type = temp_data[4]
                        if secu_type == 'stock':
                            if 0 <= int(temp_data[2]) <= 70:
                                insert_data_list_new.append(
                                    [broker_id, b, secu_type, 3, adjust_status_in, None,
                                     temp_data[2], 1, 1, datetime.datetime.now(), forever_end_dt, None])
                            else:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                        elif secu_type == 'fund':
                            if 0 <= int(temp_data[2]) <= 95:
                                insert_data_list_new.append(
                                    [broker_id, b, secu_type, 3, adjust_status_in, None,
                                     temp_data[2], 1, 1, datetime.datetime.now(), forever_end_dt, None])
                            else:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                        elif secu_type == 'bond':
                            if 0 <= int(temp_data[2]) <= 95:
                                insert_data_list_new.append(
                                    [broker_id, b, secu_type, 3, adjust_status_in, None,
                                     temp_data[2], 1, 1, datetime.datetime.now(), forever_end_dt, None])
                            else:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                    if b == temp_data[3] and temp_data[2] is None:
                        insert_data_list_new.append([broker_id, b, temp_data[4], 3, adjust_status_out, None,
                             None, 1, 1, datetime.datetime.now(), forever_end_dt, None])
            if insert_data_list_new:
                logger.info(f'业务数据入库开始,t_broker_mt_business_security...')
                insert_broker_mt_business_security(insert_data_list_new)
                logger.info(f'业务数据入库完成，共{len(insert_data_list_new)}条')

        b_list = list(set(haved_list).difference(set(query_list)))
        if b_list:
            for s in b_list:
                update_business_security((str(rs[1])).replace('-', ''), s, broker_id, 3)

        insert_data_list_noempty = []
        for row in real_data:
            if len(row) == 5:
                if row[2] is not None:
                    sec_code = row[0]
                    sec_id = row[3]
                    secu_type = row[4]
                    round_rate = row[2]

                    db_record = df_exists_index(result, sec_code, sec_id)
                    if db_record is not None:
                        old_rate = db_record[7]
                        adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                        if adjust_status != adjust_status_invariant:
                            if adjust_status == adjust_status_in:
                                # 调入 新增记录,更新之前一条位失效（仅限同一天内）
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, 3)
                                if secu_type == 'stock':
                                    if 0 <= int(round_rate) <= 70:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_in, old_rate, round_rate, 1,
                                             1,
                                             datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'fund':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_in, old_rate, round_rate, 1,
                                             1,
                                             datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'bond':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_in, old_rate, round_rate, 1,
                                             1,
                                             datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                            elif adjust_status == adjust_status_high:
                                # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, 3)
                                if secu_type == 'stock':
                                    if 0 <= int(round_rate) <= 70:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_high, old_rate, round_rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'fund':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_high, old_rate, round_rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'bond':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_high, old_rate, round_rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                            elif adjust_status == adjust_status_low:
                                # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, 3)
                                if secu_type == 'stock':
                                    if 0 <= int(round_rate) <= 70:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_low, old_rate, round_rate,
                                             1, 1,
                                             datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'fund':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_low, old_rate, round_rate,
                                             1, 1,
                                             datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                elif secu_type == 'bond':
                                    if 0 <= int(round_rate) <= 95:
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, 3, adjust_status_low, old_rate, round_rate,
                                             1, 1,
                                             datetime.datetime.now(), forever_end_dt, None])
                                    else:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                            elif adjust_status == adjust_status_out:
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, 3)
                                # 调出 更新记录，rate置为空，新增一条调处记录，更新其他字段,
                                insert_data_list_noempty.append(
                                    [broker_id, sec_id, secu_type, 3, adjust_status_out, old_rate, None, 1, 1,
                                     datetime.datetime.now(), forever_end_dt, None])
                else:
                    insert_data_list_noempty.append([broker_id, row[3], row[4], 3, adjust_status_out, None, None, 1, 1,
                         datetime.datetime.now(), forever_end_dt, None])
            else:
                invalid_data_list.append(row)
        if invalid_data_list:
            logger.error(f'{rs[3]}，如下数据无证券id，请检查!{invalid_data_list}')
        if insert_data_list_noempty:
            logger.info(f'业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list_noempty)
            logger.info(f'业务数据入库完成，共{len(insert_data_list_noempty)}条')


def get_secu_type(etl_type):
    if etl_type == 'sec_fund':
        secu_type = 'fund'
    elif etl_type == 'sec_stock':
        secu_type = 'stock'
    elif etl_type == 'sec_bond':
        secu_type = 'bond'
    else:
        secu_type = 'other'
    return secu_type


# 统一根据代码匹配规则和360，注册中心服务获取证券id和证券类型的接口
def temp_deal(data, market_flag):
    if market_flag:
        ax_list = []
        bx_list = []
        for a in data:
            code = str(a[1]).replace(' ', '')
            rs = sec_code_rules_match(code)
            if rs:
                ax_list.append(a)
            else:
                bx_list.append(a)

        args_list = []
        for ax in ax_list:
            with_suffix_code = sec_code_rules_match(str(ax[1]).replace(' ', ''))['code']
            ax[1] = with_suffix_code
            args_list.append(with_suffix_code)

        query_datas = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "query_sec",
            "args": args_list
        }
        query_result = post_data_job(query_datas)
        res_data = query_result['data']
        for ax in ax_list:
            for res in res_data:
                if ax[1] == res['sec_code_market']:
                    if sec_code_rules_match(str(ax[1]).replace(' ', ''))['type'] in res['sec_category']:
                        ax.append(res['sec_id'])
                        ax.append(get_secu_type(res['sec_category']))

        for ax_ in ax_list:
            if len(ax_) == 4:
                args_code = sec_code_rules_match(str(ax_[1]).replace(' ', ''))['code']
                type_code = sec_code_rules_match(str(ax_[1]).replace(' ', ''))['type']
                # 注册到360
                sec_type = None
                if type_code == 'stock':
                    sec_type = 'AS'
                elif type_code == 'bond':
                    sec_type = 'B'
                elif type_code == 'fund':
                    sec_type = 'OF'

                args_dict = {
                    "sec_code_market": args_code,
                    "sec_type": sec_type,
                    "sec_name": str(ax_[2]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷',
                                                                                                           '(4)').replace(
                        '⑼', '(9)'),
                    "update_flag": 1
                }
                args_ = [args_dict]
                sync_datas = {
                    "module": "pysec.etl.sec360.api.sec_api",
                    "method": "sync_sec",
                    "args": args_
                }
                sync_result = post_data_job(sync_datas)
                data_rs = sync_result['data']
                if data_rs:
                    secu_id = data_rs[0]['sec_id']
                    secu_type = get_secu_type(data_rs[0]['sec_category'])
                    ax_.append(secu_id)
                    ax_.append(secu_type)
                    logger.info(f'该条已注册到360{ax_}')
                else:
                    logger.error(f'匹配到现有规则的证券代码{ax_}注册到360失败！请检查')

        for bx in bx_list:
            boName = str(bx[2]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷', '(4)').replace(
                '⑼', '(9)')
            boCode = str(bx[1]).replace(' ', '').split('.')[0] if '.' in str(bx[1]).replace(' ', '') else str(
                bx[1]).replace(' ', '')
            data_dict = {"boCode": boCode}
            res = get_securities_type_job(data_dict)
            if res:
                if 'ETF' in boName or 'LOF' in boName or '基金' in boName or boName.endswith('基'):
                    temp_type = 'fund'
                    if len(res) == 1:
                        if res[0]['boIdType'] == temp_type:
                            # 提前知晓类型，就直接去判断类型，因为boid唯一
                            secu_id = res[0]['boId']
                            secu_type = res[0]['boIdType']
                            bx.append(secu_id)
                            bx.append(secu_type)
                    elif len(res) > 1:
                        for r in res:
                            if r['boIdType'] == temp_type:
                                secu_id = r['boId']
                                secu_type = r['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                                break
                            else:
                                continue

                    # 说明没有查到证券id
                    if len(bx) == 4:
                        logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                elif '债券' in boName or boName.endswith('债') or boName.endswith('转'):
                    temp_type = 'bond'
                    # 先匹配名称 若还是查不到证券id和类型再匹配类型
                    if len(res) == 1:
                        if res[0]['boName'] == boName:
                            secu_id = res[0]['boId']
                            secu_type = res[0]['boIdType']
                            bx.append(secu_id)
                            bx.append(secu_type)
                    elif len(res) > 1:
                        for r in res:
                            if r['boName'] == boName:
                                secu_id = r['boId']
                                secu_type = r['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                                break
                            else:
                                continue

                    if len(bx) == 4:
                        # 匹配名字仍查不到证券id和类型，则匹配类型
                        if len(res) == 1:
                            if res[0]['boIdType'] == temp_type:
                                # 提前知晓类型，就直接去判断类型，因为boid唯一
                                secu_id = res[0]['boId']
                                secu_type = res[0]['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                        elif len(res) > 1:
                            for r in res:
                                if r['boIdType'] == temp_type:
                                    secu_id = r['boId']
                                    secu_type = r['boIdType']
                                    bx.append(secu_id)
                                    bx.append(secu_type)
                                    break
                                else:
                                    continue

                    # 说明没有查到证券id
                    if len(bx) == 4:
                        logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                else:
                    if len(res) == 1:
                        if res[0]['boName'] == boName and boCode in res[0]['boIdCode']:
                            secu_id = res[0]['boId']
                            secu_type = res[0]['boIdType']
                            bx.append(secu_id)
                            bx.append(secu_type)
                        else:
                            logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                    elif len(res) > 1:
                        for r in res:
                            if r['boName'] == boName and boCode in r['boIdCode']:
                                secu_id = r['boId']
                                secu_type = r['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                                break
                            else:
                                continue

                    if len(bx) == 4 or len(bx) == 3:
                        temp_market = str(bx[0]).replace(' ', '')
                        args_code = None
                        if temp_market == '深圳' or temp_market == '深市' or temp_market == '2' or temp_market == '深交所' or temp_market == '深A' or temp_market == '深圳证券交易所':
                            args_code = str(bx[1]).replace(' ', '') + '.SZ'
                        elif temp_market == '上海' or temp_market == '沪市' or temp_market == '1' or temp_market == '上交所' or temp_market == '沪A' or temp_market == '上海证券交易所':
                            args_code = str(bx[1]).replace(' ', '') + '.SH'

                        args_list = [args_code]
                        query_datas = {
                            "module": "pysec.etl.sec360.api.sec_api",
                            "method": "query_sec",
                            "args": args_list
                        }
                        query_result = post_data_job(query_datas)
                        res_data = query_result['data']
                        if res_data:
                            secu_id = res_data[0]['sec_id']
                            secu_type = get_secu_type(res_data[0]['sec_category'])
                            bx.append(secu_id)
                            bx.append(secu_type)
                        else:
                            logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                        # if len(res) == 1:
                        #     if boCode in res[0]['boIdCode']:
                        #         secu_id = res[0]['boId']
                        #         secu_type = res[0]['boIdType']
                        #         bx.append(secu_id)
                        #         bx.append(secu_type)
                        #     else:
                        #         logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                        # elif len(res) > 1:
                        #     for r in res:
                        #         if boCode == r['boIdCode']:
                        #             secu_id = r['boId']
                        #             secu_type = r['boIdType']
                        #             bx.append(secu_id)
                        #             bx.append(secu_type)
                        #             break
                        #         else:
                        #             continue

                    # 说明没有查到证券id
                    if len(bx) == 4:
                        logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
            else:
                # 若注册中心查不到结果 但是带市场后缀 则拼接后缀去360查
                temp_market = str(bx[0]).replace(' ', '')
                args_code = None
                if temp_market == '深圳' or temp_market == '深市' or temp_market == '2' or temp_market == '深交所' or temp_market == '深A' or temp_market == '深圳证券交易所':
                    args_code = str(bx[1]).replace(' ', '') + '.SZ'
                elif temp_market == '上海' or temp_market == '沪市' or temp_market == '1' or temp_market == '上交所' or temp_market == '沪A' or temp_market == '上海证券交易所':
                    args_code = str(bx[1]).replace(' ', '') + '.SH'

                args_list = [args_code]
                query_datas = {
                    "module": "pysec.etl.sec360.api.sec_api",
                    "method": "query_sec",
                    "args": args_list
                }
                query_result = post_data_job(query_datas)
                res_data = query_result['data']
                if res_data:
                    secu_id = res_data[0]['sec_id']
                    secu_type = get_secu_type(res_data[0]['sec_category'])
                    bx.append(secu_id)
                    bx.append(secu_type)
                else:
                    logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')

        ax_list.extend(bx_list)
        for lo in ax_list:
            logger.info(lo)
        return ax_list
    else:
        # 不带市场
        ax_list = []
        bx_list = []
        for a in data:
            code = str(a[0]).replace(' ', '')
            rs = sec_code_rules_match(code)
            if rs:
                ax_list.append(a)
            else:
                bx_list.append(a)

        args_list = []
        for ax in ax_list:
            with_suffix_code = sec_code_rules_match(str(ax[0]).replace(' ', ''))['code']
            ax[0] = with_suffix_code
            args_list.append(with_suffix_code)

        query_datas = {
            "module": "pysec.etl.sec360.api.sec_api",
            "method": "query_sec",
            "args": args_list
        }
        query_result = post_data_job(query_datas)
        res_data = query_result['data']
        for ax in ax_list:
            for res in res_data:
                if ax[0] == res['sec_code_market']:
                    if sec_code_rules_match(str(ax[0]).replace(' ', ''))['type'] in res['sec_category']:
                        ax.append(res['sec_id'])
                        ax.append(get_secu_type(res['sec_category']))

        for ax_ in ax_list:
            if len(ax_) == 4:
                args_code = sec_code_rules_match(str(ax_[0]).replace(' ', ''))['code']
                type_code = sec_code_rules_match(str(ax_[0]).replace(' ', ''))['type']
                # 注册到360
                sec_type = None
                if type_code == 'stock':
                    sec_type = 'AS'
                elif type_code == 'bond':
                    sec_type = 'B'
                elif type_code == 'fund':
                    sec_type = 'OF'

                args_dict = {
                    "sec_code_market": args_code,
                    "sec_type": sec_type,
                    "sec_name": str(ax_[1]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷',
                                                                                                           '(4)').replace(
                        '⑼', '(9)'),
                    "update_flag": 1
                }
                args_ = [args_dict]
                sync_datas = {
                    "module": "pysec.etl.sec360.api.sec_api",
                    "method": "sync_sec",
                    "args": args_
                }
                sync_result = post_data_job(sync_datas)
                data_rs = sync_result['data']
                if data_rs:
                    secu_id = data_rs[0]['sec_id']
                    secu_type = get_secu_type(data_rs[0]['sec_category'])
                    ax_.append(secu_id)
                    ax_.append(secu_type)
                    logger.info(f'该条已注册到360{ax_}')
                else:
                    logger.error(f'匹配到现有规则的证券代码{ax_}注册到360失败！请检查')

        for bx in bx_list:
            boName = str(bx[1]).replace(' ', '').replace('Ａ', 'A').replace('⑶', '(3)').replace('⑷', '(4)').replace(
                '⑼', '(9)')
            boCode = str(bx[0]).replace(' ', '').split('.')[0] if '.' in str(bx[0]).replace(' ', '') else str(
                bx[0]).replace(' ', '')
            data_dict = {"boCode": boCode}
            res = get_securities_type_job(data_dict)
            if res:
                if 'ETF' in boName or 'LOF' in boName or '基金' in boName or boName.endswith('基'):
                    temp_type = 'fund'
                    if len(res) == 1:
                        if res[0]['boIdType'] == temp_type:
                            # 提前知晓类型，就直接去判断类型，因为boid唯一
                            secu_id = res[0]['boId']
                            secu_type = res[0]['boIdType']
                            bx.append(secu_id)
                            bx.append(secu_type)
                    elif len(res) > 1:
                        for r in res:
                            if r['boIdType'] == temp_type:
                                secu_id = r['boId']
                                secu_type = r['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                                break
                            else:
                                continue

                    # 说明没有查到证券id
                    if len(bx) == 3 or len(bx) == 4:
                        logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                elif '债券' in boName or boName.endswith('债') or boName.endswith('转'):
                    temp_type = 'bond'
                    # 先匹配名称 若还是查不到证券id和类型再匹配类型
                    if len(res) == 1:
                        if res[0]['boName'] == boName:
                            secu_id = res[0]['boId']
                            secu_type = res[0]['boIdType']
                            bx.append(secu_id)
                            bx.append(secu_type)
                    elif len(res) > 1:
                        for r in res:
                            if r['boName'] == boName:
                                secu_id = r['boId']
                                secu_type = r['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                                break
                            else:
                                continue

                    if len(bx) == 3 or len(bx) == 4:
                        # 匹配名字仍查不到证券id和类型，则匹配类型
                        if len(res) == 1:
                            if res[0]['boIdType'] == temp_type:
                                # 提前知晓类型，就直接去判断类型，因为boid唯一
                                secu_id = res[0]['boId']
                                secu_type = res[0]['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                        elif len(res) > 1:
                            for r in res:
                                if r['boIdType'] == temp_type:
                                    secu_id = r['boId']
                                    secu_type = r['boIdType']
                                    bx.append(secu_id)
                                    bx.append(secu_type)
                                    break
                                else:
                                    continue

                    # 说明没有查到证券id
                    if len(bx) == 3 or len(bx) == 4:
                        logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                else:
                    if len(res) == 1:
                        if res[0]['boName'] == boName and boCode in res[0]['boIdCode']:
                            secu_id = res[0]['boId']
                            secu_type = res[0]['boIdType']
                            bx.append(secu_id)
                            bx.append(secu_type)
                        else:
                            logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                    elif len(res) > 1:
                        for r in res:
                            if r['boName'] == boName and boCode in r['boIdCode']:
                                secu_id = r['boId']
                                secu_type = r['boIdType']
                                bx.append(secu_id)
                                bx.append(secu_type)
                                break
                            else:
                                continue

                    # if len(bx) == 4 or len(bx) == 3:
                    #     if len(res) == 1:
                    #         if boCode in res[0]['boIdCode']:
                    #             secu_id = res[0]['boId']
                    #             secu_type = res[0]['boIdType']
                    #             bx.append(secu_id)
                    #             bx.append(secu_type)
                    #         else:
                    #             logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
                    #     elif len(res) > 1:
                    #         for r in res:
                    #             if boCode in r['boIdCode']:
                    #                 secu_id = r['boId']
                    #                 secu_type = r['boIdType']
                    #                 bx.append(secu_id)
                    #                 bx.append(secu_type)
                    #                 break
                    #             else:
                    #                 continue

                    # 说明没有查到证券id
                    if len(bx) == 3 or len(bx) == 4:
                        logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')
            else:
                logger.error(f'该证券代码{bx}获取证券id和证券类型失败，请检查！')

        ax_list.extend(bx_list)
        for li in ax_list:
            logger.info(li)
        return ax_list


# 融资融券rate解析公共方法
def securities_rzrq_parsing_data(rs, biz_type, data_):
    logger.info(f'进入数据解析处理，biz_type={biz_type}')
    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), biz_type, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in data_:
            if len(i) == 5:
                if i[2] is not None:
                    if biz_type == 1:
                        # 融资
                        if int(i[2]) < 100:
                            rate = None
                            insert_data_list.append(
                                [broker_id, i[3], i[4], biz_type, adjust_status_out, None, rate, 1, 1,
                                 datetime.datetime.now(),
                                 forever_end_dt, None])
                        elif int(i[2]) > 200:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                        else:
                            rate = int(i[2])
                            insert_data_list.append(
                                [broker_id, i[3], i[4], biz_type, adjust_status_in, None, rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt,
                                 None])
                    elif biz_type == 2:
                        # 融券
                        if int(i[2]) < 50:
                            rate = None
                            insert_data_list.append(
                                [broker_id, i[3], i[4], biz_type, adjust_status_out, None, rate, 1, 1,
                                 datetime.datetime.now(),
                                 forever_end_dt, None])
                        elif int(i[2]) > 200:
                            logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                            raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{i}')
                        else:
                            rate = int(i[2])
                            insert_data_list.append(
                                [broker_id, i[3], i[4], biz_type, adjust_status_in, None, rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt,
                                 None])
                else:
                    insert_data_list.append(
                        [broker_id, i[3], i[4], biz_type, adjust_status_out, None, None, 1, 1,
                         datetime.datetime.now(),
                         forever_end_dt, None])
            else:
                invalid_data_list.append(i)
        if invalid_data_list:
            logger.error(f'{rs[3]}，如下数据无证券id，请检查!{invalid_data_list}')
        if insert_data_list:
            logger.info(f'业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list)
            logger.info(f'业务数据入库完成，共{len(insert_data_list)}条')

    else:
        logger.info(f'进入不为空的判断')
        # 先把本次查询到的数据中有的 但是数据库中没有的更新为失效
        haved_list = []
        query_list = []
        for col in result.values.tolist():
            haved_list.append(col[2])

        for row_ in data_:
            if len(row_) == 5:
                query_list.append(row_[3])

        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            insert_data_list_new = []
            for b in s_list:
                for temp_data in data_:
                    if b == temp_data[3] and temp_data[2] is not None:
                        secu_type = temp_data[4]
                        if biz_type == 1:
                            # 融资
                            if int(temp_data[2]) < 100:
                                rate = None
                                insert_data_list_new.append([broker_id, temp_data[3], temp_data[4], biz_type, adjust_status_out, None, rate, 1, 1,
                                 datetime.datetime.now(),
                                 forever_end_dt, None])
                            elif int(temp_data[2]) > 200:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                            else:
                                rate = int(temp_data[2])
                                insert_data_list_new.append([broker_id, temp_data[3], temp_data[4], biz_type, adjust_status_in, None, rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt,
                                 None])
                        elif biz_type == 2:
                            # 融券
                            if int(temp_data[2]) < 50:
                                rate = None
                                insert_data_list_new.append(
                                    [broker_id, temp_data[3], temp_data[4], biz_type, adjust_status_out, None, rate, 1, 1,
                                     datetime.datetime.now(),
                                     forever_end_dt, None])
                            elif int(temp_data[2]) > 200:
                                logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                                raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{temp_data}')
                            else:
                                rate = int(temp_data[2])
                                insert_data_list_new.append(
                                    [broker_id, temp_data[3], temp_data[4], biz_type, adjust_status_in, None, rate, 1, 1,
                                     datetime.datetime.now(), forever_end_dt,
                                     None])
                    elif b == temp_data[3] and temp_data[2] is None:
                        insert_data_list_new.append([broker_id, temp_data[3], temp_data[4], biz_type, adjust_status_out, None, None, 1, 1,
                             datetime.datetime.now(),forever_end_dt, None])
            if insert_data_list_new:
                logger.info(f'业务数据入库开始,t_broker_mt_business_security...')
                insert_broker_mt_business_security(insert_data_list_new)
                logger.info(f'业务数据入库完成，共{len(insert_data_list_new)}条')

        b_list = list(set(haved_list).difference(set(query_list)))
        if b_list:
            for s in b_list:
                update_business_security((str(rs[1])).replace('-', ''), s, broker_id, biz_type)

        insert_data_list_noempty = []
        for row in data_:
            if len(row) == 5:
                if row[2] is not None:
                    sec_code = row[0]
                    sec_id = row[3]
                    secu_type = row[4]
                    round_rate = row[2]

                    db_record = df_exists_index(result, sec_code, sec_id)
                    if db_record is not None:
                        old_rate = db_record[7]
                        adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                        if adjust_status != adjust_status_invariant:
                            logger.info(f'进入比例调整处理逻辑...')
                            if adjust_status == adjust_status_in:
                                # 调入 新增记录,更新之前一条位失效（仅限同一天内）
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                if biz_type == 1:
                                    # 融资
                                    if int(round_rate) < 100:
                                        rate = None
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    elif int(round_rate) > 200:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                    else:
                                        rate = int(round_rate)
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_in, old_rate, rate,
                                             1, 1,
                                             datetime.datetime.now(), forever_end_dt, None])
                                elif biz_type == 2:
                                    # 融券
                                    if int(round_rate) < 50:
                                        rate = None
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    elif int(round_rate) > 200:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                    else:
                                        rate = int(round_rate)
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_in, old_rate, rate,
                                             1, 1,
                                             datetime.datetime.now(), forever_end_dt, None])
                            elif adjust_status == adjust_status_high:
                                # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                if biz_type == 1:
                                    # 融资
                                    if int(round_rate) < 100:
                                        rate = None
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    elif int(round_rate) > 200:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                    else:
                                        rate = int(round_rate)
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                elif biz_type == 2:
                                    # 融券
                                    if int(round_rate) < 50:
                                        rate = None
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    elif int(round_rate) > 200:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                    else:
                                        rate = int(round_rate)
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                            elif adjust_status == adjust_status_low:
                                # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                if biz_type == 1:
                                    # 融资
                                    if int(round_rate) < 100:
                                        rate = None
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    elif int(round_rate) > 200:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                    else:
                                        rate = int(round_rate)
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                elif biz_type == 2:
                                    # 融券
                                    if int(round_rate) < 50:
                                        rate = None
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                                    elif int(round_rate) > 200:
                                        logger.error(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                        raise Exception(f'本次解析数据违反业务规则!存在严重异常,不落地数据库,解析结束!{row}')
                                    else:
                                        rate = int(round_rate)
                                        insert_data_list_noempty.append(
                                            [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate, rate,
                                             1,
                                             1, datetime.datetime.now(), forever_end_dt, None])
                            elif adjust_status == adjust_status_out:
                                update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                                # 调出 更新记录，rate置为空，新增一条调处记录，更新其他字段,
                                insert_data_list_noempty.append(
                                    [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, None, 1, 1,
                                     datetime.datetime.now(), forever_end_dt, None])
                else:
                    insert_data_list_noempty.append(
                        [broker_id, row[3], row[4], biz_type, adjust_status_low, None, None,1,
                         1, datetime.datetime.now(), forever_end_dt, None])
            else:
                invalid_data_list.append(row)
        if invalid_data_list:
            logger.error(f'{rs[3]},如下数据无证券id，请检查!{invalid_data_list}')

        if insert_data_list_noempty:
            logger.info(f'业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list_noempty)
            logger.info(f'业务数据入库完成，共{len(insert_data_list_noempty)}条')


# 集中度分组数据解析--可充抵保证金
def securities_stockgroup_parsing_data(rs, biz_type, stockgroup_data):
    real_data = temp_deal(stockgroup_data, True)
    invalid_data_list = []
    if rs[3] == '上海交易所' or rs[3] == '深圳交易所':
        broker_key = '交易所'
    else:
        broker_key = rs[3]
    broker_id = broker_id_map.get(broker_key)
    result = query_business_security_item(str(rs[1]), biz_type, broker_id)
    if result.empty:
        logger.info(f'进入为空的判断')
        # 查询结果为空，第一次处理，从数据采集平台爬取到的数据进行入库处理,调整类型为调入
        insert_data_list = []
        for i in real_data:
            if len(i) == 6:
                insert_data_list.append(
                    [broker_id, i[4], i[5], biz_type, adjust_status_in, None, i[3], 1, 1, datetime.datetime.now(),
                     forever_end_dt, None])
            else:
                invalid_data_list.append(i)
        if invalid_data_list:
            logger.error(f'{rs[3]},如下数据无证券id，请检查!{invalid_data_list}')
        if insert_data_list:
            logger.info(f'业务数据入库开始...')
            insert_broker_mt_business_security(insert_data_list)
            logger.info(f'业务数据入库完成,共{len(insert_data_list)}条')
    else:
        logger.info(f'进入不为空的判断')
        # 先把本次查询到的数据中有的 但是数据库中没有的更新为失效
        haved_list = []
        query_list = []
        for col in result.values.tolist():
            haved_list.append(col[2])

        for row_ in real_data:
            if len(row_) == 6:
                query_list.append(row_[4])

        s_list = list(set(query_list).difference(set(haved_list)))
        if s_list:
            insert_data_list_new = []
            for b in s_list:
                for temp_data in real_data:
                    if b == temp_data[4] and temp_data[3] is not None:
                        insert_data_list_new.append([broker_id, temp_data[4], temp_data[5], biz_type, adjust_status_in, None, temp_data[3], 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None])

            if insert_data_list_new:
                insert_broker_mt_business_security(insert_data_list_new)

        b_list = list(set(haved_list).difference(set(query_list)))
        if b_list:
            for s in b_list:
                update_business_security((str(rs[1])).replace('-', ''), s, broker_id, biz_type)

        for row in real_data:
            if len(row) == 6:
                sec_code = row[1]
                sec_id = row[4]
                secu_type = row[5]
                round_rate = row[3]

                db_record = df_exists_index(result, sec_code, sec_id)
                if db_record is not None:
                    old_rate = db_record[7]
                    adjust_status = get_adjust_status_by_two_rate(old_rate, round_rate)
                    if adjust_status != adjust_status_invariant:
                        if adjust_status == adjust_status_in:
                            # 调入 新增记录，更新之前的为失效（同一天内）
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_in, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            if insert_data_list:
                                insert_broker_mt_business_security(insert_data_list)

                        elif adjust_status == adjust_status_high:
                            # 调高 更新记录，更新cur_value,adjust_type,data_status,biz_status
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_high, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            if insert_data_list:
                                insert_broker_mt_business_security(insert_data_list)
                        elif adjust_status == adjust_status_low:
                            # 调低 更新记录，更新cur_value,adjust_type,data_status,biz_status
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)

                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_low, old_rate, round_rate, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            if insert_data_list:
                                insert_broker_mt_business_security(insert_data_list)
                        elif adjust_status == adjust_status_out:
                            update_business_security((str(rs[1])).replace('-', ''), sec_id, broker_id, biz_type)
                            # 调出 更新记录，rate置为空，新增一条调处记录，更新其他字段,
                            insert_data_list = [
                                [broker_id, sec_id, secu_type, biz_type, adjust_status_out, old_rate, None, 1, 1,
                                 datetime.datetime.now(), forever_end_dt, None]]
                            if insert_data_list:
                                insert_broker_mt_business_security(insert_data_list)
                # else:
                #     insert_list = [[broker_id, sec_id, secu_type, biz_type, adjust_status_in, None, round_rate, 1, 1,
                #                     datetime.datetime.now(), forever_end_dt, None]]
                #     if insert_list:
                #         insert_broker_mt_business_security(insert_list)
            else:
                invalid_data_list.append(row)
        if invalid_data_list:
            logger.error(f'{rs[3]},如下数据无证券id，请检查!{invalid_data_list}')
