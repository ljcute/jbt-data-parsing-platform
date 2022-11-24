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
import pandas as pd
from io import StringIO
from util.logs_utils import logger
from data.ms.fdb import get_ex_discount_limit_rate
from data.ms.register_center import search_bo_info
from data.ms.sec360 import get_sec360_sec_id_code, register_sec360_security
sz = r'(\d+)'
zm = r'[\u0041-\u005a|\u0061-\u007a]+'
zw = r'[\u4e00-\u9fa5]+'


def get_df_from_cdata(cdata):
    return pd.read_csv(StringIO(cdata['data_text'][0]), sep=",")


class Cache(object):

    _sic_df = pd.DataFrame(columns=['sec_type', 'sec_id', 'sec_code', 'sec360_name', 'exchange_sec_name'])

    @classmethod
    def get_sic_df(cls):
        return Cache._sic_df.copy()

    @classmethod
    def set_sic_df(cls, sic_df):
        Cache._sic_df = sic_df
        return cls.get_sic_df()


def get_sic_df():
    return Cache.get_sic_df()


def set_sic_df(_df360, _df_exchange, exchange=False):
    if _df360.empty:
        return get_sic_df()
    mutex = threading.Lock()
    mutex.acquire(60)  # 里面可以加blocking(等待的时间)或者不加，不加就会一直等待(堵塞)
    _df = _df360.copy()
    if exchange:
        _df = _df.merge(_df_exchange[['sec_code', 'sec_name']], on='sec_code').copy()
        _df.rename(columns={'sec_name': 'exchange_sec_name'}, inplace=True)
    else:
        _df['exchange_sec_name'] = None
    df = Cache.set_sic_df(pd.concat([Cache.get_sic_df(), _df]).drop_duplicates())
    mutex.release()
    return df


def refresh_sic_df(_df, exchange=False):
    if _df.empty:
        return get_sic_df()
    # 证券360刷证券ID
    sec = get_sec360_sec_id_code(_df['sec_code'].tolist())
    # 注册中心刷证券ID
    no_sec360 = _df.loc[~_df['sec_code'].isin(sec['sec_code'].tolist())][sec.columns.tolist() + ['sec_name']]
    no_sec360['sec360_name'] = no_sec360['sec_name']
    no_sec360 = no_sec360[sec.columns.tolist()]
    if not no_sec360.empty:
        # 注册中心找对象
        for index, row in no_sec360.iterrows():
            bo = search_bo_info(row['sec_code'][:-3])
            if bo.empty:
                continue
            # 代码全匹配
            _bo = bo.loc[bo['sec_code'] == row['sec_code']]
            if not _bo.empty:
                row['sec_type'] = _bo['sec_type'].tolist()[0]
                row['sec_id'] = _bo['sec_id'].tolist()[0]
                row['sec360_name'] = min(_bo['sec_name'].tolist(), key=len)
                sec = pd.concat([sec, row.to_frame().T])
                continue
            # 名称全匹配
            _bo = bo.loc[bo['sec_name'] == row['sec360_name']]
            if not _bo.empty:
                row['sec_type'] = _bo['sec_type'].tolist()[0]
                row['sec_id'] = _bo['sec_id'].tolist()[0]
                row['sec_code'] = _bo['sec_code'].tolist()[0]
                sec = pd.concat([sec, row.to_frame().T])
                continue
            # 名称包含匹配
            for idx, rw in bo.iterrows():
                if rw['sec_name'] in row['sec360_name'] or row['sec360_name'] in rw['sec_name']:
                    row['sec_type'] = rw['sec_type']
                    row['sec_id'] = rw['sec_id']
                    row['sec_code'] = rw['sec_code']
                    sec = pd.concat([sec, row.to_frame().T])
                    break
            # 名称拆解再模糊匹配
            sec_name = re.findall(sz, row['sec360_name']) + re.findall(zm, row['sec360_name']) + cn_char_arr_split(re.findall(zw, row['sec360_name']))
            for idx, rw in bo.iterrows():
                _sec_name = re.findall(sz, rw['sec_name']) + re.findall(zm, rw['sec_name']) + cn_char_arr_split(re.findall(zw, rw['sec_name']))
                if len(set(sec_name) & set(_sec_name)) > 0:
                    row['sec_type'] = rw['sec_type']
                    row['sec_id'] = rw['sec_id']
                    row['sec_code'] = rw['sec_code']
                    sec = pd.concat([sec, row.to_frame().T])
                    break
    return set_sic_df(sec, _df, exchange)


def register_sic_df(_df, exchange=False):
    if _df.empty:
        return get_sic_df()
    return set_sic_df(register_sec360_security(_df[['sec_type', 'sec_code', 'sec_name']]), _df, exchange)


def code_ref_id(_df, exchange=False):
    df1 = _df.merge(get_sic_df(), how='left', on='sec_code')
    no_sec_id_df = df1.loc[df1['sec_id'].isna()]
    if not no_sec_id_df.empty:
        # 刷证券ID
        df1 = _df.merge(refresh_sic_df(no_sec_id_df, exchange), how='left', on='sec_code')
        zc_sec_df = df1.loc[df1['sec_id'].isna()][['market', 'sec_code', 'sec_name']]
        if not zc_sec_df.empty:
            # 监控并Email关键词：本次需注册证券对象有
            logger.warn(f"本次需注册证券对象有({zc_sec_df.index.size}只)：\n{zc_sec_df.reset_index(drop=True)}")
    return df1.loc[~df1['sec_id'].isna()].copy()


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
    _df = pd.merge(df, sbf_rate, how="left", on="sec_id")
    no_rate_df = _df.loc[_df['rate'].isna()][['sec_type', 'sec_id', 'sec_code', 'sec_name']]
    if not no_rate_df.empty:
        # 监控并Email关键词：如下证券缺少交易所折算率上限
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        logger.warn(f"如下证券缺少交易所折算率上限({no_rate_df.index.size}只)：\n{no_rate_df.reset_index(drop=True)}")
    return _df


SZ_STOCK_PRE1 = ['2', '3']
SZ_STOCK_PRE2 = ['00']
SZ_FUND_PRE3 = ['161', '164']
SZ_FUND_PRE4 = ['1848']
SZ_BOND_PRE3 = ['102', '104', '105', '106', '107', '108', '112', '114', '115', '116', '117', '119', '138', '148', '190', '191', '192', '198']
SZ_BOND_PRE4 = ['1003', '1004', '1006', '1008', '1009', '1019', '1111', '1119', '1181', '1182', '1183', '1184', '1185', '1186', '1187', '1188', '1189', '1215', '1263', '1266', '1267', '1281', '1282', '1290', '1330', '1331', '1333', '1351', '1372', '1373', '1374', '1378', '1379', '1395', '1396', '1397', '1398', '1399', '1930']
SZ_BOND_PRE5 = ['10020', '10021', '10050', '10051', '10070', '10071', '10998', '10999', '11101', '11102', '11103', '11104', '11105', '11106', '11107', '11108', '11109', '11802', '11803', '11804', '11805', '11806', '11807', '11808', '11809', '12311', '12312', '12313', '12314', '12315', '12801', '12802', '12803', '12804', '12805', '12806', '12807', '12808', '12809', '13320', '13321', '13322', '13323', '13324', '13326', '13327', '13328', '13329', '13716', '13717', '13718', '13719', '13949', '14995']

SH_STOCK_PRE1 = ['6', ]
SH_STOCK_PRE2 = ['90']
SH_FUND_PRE1 = ['5', ]
SH_BOND_PRE2 = ['01', '02', '17', '95']
SH_BOND_PRE3 = ['110', '113', '122', '130', '131', '132', '140', '142', '143', '145', '146', '147', '151', '152', '155', '156', '157', '182', '183', '185', '186', '188', '189', '196', '197']
SH_BOND_PRE4 = ['1000', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1232', '1233', '1234', '1235', '1236', '1237', '1238', '1239', '1241', '1242', '1243', '1244', '1245', '1246', '1247', '1248', '1249', '1251', '1252', '1271', '1272', '1273', '1274', '1275', '1276', '1277', '1278', '1279', '1296', '1297', '1298', '1299', '1355', '1356', '1357', '1358', '1500', '1501', '1502', '1503', '1504', '1505', '1506', '1507', '1508', '1509', '1591', '1592', '1593', '1594', '1595', '1600', '1628', '1629', '1636', '1637', '1650', '1651', '1652', '1654', '1656', '1657', '1658', '1659', '1662', '1663', '1665', '1666', '1667', '1668', '1669', '1671', '1672', '1674', '1676', '1677', '1678', '1679', '1685', '1686', '1688', '1689', '1690', '1693', '1694', '1695', '1696', '1697', '1698', '1699', '1800', '1806', '1807', '1840', '1841', '1842', '1843', '1844', '1845', '1947', '1948', '1949']
SH_BOND_PRE5 = ['10017', '10019', '10022', '10023', '10056', '10079', '10105', '10106', '10107', '10108', '10109', '10113', '10114', '10115', '10116', '10117', '10118', '10119', '10123', '10124', '10125', '10126', '10127', '10128', '10129', '10133', '10134', '10135', '10136', '10137', '10138', '10139', '10144', '10145', '10146', '10147', '10148', '10149', '10153', '10154', '10155', '10156', '10157', '10158', '10159', '10163', '10164', '10165', '10166', '10167', '10168', '10169', '10173', '10174', '10175', '10176', '10177', '10178', '10183', '10184', '10185', '10186', '10187', '10188', '10189', '11100', '12101', '12102', '12403', '12404', '12405', '12406', '12407', '12408', '12409', '12501', '12503', '12504', '12505', '12507', '12509', '12531', '12532', '12533', '12534', '12535', '12536', '12537', '12538', '12539', '12540', '12541', '12542', '12543', '12544', '12545', '12546', '12547', '12549', '12550', '12551', '12553', '12554', '12555', '12556', '12560', '12561', '12564', '12565', '12566', '12567', '12568', '12569', '12574', '12575', '12576', '12577', '12578', '12579', '12580', '12581', '12583', '12584', '12585', '12586', '12587', '12590', '12591', '12592', '12597', '12598', '12599', '12601', '12708', '12709', '13541', '13544', '13545', '13546', '13547', '13548', '13549', '15901', '15902', '15903', '15904', '15905', '15906', '15907', '15908', '15909', '15965', '15966', '15967', '15968', '15969', '16015', '16016', '16017', '16018', '16019', '16020', '16023', '16024', '16025', '16026', '16027', '16028', '16029', '16030', '16033', '16034', '16035', '16036', '16037', '16038', '16039', '16040', '16043', '16044', '16045', '16046', '16047', '16048', '16049', '16053', '16054', '16055', '16056', '16057', '16058', '16059', '16065', '16066', '16067', '16068', '16069', '16073', '16074', '16075', '16076', '16077', '16078', '16079', '16082', '16083', '16084', '16085', '16086', '16087', '16088', '16089', '16090', '16093', '16094', '16095', '16096', '16097', '16098', '16099', '16201', '16202', '16203', '16204', '16205', '16206', '16207', '16208', '16209', '16211', '16212', '16213', '16214', '16215', '16216', '16217', '16218', '16219', '16222', '16223', '16224', '16225', '16226', '16227', '16228', '16229', '16231', '16232', '16233', '16234', '16235', '16236', '16237', '16238', '16239', '16240', '16242', '16243', '16244', '16245', '16246', '16247', '16248', '16249', '16252', '16253', '16254', '16255', '16256', '16257', '16258', '16259', '16261', '16262', '16263', '16264', '16265', '16266', '16267', '16268', '16269', '16273', '16274', '16275', '16276', '16277', '16278', '16279', '16301', '16302', '16303', '16304', '16305', '16306', '16307', '16308', '16309', '16312', '16313', '16314', '16315', '16316', '16317', '16318', '16319', '16322', '16323', '16324', '16325', '16326', '16327', '16328', '16329', '16331', '16332', '16333', '16334', '16335', '16336', '16337', '16338', '16339', '16342', '16343', '16344', '16345', '16346', '16347', '16348', '16349', '16351', '16352', '16353', '16354', '16355', '16356', '16357', '16358', '16359', '16383', '16384', '16385', '16386', '16387', '16388', '16389', '16391', '16392', '16393', '16394', '16395', '16396', '16397', '16398', '16399', '16532', '16533', '16534', '16535', '16536', '16537', '16538', '16539', '16553', '16554', '16555', '16556', '16557', '16558', '16559', '16603', '16604', '16605', '16606', '16607', '16608', '16609', '16611', '16612', '16613', '16614', '16615', '16616', '16617', '16618', '16619', '16641', '16642', '16643', '16644', '16645', '16646', '16647', '16648', '16649', '16701', '16702', '16703', '16704', '16705', '16706', '16707', '16708', '16709', '16731', '16732', '16733', '16734', '16735', '16736', '16737', '16738', '16739', '16751', '16752', '16753', '16754', '16755', '16756', '16757', '16758', '16759', '16801', '16802', '16803', '16804', '16805', '16806', '16807', '16808', '16809', '16811', '16812', '16813', '16814', '16815', '16816', '16817', '16818', '16819', '16821', '16822', '16823', '16824', '16825', '16826', '16827', '16828', '16829', '16831', '16832', '16833', '16834', '16835', '16836', '16837', '16838', '16839', '16841', '16842', '16843', '16844', '16845', '16846', '16847', '16848', '16849', '16871', '16872', '16873', '16874', '16875', '16876', '16877', '16878', '16879', '16911', '16912', '16913', '16914', '16915', '16916', '16917', '16918', '16919', '16921', '16922', '16923', '16924', '16925', '16926', '16927', '16928', '16929', '18011', '18012', '18013', '18014', '18015', '18016', '18017', '18018', '18019', '18021', '18023', '18024', '18025', '18026', '18027', '18028', '18029', '18031', '18032', '18033', '18034', '18035', '18036', '18037', '18038', '18039', '18041', '18042', '18043', '18044', '18045', '18046', '18047', '18048', '18049', '18051', '18052', '18053', '18054', '18055', '18056', '18057', '18058', '18059', '18081', '18083', '18084', '18085', '19464', '19465', '19466', '19467', '19468', '19469']

BJ_STOCK_PRE1 = ['4']
BJ_STOCK_PRE2 = ['83', '87']
BJ_BOND_PRE2 = ['80']
BJ_BOND_PRE4 = ['8122']


def code_rule_to_market(code, name):
    if code is None:
        return
    pre1_code = str(code)[:1]
    pre2_code = str(code)[:2]
    pre3_code = str(code)[:3]
    pre4_code = str(code)[:4]
    pre5_code = str(code)[:5]
    ret_code = None
    sec_type = None
    if pre1_code in SZ_STOCK_PRE1 or pre2_code in SZ_STOCK_PRE2:
        ret_code = 'SZ_stock'
    elif pre1_code in SH_STOCK_PRE1 or pre2_code in SH_STOCK_PRE2:
        ret_code = 'SH_stock'
    elif pre1_code in BJ_STOCK_PRE1 or pre2_code in BJ_STOCK_PRE2:
        ret_code = 'BJ_stock'
    if ret_code is not None:
        return ret_code

    if pre3_code in SZ_FUND_PRE3 or pre4_code in SZ_FUND_PRE4:
        ret_code = 'SZ_fund'
    elif pre1_code in SH_FUND_PRE1:
        ret_code = 'SH_fund'
    elif pre3_code in SZ_BOND_PRE3 or pre4_code in SZ_BOND_PRE4 or pre4_code in SZ_BOND_PRE5:
        ret_code = 'SZ_bond'
    elif pre2_code in SH_BOND_PRE2 or pre3_code in SH_BOND_PRE3 or pre4_code in SH_BOND_PRE4 or pre5_code in SH_BOND_PRE5:
        ret_code = 'SH_bond'
    elif pre2_code in BJ_BOND_PRE2 or pre4_code in BJ_BOND_PRE4:
        ret_code = 'BJ_bond'
    if name is not None:
        _name = re.sub(r'[0-9]+', '', name)
        if 'ETF' in _name or 'LOF' in _name or '基金' in _name or _name.endswith('基'):
            sec_type = 'fund'
    if ret_code is not None and sec_type is not None:
        if not ret_code.endswith(sec_type):
            logger.warn(f"识别内容冲突，需升级规则: 内容code={code}, name={name}; 结果sec_type={sec_type}, ret_code={ret_code}")
            ret_code = ('SH_' if ret_code[:2] == 'SZ' else 'SZ_' if ret_code[:2] == 'SH' else 'BJ_') + sec_type
    if '债券' in _name or _name.endswith('转') or _name.endswith('债'):
        sec_type = 'bond'
    return ret_code


def cn_char_split(value, split_length=2):
    if isinstance(value, str):
        length = len(value)
        if length > split_length:
            _value = []
            for i in range(length - split_length + 1):
                _value.append(value[i:i+split_length])
            return _value
    return [value]


def cn_char_arr_split(values, split_length=2):
    if isinstance(values, list):
        _values = []
        for value in values:
            _values += cn_char_split(value, split_length)
        return _values
    return values


def match_sid_by_code_and_name(df):
    """"
    1、找出df中每个代码，在sid_df中只存在1个代码的代码，认为是相同代码
    2、再再出df中每个代码的前5位，在sid_df中只存在1个市场和证券类型的代码，认为是相同市场的相同证券类型
    """
    if df.empty:
        return df
    df1 = df[['sec_code', 'sec_name']].drop_duplicates()
    _sid_df = get_sic_df().rename(columns={'sec_type': 'stp', 'sec_id': 'sid', 'sec_code': 'scd'})
    _sid_df[['cd6', 'market']] = _sid_df['scd'].str.split('.', n=1, expand=True)
    _sid_df['cd5'] = _sid_df['cd6'].str[:-1]
    df1['code5'] = df1['sec_code'].str[:-1]
    # 6位代码唯一匹配
    match6 = _sid_df.merge(df1, how='inner', left_on='cd6', right_on='sec_code')
    ok_match6 = match6.drop_duplicates(['cd6'], keep=False)
    # 6位代码 + 名称完全匹配
    match6_nm = match6.loc[~match6['cd6'].isin(ok_match6['cd6'].tolist())]
    match6_nm = match6_nm.loc[((match6_nm['sec360_name'] == match6_nm['sec_name']) | (match6_nm['exchange_sec_name'] == match6_nm['sec_name']))]
    match = pd.concat([ok_match6, match6_nm])
    # 6位代码 + 名称模糊匹配(包含关系)
    like_name = match6.loc[~match6['cd6'].isin(match['cd6'].tolist())]
    _like_name = pd.DataFrame(columns=like_name.columns)
    for index, row in like_name.iterrows():
        if row['sec_name'] in row['sec360_name'] or row['sec_name'] in row['exchange_sec_name'] or row['sec360_name'] in row['sec_name'] or row['exchange_sec_name'] in row['sec_name']:
            _like_name = pd.concat([_like_name, row.to_frame().T])
    match = pd.concat([match, _like_name])
    # 6位代码 + 名称模糊匹配(按数字、字母(转大写)、中文（至少2个中文搭配）拆分后包含关系)
    like_name = match6.loc[~match6['cd6'].isin(match['cd6'].tolist())]
    _like_name = pd.DataFrame(columns=like_name.columns)
    for index, row in like_name.iterrows():
        sec_name = re.findall(sz, row['sec_name']) + re.findall(zm, row['sec_name']) + cn_char_arr_split(re.findall(zw, row['sec_name']))
        sec360_name = re.findall(sz, row['sec360_name']) + re.findall(zm, row['sec360_name']) + cn_char_arr_split(re.findall(zw, row['sec360_name']))
        exchange_sec_name = re.findall(sz, row['exchange_sec_name']) + re.findall(zm, row['exchange_sec_name']) + cn_char_arr_split(re.findall(zw, row['exchange_sec_name']))
        if len(set(sec_name) & set(sec360_name)) > 0 or len(set(sec_name) & set(exchange_sec_name)) > 0:
            _like_name = pd.concat([_like_name, row.to_frame().T])
    match = pd.concat([match, _like_name])
    # 剩余5位代码匹配
    _df = df1.loc[~df1['sec_code'].isin(match['cd6'].tolist())]
    match5 = _sid_df.merge(_df, how='inner', left_on='cd5', right_on='code5')
    # 相同5位代码，只有1个市场和1个证券类型，则该5位，也为该市场和该证券类型
    _ok_match5 = match5.drop_duplicates(['cd5', 'stp', 'market'])
    ok_match5 = _ok_match5.drop_duplicates(['cd5'], keep=False)
    # 匹配证券
    match = pd.concat([match, ok_match5])[['sec_code', 'sec_name', 'sid', 'stp', 'market', 'scd']]
    # 未匹配证券s
    no_match = df1.loc[~df1['sec_code'].isin(match['sec_code'].tolist())][['sec_code', 'sec_name']].copy()
    # 未匹配证券，通过注册中心寻找历史名称再识别
    no_match['stp'] = None
    no_match['sid'] = None
    no_match['scd'] = None
    no_match['market'] = None
    for index, row in no_match.iterrows():
        sec = search_bo_info(row['sec_code'])
        _zc_sec = sec.loc[sec['sec_name'] == row['sec_name']]
        if not _zc_sec.empty:
            row['sid'] = _zc_sec['sec_id'].tolist()[0]
            row['stp'] = _zc_sec['sec_type'].tolist()[0]
            row['scd'] = _zc_sec['sec_code'].tolist()[0]
            row['market'] = _zc_sec['sec_code'].tolist()[0][-2:]
            continue
        sec_name = re.findall(sz, row['sec_name']) + re.findall(zm, row['sec_name']) + cn_char_arr_split(re.findall(zw, row['sec_name']))
        for idx, rw in sec.iterrows():
            _sec_name = re.findall(sz, rw['sec_name']) + re.findall(zm, rw['sec_name']) + cn_char_arr_split(re.findall(zw, rw['sec_name']))
            if len(set(sec_name) & set(_sec_name)) > 0:
                row['sid'] = rw['sec_id']
                row['stp'] = rw['sec_type']
                row['scd'] = rw['sec_code']
                row['market'] = rw['sec_code'][-2:]
                break
    match = pd.concat([match, no_match.loc[~no_match['sid'].isna()]])
    no_match = df1.merge(match, how='left', on=['sec_code', 'sec_name'])
    no_match = no_match.loc[no_match['sid'].isna()]
    if not no_match.empty:
        # 监控并Email关键词：如下证券对象无法识别
        logger.warn(f"如下证券对象无法识别\n {no_match}")
    return match.rename(columns={'sid': 'sec_id', 'stp': 'sec_type'})
