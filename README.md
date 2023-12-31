# jbt-data-parsing-platform

# 互联网数据解析平台

# 环境：Python 3.9

# 业务数据解析流程伪代码：

    担保券：折算率比例
    融资标的券：融资保证金比例
    融券标的券：融券保证金比例

    ===招商证券有修改===
    中泰证券 海通证券 安信证券 中金财富

    主要为处理同业数据,具体处理流程如下：

    1.首先查询’券商两融业务证券数据表‘，若查询为空则为第一次处理数据，通过从数据采集平台爬取到的数据进行入库处理.
      startdt = 处理当天日期，enddt = 2999.12.31, cur_value = rate, pre_value = null

    2.若首次查询不为空，则进行业务数据解析处理，根据start_dt <= biz_dt and biz_dt < end_dt的条件进行查询，获取到一条之前的数据处理记录记为（1），
      再从数据采集平台获取到当日爬取到的数据记为（2），对（1），（2）两条数据进行比较，主要比较rate是否相同

    3.若（1），（2）两条数据的rate相同，则调整类型为不变，若rate不同，则进行比较，结果为调高或调低。

    4.注:此处需要判断rate来自哪条数据，若rate在（2）中存在但不在（1）中存在，则说明该条记录为第一次新增，则调整状态为调入

    5.若rate在（1）中存在但不在（2）中存在，则说明该条记录为调入失效，则调整状态为调出（调出的意思为具体券商或者公司没有使用该条记录数据进行业务处理）
      调整状态为调出，则需新增一条调出记录，生效日期为当天日期，结束日期为2999.12.31
      
    6.注：每个券商或公司永远只能存在一条生效的记录，每次的rate调整变动，都需要将之前的记录（数据状态）置为失效状态，且业务状态置为受限

    7.若处理当天数据采集平台爬取了多条当天的记录，则需要把每一条记录与上一个交易日历日期进行比较，得到rate的变动值，及其调整类型。
      此处有更优的解决方案：通过数据采集平台的log日志表，把当天采集的多条记录进行对比，若完全相同则无需处理，若有不同，则取出不同的数据进行处理。

    8.上述所有处理均应在循环中进行，处理所有券商公司的记录。


    # args_code = None
    # temp_market = str(data[0]).replace(' ', '')
    # if temp_market == '深圳' or temp_market == '深市' or temp_market == '2' or temp_market == '深交所' or temp_market == '深A' or temp_market == '深圳证券交易所':
    #     args_code = str(data[1]).replace(' ', '') + '.SZ'
    # elif temp_market == '上海' or temp_market == '沪市' or temp_market == '1' or temp_market == '上交所' or temp_market == '沪A' or temp_market == '上海证券交易所':
    #     args_code = str(data[1]).replace(' ', '') + '.SH'
      

# 问题排查流程：
    1。通过观察解析监控邮件告警信息，筛选出为告警状态的数据以及业务调整数值过大的数据。
    2。通过对parsing_monitoring.py文件进行debug，可以获取具体的异常数据（注意区分业务类型，担保券，融资标的券，融券标的券）
    3。在具体卷商的具体业务类型出打好断点之后，往下运行，可以获取到其具体的调入，调出，调高，调低的详细个数和具体的票。
    4。然后对比调整个数是否与邮件告警中的一致，如果一致则查询采集日志表当天和上一个交易日的采集数据。
    5。对上一个交易日的数据和当日的数据进行对比，看调整个数是否与邮件告警相同。
    6。这里比较常见的问题分为两类：一种是官网公布的数据波动或错误，导致采集程序对比和解析程序对比不一致而导致邮件报警，这种情况则去卷商官网核实，如果官网数据确实有问题，则忽略该告警。
                             第二种是官网公布了重复数据，但是解析程序只会有一条正确数据入库而导致两边调整数量不一致。   