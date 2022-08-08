# jbt-data-parsing-platform

# 互联网数据解析平台

# 环境：Python 3.9

# 业务数据解析流程伪代码：
    主要为处理同业数据,具体处理流程如下：

    1.首先查询’券商两融业务证券数据表‘，若查询为空则为第一次处理数据，通过从数据采集平台爬取到的数据进行入库处理.
      startdt = 处理当天日期，enddt = 2999.12.31, cur_value = rate, pre_value = null

    2.若首次查询不为空，则进行业务数据解析处理，根据startdt<= bizdt < enddt的条件进行查询，获取到一条之前的数据处理记录记为（1），
      再从数据采集平台获取到当日爬取到的数据记为（2），对（1），（2）两条数据进行比较，主要比较rate是否相同

    3.若（1），（2）两条数据的rate相同，则调整类型为不变，若rate不同，则进行比较，结果为调高或调低。

    4.注:此处需要判断rate来自哪条数据，若rate在（2）中存在但不在（1）中存在，则说明该条记录为第一次新增，则调整状态为调入

    5.若rate在（1）中存在但不在（2）中存在，则说明该条记录为调入失效，则调整状态为调出（调出的意思为具体券商或者公司没有使用该条记录数据进行业务处理）
      调整状态为调出，则需新增一条调出记录，生效日期为当天日期，结束日期为2999.12.31
      
    6.注：每个券商或公司永远只能存在一条生效的记录，每次的rate调整变动，都需要将之前的记录（数据状态）置为失效状态，且业务状态置为受限

    7.若处理当天数据采集平台爬取了多条当天的记录，则需要把每一条记录与上一个交易日历日期进行比较，得到rate的变动值，及其调整类型。
      此处有更优的解决方案：通过数据采集平台的log日志表，把当天采集的多条记录进行对比，若完全相同则无需处理，若有不同，则取出不同的数据进行处理。

    8.上述所有处理均应在循环中进行，处理所有券商公司的记录。

      