EMAIL_SERVER = "smtpdm.aliyun.com"
EMAIL_PORT = 465
EMAIL_TLS = True
EMAIL_USERNAME = "info@mail.igbsc.com"
EMAIL_PASSWORD = "hkigbsc2016"
EMAIL_FROM = "info@mail.igbsc.com"
EMAIL_SENDER = 'info@mail.igbsc.com'
EMAIL_EPLY_TO = 'info@mail.igbsc.com'
EMAIL_TO = 'liuzh@igoldenbeta.com'
EMAIL_CC = None
EMAIL_SUBJECT = '【金贝塔两融市场专题报告】——券商调入融资融券标的情况'


# 收件人名单
EMAIL_RECIVER_XXX = """
liuys02@igoldenbeta.com
zhouying01@igoldenbeta.com
lys523094451@126.com
"""

# luolx@bici.com.cn
# zhuwb01@igoldenbeta.com

"""
-- 查询邮件列表的SQL
SELECT `email`  FROM  `t_contact` WHERE  `contact_id` IN (
SELECT `contact_id` FROM `t_user`  WHERE `user_id` in(
      SELECT user_id from t_user_role WHERE role_id in (
        -- SELECT role_id from t_role WHERE role_name='探雷两融使用角色'
        SELECT role_id from t_role WHERE role_name='基金优选使用角色'
    )  and start_dt2 < now() and end_dt2 > now()
  )  AND create_dt < now()
) AND `email` LIKE '%@%' and `email` NOT LIKE '%igoldenbeta.com' and `email` NOT LIKE '%bici.com.cn'
"""

EMAIL_RECIVER = """
liuzh@igoldenbeta.com
"""

# EMAIL_CONTENT = """
# <p>尊敬的探雷工兵用户：</p>
# <p></p>
# <p>您好！2022年10月24日，沪深交易所正式扩大融资融券标的股票范围，上交所将主板标的股票数量由现有的800只扩大到1000只，深交所将注册制股票以外的标的股票数量由现有800只扩大到1200只。</p>
# <p>为方便各位领导及时了解券商同业调整情况，金贝塔基于“金贝塔资管嘉—探雷工兵（两融版）”SaaS系统，结合券商官网调整公告，对系统内11家券商（中信证券、国泰君安、华泰证券、国信证券、中泰证券、兴业证券、中信建投、广发证券、招商证券、银河证券、国元证券）融资融券标的调入情况作了分析，并输出
# <b><span collor="red">《金贝塔两融市场专题报告》</span></b>，敬请查阅！
# </p> </div>
# """


EMAIL_CONTENT = """
        <head><style>
        body { line-height: 1.5; }
        p { margin: 1em auto; }
        body { font-size: 14px; font-family: "Microsoft YaHei UI"; color: rgb(0, 0, 0); line-height: 1.5; }
        </style></head><body>

<img src="cid:img1"  />

</body>
 """