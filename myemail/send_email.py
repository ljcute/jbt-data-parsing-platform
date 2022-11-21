#!/usr/bin/env python3
# -*- coding: utf-8 -*-



"""
# 安装步骤
mkdir rong-web-risk-weekly-report
cd rong-web-risk-weekly-report
"""


import sys
import logging, os, traceback, datetime
# import requests
import json
from myemail.conf import *
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.header import Header

__author__ = "liuys02@igoldenbeta.com"

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), "log.txt"),
                    filemode='a')


class Mail:
    """邮件类"""
    def __init__(self, server, port, username, password, subject, email_from, email_to, email_tls=False, email_cc=None,
                 email_sender=None, email_eply_to=None, css_header=''):
        self.email_smtp_server = server
        self.email_smtp_port = port
        self.email_username = username
        self.email_password = password
        # 标题后面加日期
        self.mail_subject = subject
        self.css_header = css_header
        self.email_from = email_from
        self.email_to = email_to
        self.email_cc = email_cc
        self.email_sender = email_sender
        self.email_eply_to = email_eply_to
        self.email_tls = email_tls

    def addEmailAttach(self, f):
        """添加邮件附件"""
        fp = open(f, 'rb')
        att = MIMEText(fp.read(), 'base64', 'utf-8')
        fp.close()
        att["Content-Type"] = 'application/octet-stream'
        # 附件为中文
        #att.add_header("Content-Disposition", "attachment", filename=('gbk', '', os.path.basename(f)))
        att.add_header("Content-Disposition", "attachment", filename=Header(os.path.basename(f), 'utf-8').encode())
        return att

    # def getTimestamp(self):
    #     t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #     return str(t)
    #
    # def getDate(self):
    #     t = time.strftime("%Y-%m-%d", time.localtime())
    #     return str(t)

    def send_mail(self, content, attachment_list, img_list):
        """ 发送邮件
        img_list = [{"data": data, "id": "img_id"}]
        """
        logging.info("准备发送邮件...")
        msg = MIMEMultipart('related')
        msgtext = MIMEText(content, "html", "utf-8")
        # 添加邮件内容
        msg.attach(msgtext)
        # 设置邮件消息头
        msg['Subject'] = self.mail_subject
        msg['From'] = self.email_from
        msg['To'] = self.email_to
        if self.email_cc:
            msg['Cc'] = self.email_cc
        if self.email_eply_to:
            msg['Reply-to'] = self.email_eply_to
        if self.email_sender:
            msg['sender'] = self.email_sender
        # msg['Importance']='high'
        logging.info("SUBJECT:" + str(msg['Subject']))
        logging.info("发件人：{email_from}".format(email_from=msg["From"]))
        logging.info("收件人：{}".format(self.email_to))
        logging.info("抄送：{}".format(self.email_cc))
        assert isinstance(self.email_to, str)
        email_to = str(self.email_to).split(',')
        email_cc = str(self.email_cc).split(',')
        email_to = email_to + email_cc
        for i in img_list:
            if i:
                img = MIMEImage(i["data"])
                img.add_header('Content-ID', i["id"])
                msg.attach(img)
        for f in attachment_list:
            msg.attach(self.addEmailAttach(f))
        try:
            # 登录邮件服务器
            if self.email_tls:
                logging.info("使用SSL加密发送")
                server = smtplib.SMTP_SSL()
            else:
                logging.info("明文发送")
                server = smtplib.SMTP()
            server.connect(self.email_smtp_server, self.email_smtp_port)
            server.login(self.email_username, self.email_password)
            # 发邮件
            server.sendmail(self.email_from, email_to, msg.as_string())
            server.quit()
            logging.info("邮件发送成功！")
            return True
        except Exception as e:
            traceback.print_exc()
            logging.error(str(traceback.format_exc()))
            # logging.error(str(e))
            alert_msg = f"邮件发送失败！{self.email_to}"
            logging.error(alert_msg)
            return False

    def read_img_data(self, img_path):
        """获取图片内容"""
        with open(img_path,'rb') as img:
            return img.read()

if __name__ == '__main__':
    # 有附件
    attachment = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "【金贝塔两融市场专题报告】——券商调入融资融券标的情况.pdf")
    ]
    # 无附件
    attachment = []
    try:
        for email_to in EMAIL_RECIVER.split():
            mail = Mail(server=EMAIL_SERVER, port=EMAIL_PORT, username=EMAIL_USERNAME, password=EMAIL_PASSWORD,
                        subject=EMAIL_SUBJECT, email_from=EMAIL_FROM, email_to=email_to, email_cc=EMAIL_CC,
                        email_tls=True, email_sender=EMAIL_SENDER, email_eply_to=EMAIL_EPLY_TO)
            # 有图片
            # img_data1 = mail.read_img_data(os.path.join(os.path.dirname(os.path.abspath(__file__)), "rong-web_20221025.jpg"))
            # result = mail.send_mail(EMAIL_CONTENT, attachment, [{"data": img_data1, "id": "img1"}])
            # 无图片
            result = mail.send_mail(EMAIL_CONTENT, attachment, [])
    except Exception as e:
        traceback.print_exc()
        logging.error(str(traceback.format_exc()))