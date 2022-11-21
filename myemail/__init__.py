#!/usr/bin/env python3.10.0
# -*- coding: utf-8 -*-
"""
@Description :
@File        : __init__.py
@Date        : 2022-11-20
@Author      : Eagle (liuzh@igoldenbeta.com)
@Software    : PyCharm
"""

__author__ = 'Eagle (liuzh@igoldenbeta.com)'

import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from util.logs_utils import logger


class EmailClient(object):

    def __init__(self, host, port, user, password, sender, receivers):
        """
        #设置登录及服务器信息
        :param host:邮箱服务器地址
        :param port:邮箱服务器端口
        :param user:邮箱账户
        :param password:邮箱密码
        :param sender:发送者
        :param receivers:接收者
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = str(password)
        self.sender = sender
        self.receivers = receivers

    def send(self, message, receivers=None):
        # 登录并发送
        smtp = None
        try:
            smtp = smtplib.SMTP_SSL(host=self.host, port=smtplib.SMTP_SSL_PORT)
            smtp.connect(host=self.host)

            # with smtplib.SMTP('localhost') as s:
            #     s.send_message(msg)

            smtp.login(self.user, self.password)
            if receivers is None:
                smtp.sendmail(self.sender, self.receivers, message.as_string())
            else:
                smtp.sendmail(self.sender, receivers, message.as_string())
        except smtplib.SMTPException as e:
            logger.error(f"邮件发送失败！receivers = {receivers}， message={message}")
        finally:
            if smtp:
                smtp.close()
                smtp.quit()

    def get_init_message(self, receivers=None):
        """
        # 添加一个MIMEMultipart类，处理正文及附件
        """
        message = MIMEMultipart()
        message['From'] = self.sender
        if receivers is None:
            message['To'] = self.receivers[0]
        else:
            message['To'] = receivers[0]
        return message

    @classmethod
    def get_text_attach(cls, text_content, message, attachment_filename='文本附件'):
        """
        # 添加一个txt文本附件
        """
        # with open('abc.txt', 'r') as h:
        #     text_content = h.read()
        # 设置txt参数
        mime_text = MIMEText(text_content, 'plain', 'utf-8')
        # 附件设置内容类型，方便起见，设置为二进制流
        mime_text['Content-Type'] = 'application/octet-stream'
        # 设置附件头，添加文件名
        mime_text['Content-Disposition'] = f'attachment;filename="{attachment_filename}.txt"'
        message.attach(mime_text)

    @classmethod
    def get_picture_attach(cls, picture_content, attachment_filename='图片附件'):
        """
        # 添加照片附件
        """
        # with open('1.png', 'rb') as fp:
        #     picture_content = fp.read()
        mime_image = MIMEImage(picture_content)
        # 附件设置内容类型，方便起见，设置为二进制流
        mime_image['Content-Type'] = 'application/octet-stream'
        # 设置附件头，添加文件名
        mime_image['Content-Disposition'] = f'attachment;filename="{attachment_filename}.png"'
        return mime_image

    def send_email(self, receivers=None):
        message = self.get_init_message(receivers)
        message['Subject'] = 'title'
        # 推荐使用html格式的正文内容，这样比较灵活，可以附加图片地址，调整格式等
        with open('template1.html', 'r', encoding='ISO-8859-1') as f:
            content = f.read()
        # 设置html格式参数
        part1 = MIMEText(content, 'html', 'utf-8')
        # 将内容附加到邮件主体中
        message.attach(part1)
        # with open('abc.txt', 'r') as h:
        #     text_content = h.read()
        df = pd.DataFrame()
        df['name'] = ['张三', '李四', '王五']
        df['age'] = [27, 29, 31]
        message.attach(self.get_text_attach(df.to_csv(index=False), message))
        # 登录并发送
        self.send(receivers, message)


if __name__ == "__main__":
    from config import Config
    mail_cfg = Config.get_cfg().get_content("mail")
    ec = EmailClient(mail_cfg.get("host"), mail_cfg.get("port"), mail_cfg.get("user"), mail_cfg.get("pass"), mail_cfg.get("from"), mail_cfg.get("to"))
    ec.send_email()
