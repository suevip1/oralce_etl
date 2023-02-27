#!/usr/bin/env python
# -*- coding:utf-8 -*-
# cython:language_level=3

# Project：airline_sqletl
# Datetime：2022/4/81:51 下午
# Description：发送邮件类
# @author 汤奇朋
# @version 1.0
import json
import os
import smtplib
import sys
import time

import requests
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Notify:
    def __init__(self):
        self.sender = 'hbhk@aograph.com'
        self.mail_host = "smtp.exmail.qq.com"  # 设置服务器
        self.mail_user = "hbhk@aograph.com"  # 用户名
        self.mail_pass = "Zz!111111"  # 口令
        self.weChatBaseUrl = "https://qyapi.weixin.qq.com/cgi-bin/"
        self.access_token = ''

    def sendEmail(self, message, receivers):
        try:
            smtpObj = smtplib.SMTP_SSL(self.mail_host, 465)
            smtpObj.login(self.mail_user, self.mail_pass)
            smtpObj.sendmail(self.sender, receivers, message.as_string())
            print("邮件发送成功")
        except smtplib.SMTPException:
            print("Error: 无法发送邮件")

    def notifyD0predict(self, msg):
        receivers = ['libin@aograph.com', 'tangqipeng@aograph.com']
        content = f'<html><body><p style="color:red;">{msg}</p></body></html>'
        message = MIMEText(content, 'html', 'utf-8')
        message['From'] = Header("hbhk@aograph.com", 'utf-8')
        message['To'] = Header("tangqipeng@aograph.com", 'utf-8')
        message['To'] = Header("libin@aograph.com", 'utf-8')
        subject = 'D0预测辅助表任务监控'
        message['Subject'] = Header(subject, 'utf-8')
        self.sendEmail(message, receivers)

    def notifyReleaseEtl(self, tag):
        receivers = ['shihaonan@aograph.com', 'tangqipeng@aograph.com']
        message = MIMEMultipart()
        message['From'] = Header("hbhk@aograph.com", 'utf-8')
        message['To'] = Header("tangqipeng@aograph.com", 'utf-8')
        message['To'] = Header("shihaonan@aograph.com", 'utf-8')
        if tag == 'D':
            envT = '东航测试环境'
        else:
            envT = '东航生产环境'
        subject = f'{envT}etl发布更新'
        message['Subject'] = Header(subject, 'utf-8')
        content = f'<h1>请更新新包到{envT}正式环境。<h1>\n<h2>更新步骤如下：<h2>\n' \
                  '<h3>1.上传tgz包到指定路径\n<h3>' \
                  '<h3>2.将feature-engineering文件夹删除，不需要手动创建\n<h3>' \
                  '<h3>3.使用tar -zxvf *.tgz解压文件\n<h3>' \
                  '<h3>4.cd到feature-engineering文件夹下\n<h3>' \
                  '<h3>5.执行./service.sh restart。\n<h3>' \
                  '<h3>6.使用ps -ef | grep chara命令检查进程。<h3>' \
                  '<p style="color:red;">如有报错，请联系开发者。部署完了请联系开发者测试，谢谢。</p>'

        msg = f"<html><body>{content}</body></html>"
        message.attach(MIMEText(msg, 'html', 'utf-8'))
        basePath = os.getcwd()
        tgzFile = ''
        files = os.listdir(os.getcwd())
        for fi in files:
            if fi.__contains__(".tgz"):
                tgzFile = fi
                break

        attFile = basePath + os.sep + tgzFile
        # 构造附件1，传送上一级目录下的 .tgz 文件
        att1 = MIMEText(open(attFile, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att1["Content-Disposition"] = f'attachment; filename="{tgzFile}"'
        message.attach(att1)

        self.sendEmail(message, receivers)

    def getWeChatAccessToken(self):
        url = "gettoken?corpid=ww328b794272e625c5&corpsecret=EbtUl0jGajxl9Y2CxGCN6tdM5lnWZNj67If1Hfkzz8U"
        urlPath = f"{self.weChatBaseUrl}{url}"
        print(urlPath)
        r = requests.get(urlPath)
        res = json.loads(r.text)
        print(res)
        return res['access_token']

    def uploadFile(self, url, path, fileName):
        self.access_token = self.getWeChatAccessToken()
        urlPath = f"{self.weChatBaseUrl}{url}" + self.access_token
        response = requests.post(urlPath, files={"file": (fileName,  # 文件名
                                                          open(path, 'rb'),  # 文件流
                                                          'tgz',  # 请求头Content-Type字段对应的值
                                                          {'Expires': '0'})})
        res = json.loads(response.text)
        print("res is " + str(res))
        return res['media_id']

    def postFilesToWeChat(self):
        basePath = os.path.abspath('..')
        tgzFile = ''
        files = os.listdir(basePath)
        for fi in files:
            if fi.__contains__(".tgz"):
                tgzFile = fi
                break
        attFile = basePath + os.sep + tgzFile
        print("attFile is " + attFile)
        url = "media/upload?type=file&access_token="
        mediaId1 = self.uploadFile(url, attFile, tgzFile)
        param = "{\n" \
                + "   \"touser\" : \"TangQiPeng|15297324668\",\n" \
                + "   \"toparty\" : \"\",\n" \
                + "   \"totag\" : \"\",\n" \
                + "   \"msgtype\" : \"file\",\n" \
                + "   \"agentid\" : 1000011,\n" \
                + "   \"file\" : {\n" \
                + "        \"media_id\": \"" + mediaId1 + "\"\n" \
                + "   },\n" \
                + "   \"safe\":0,\n" \
                + "   \"enable_duplicate_check\": 0,\n" \
                + "   \"duplicate_check_interval\": 1800\n" \
                + "}"
        url = "message/send?access_token="
        urlPath = f"{self.weChatBaseUrl}{url}" + self.access_token
        response = requests.post(url=urlPath, data=param)
        res = json.loads(response.text)
        print(res)

    def sendMessageToWeChat(self, tag):
        self.access_token = self.getWeChatAccessToken()
        url = "message/send?access_token="
        urlPath = f"{self.weChatBaseUrl}{url}" + self.access_token
        if tag == 'D':
            envT = '东航测试环境'
        else:
            envT = '东航生产环境'
        content = f'文件已通过邮件发送，请更新新包到{envT}正式环境。\n' \
                  '更新步骤如下：\n' \
                  '1.上传tgz包到指定路径\n' \
                  '2.将feature-engineering文件夹删除，不需要手动创建\n' \
                  '3.使用tar -zxvf *.tgz解压文件\n' \
                  '4.cd到feature-engineering文件夹下\n' \
                  '5.执行./service.sh restart。\n' \
                  '6.使用ps -ef | grep chara命令检查进程。' \
                  '如有报错，请联系开发者，谢谢。'
        contentStr = content.encode(encoding='utf-8').decode('latin-1')
        param = "{\n" \
                + "  \"touser\": \"TangQiPeng|15297324668\",\n" \
                + "  \"toparty\": \"\",\n" \
                + "  \"totag\": \"\",\n" \
                + "  \"msgtype\": \"text\",\n" \
                + "  \"agentid\": 1000011,\n" \
                + "  \"text\": {\n" \
                + "    \"content\": \"" + contentStr + "\"\n" \
                + "  },\n" \
                + "  \"safe\": 0,\n" \
                + "  \"enable_id_trans\": 0,\n" \
                + "  \"enable_duplicate_check\": 0\n" \
                + "}"
        print(param)
        response = requests.post(url=urlPath, data=param)
        res = json.loads(response.text)
        print(res)


if __name__ == '__main__':
    if int(sys.argv[1:][0]) == 1 and sys.argv[1:][1] != 'T':
        print(sys.argv[1:][1])
        notify = Notify()
        notify.notifyReleaseEtl(sys.argv[1:][1])
        # 文件超过20M不可以使用微信上传
#         notify.postFilesToWeChat()
#         time.sleep(1)
        notify.sendMessageToWeChat(sys.argv[1:][1])
