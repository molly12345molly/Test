#!/usr/bin/python
# -*- coding: utf-8 -*-
import time, datetime
import calendar
import numpy as np
from pandas import to_datetime
import pymysql
import yagmail
from sqlalchemy import create_engine
from dingtalkchatbot.chatbot import DingtalkChatbot
import json
import urllib
import time
import urllib.response
import urllib.request
import re
import urllib
import random
import requests
import pandas as pd
import bs4
import warnings
import configparser
warnings.filterwarnings("ignore")

'''
 select   
        a.huserid,
        count(a.id) as 订单数量,
        sum(a.pm) as 订单金额,
        count(distinct b.hb_id) as 红包使用数量,
        sum(b.use_money) as 红包使用金额        
   from zhw_dingdan a 
   left join zhw_hongbao_order  b on a.id=b.order_id
   where a.add_time between "2019-10-17 00:00:00" and "2019-10-17 23:59:59" 
   group by a.huserid having count(a.id)>=20 and count(distinct b.hb_id)>=10 and count(distinct b.hb_id)/count(a.id)>=0.65

'''
'''
 select   
        a.huserid,
        count(a.id) as 订单数量,
        sum(a.pm) as 订单金额,
        count(distinct b.hb_id) as 红包使用数量,
        sum(b.use_money) as 红包使用金额        
   from zhw_dingdan a 
   left join zhw_hongbao_order  b on a.id=b.order_id
   where a.add_time between "2019-10-17 00:00:00" and "2019-10-17 23:59:59" 
   group by a.huserid having count(a.id)>=20 and count(distinct b.hb_id)>=10 and count(distinct b.hb_id)/count(a.id)>=0.65



'''



#------------------------参数变量区----------------------------
cf = configparser.ConfigParser()
cf.read("E:/zuhaowan working/config.ini")
# cf.read("/usr/model/zhw_product/config/config.ini")
host = cf.get("Mysql-Database-yunying","host")
user = cf.get("Mysql-Database-yunying","user")
password = cf.get("Mysql-Database-yunying","password")
DB = cf.get("Mysql-Database-yunying","DB")
db = pymysql.connect(host,user,password,DB)
#连接我们的数据库
server_url = "http://www.easybots.cn/api/holiday.php?d="
#判断日期是否为节假日的接口


now = (datetime.datetime.now()).strftime('%Y%m%d') #今日日期
day_now = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime('%Y%m%d') #t-1
day_last_db = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime('%Y%m%d') #t-2
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days = 8)).strftime('%Y%m%d') #t-8


wangba_sql = ''' select   
        a.huserid,
        count(a.id) as 订单数量,
        sum(a.pm) as 订单金额,
        count(distinct b.hb_id) as 红包使用数量,
        sum(b.use_money) as 红包使用金额        
   from zhw_dingdan a 
   left join zhw_hongbao_order  b on a.id=b.order_id
   where DATE_FORMAT(a.add_time,'%Y%m%d') = {}
   group by a.huserid having count(a.id)>=20 and count(distinct b.hb_id)>=10 and count(distinct b.hb_id)/count(a.id)>=0.65'''.format(day_now)
wangba=pd.read_sql(wangba_sql,con=db)
wangba.columns = ['huserid','ds','dj','hs','hsj']



headers = {'Content-Type': 'application/json;charset=utf-8'}
hearders = "User-Agent","Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36"
url = "https://tianqi.moji.com/weather/china/henan/zhengzhou"    ##要爬去天气预报的网址
par = '(<meta name="description" content=")(.*?)(">)'    ##正则匹配，匹配出网页内要的内容



#自己写的爬虫部分
#response = urllib.request.urlopen('https://blog.csdn.net/weixin_43499626')
#print(response.read().decode('utf-8'))
#读取一个连接
#爬虫部分
##创建opener对象并设置为全局对象
opener = urllib.request.build_opener()
opener.addheaders = [hearders]
urllib.request.install_opener(opener)

##获取网页
html = urllib.request.urlopen(url).read().decode("utf-8")


number = random.randint(1685,2542)
response = requests.get('http://wufazhuce.com/one/{}'.format(str(number)))
soup = bs4.BeautifulSoup(response.text,"html.parser")
image = soup.find_all('img')[1]['src']
content = []
for meta in soup.select('meta'):
    if meta.get('name') == 'description':
        content.append(str(meta.get('content')))
print(content)











# 初始化机器人小丁
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=b7e024f3710f5889a06f93715af666c81e3d07aaa4a5a77c7c520b4c640ba4a9'
xiaoding = DingtalkChatbot(webhook)
xiaoding.send_markdown(title='统计日报', text="# 刷红包数据同步【{}】\n\n".format(now)
                            +"---\n\n"
                            + "---\n\n"
                            + "【昨日数据一览】\n\n"
                            #+"> 下面统计的是昨日{}\n\n > 【{}】<{}>的数据：\n\n(环比的定义：当日某个指标数据与上个星期同一星期天数的比值)\n\n(对比：昨天比前天)\n\n".format(zusong_people['day'][0])
                            +"### 1.刷红包数据\n\n"
                            +" ①:昨日刷红包号主用户名{}\n\n".format(wangba['huserid'])
                            +"&nbsp;\n\n"
                           ,is_at_all=False)


