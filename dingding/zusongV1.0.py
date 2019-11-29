#!/usr/bin/python
# -*- coding: utf-8 -*-
import time, datetime
import calendar
import pandas as pd
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
import numpy as np
import bs4
import warnings
import configparser
warnings.filterwarnings("ignore")




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


#租送
sql = '''select date(add_time) as 日期,
count(*)  as 订单数量,
count(distinct uid) as 订单人数,
sum(pm) as 订单金额 
from zhw_dingdan_rent_give 
where DATE_FORMAT(add_time,'%Y%m%d') = {}
group by date(add_time)'''.format(day_now)

zusong_people=pd.read_sql(sql,con=db)
zusong_people.columns = ['day','ds','dr','pm']
#畅享卡
#畅享卡自购红包产生的订单数量、红包的总使用金额、红包产生的订单金额数据
cang_sql = '''select date(add_time),count(c.id),
    sum(b.use_money),
       sum(c.pm)
  
  from zhw_hongbao a,
       zhw_hongbao_order b,
       zhw_dingdan c
 where a.id= b.hb_id
   and a.type=31 and a.issue=1010
   and b.order_id= c.id
   and DATE_FORMAT(c.add_time,'%Y%m%d') = {}
GROUP BY  date(add_time)'''.format(day_now)
cang=pd.read_sql(cang_sql,con=db)
cang.columns = ['day','ds','hpm','hspm']


#畅享卡折扣的订单数量、折扣补贴金额、折扣产生的订单金额数据*
cangz_sql = '''select 
   date(order_time),
	 count(id),
	 sum(card_money),
	 sum(money)
from zhw_share_card_order 
where DATE_FORMAT(order_time,'%Y%m%d') = {}
group by date(order_time)'''.format(day_now)
cangz=pd.read_sql(cangz_sql,con=db)
cangz.columns = ['day','ds','zkpt','zkpm']
#畅享卡下单送红包产生的订单数量、红包补贴金额、红包产生的订单金额数据*
cangh_sql = '''select date(add_time),count(c.id),
       sum(b.use_money),
       sum(c.pm)
  from zhw_hongbao a,
       zhw_hongbao_order b,
       zhw_dingdan c
 where a.id= b.hb_id
   and a.type=31 and a.money!=5
   and b.order_id= c.id
   and DATE_FORMAT(c.add_time,'%Y%m%d') = {}
GROUP BY  date(add_time)'''.format(day_now)
cangh=pd.read_sql(cangh_sql,con=db)
cangh.columns = ['day','ds','hbpt','hbpm']

headers = {'Content-Type': 'application/json;charset=utf-8'}
hearders = "User-Agent","Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36"
url = "https://tianqi.moji.com/weather/china/henan/zhengzhou"    ##要爬去天气预报的网址
par = '(<meta name="description" content=")(.*?)(">)'    ##正则匹配，匹配出网页内要的内容





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
webhook = cf.get("dingding_mac","webhook")
xiaoding = DingtalkChatbot(webhook)
xiaoding.send_markdown(title='统计日报', text="# 郑鹏伟租送数据同步【{}】\n\n".format(zusong_people['day'][0])
                            +"---\n\n"
                            + "---\n\n"
                            + "【昨日数据一览】\n\n"
                            +"### 1.租送数据\n\n"
                            +" ①**{}**:昨日{}单，{}人，{}元，\n\n".format(zusong_people['day'][0],zusong_people['ds'][0],zusong_people['dr'][0],zusong_people['pm'][0])
                            +"### 2.畅享卡数据\n\n"
                                         # + " ①**{}**:昨日自购红包产生的订单{}单，红包的总使用金额{}元，红包产生的订单金额{}元，\n\n".format(cang['day'][0],
                                          #                                       cang['ds'][0],
                                           #                                      cang['hpm'][0],
                                          #                                       cang['hspm'][0])
                                          + " ②**{}**:昨日折扣补贴{}单，折扣补贴金额{}元，折扣补贴产生的订单金额{}元，\n\n".format(cangz['day'][0],
                                                                                 cangz['ds'][0],
                                                                                 cangz['zkpt'][0],
                                                                                 cangz['zkpm'][0])
                                          + " ③**{}**:昨日折扣补贴{}单，红包补贴金额{}元，红包补贴产生的订单金额{}元，\n\n".format(cangh['day'][0],
                                                                                 cangh['ds'][0],
                                                                                 cangh['hbpt'][0],
                                                                                 cangh['hbpm'][0])

                            +"【每日赏析】\n\n"
                            +"> ![screenshot]({})\n\n".format(image)
                            +"> {}\n\n".format(content[0])
                            +"&nbsp;\n\n"
                           ,is_at_all=False)

