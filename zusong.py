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
'''
/*租送每日数据*/
select date(add_time) as 日期,
count(*)  as 订单数量,
count(distinct uid) as 订单人数,
sum(pm) as 订单金额 
from zhw_dingdan_rent_give 
where add_time between "2019-08-14 00:00:00" 
and "2019-08-14 23:59:59"
group by date(add_time)

'''



#------------------------参数变量区----------------------------
db = pymysql.connect("am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com","yunying","Mo2O68koWe3UVjn3","zhwdb" )
#连接我们的数据库
server_url = "http://www.easybots.cn/api/holiday.php?d="
#判断日期是否为节假日的接口



now = (datetime.datetime.now()).strftime('%Y%m%d') #今日日期
day_now = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime('%Y%m%d') #t-1
day_last_db = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime('%Y%m%d') #t-2
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days = 8)).strftime('%Y%m%d') #t-8









#np.random.randint(2, high=9, size=None, dtype='l')








#array = np.random.randn(5,4)
#pd.DataFrame(array)
#计算订单金额B指标，t为今日，字段t-1日订单金额，t-2日订单金额，t-8日订单金额，(t-2)/(t-1)-1为对比，com bit 对比    (t-8)/(t-1)-1为环比 per bit环比
#A是昨天，B是前天，C是上周同一天查出来的量



#租送
sql = '''select date(add_time) as 日期,
count(*)  as 订单数量,
count(distinct uid) as 订单人数,
sum(pm) as 订单金额 
from zhw_dingdan_rent_give 
where DATE_FORMAT(add_time,'%Y%m%d') = {}
group by date(add_time)'''.format(day_now)
#QQ阅读
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

'''

/*畅享卡折扣的订单数量、折扣补贴金额、折扣产生的订单金额数据*/
select 
   date(order_time),
	 count(id),
	 sum(card_money),
	 sum(money)
from zhw_share_card_order 
where order_time  between "2019-08-09 00:00:00" 
and "2019-08-09 23:59:59"
group by date(order_time)
/*畅享卡下单送红包产生的订单数量、红包补贴金额、红包产生的订单金额数据*/
select count(c.id),
       sum(b.use_money),
       sum(c.pm),
  date(add_time)
  from zhw_hongbao a,
       zhw_hongbao_order b,
       zhw_dingdan c
 where a.id= b.hb_id
   and a.type=31 and a.money!=5
   and b.order_id= c.id
   and c.add_time between  "2019-08-09 00:00:00" 
and "2019-08-09 23:59:59"
GROUP BY  date(add_time)









'''








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

##提取需要爬取的内容
tianqi = re.search(par,html).group(2)
tianqi_split = re.split(r'( |。|墨迹|建议您)\s*',tianqi)
# a = tianqi_split[0] + tianqi_split[2] + tianqi_split[4] + tianqi_split[8]

url = "https://api.seniverse.com/v3/weather/daily.json?key=SHkIfiQX_2r9wbggm&location=zhengzhou&language=zh-Hans&unit=c&start=0&days=5"
html = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(html)
test1 = '最高温度：{}度，最低温度:{}度\n\n'.format(r['results'][0]['daily'][0]['high'],r['results'][0]['daily'][0]['low'])

url = "https://api.seniverse.com/v3/life/suggestion.json?key=SHkIfiQX_2r9wbggm&location=zhengzhou&language=zh-Hans"
html = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(html)
test2 = '洗车指数：{}，运动指数：{}\n\n'.format(r['results'][0]['suggestion']['car_washing']['brief'],r['results'][0]['suggestion']['sport']['brief'])

number = random.randint(1685,2542)
response = requests.get('http://wufazhuce.com/one/{}'.format(str(number)))
soup = bs4.BeautifulSoup(response.text,"html.parser")
image = soup.find_all('img')[1]['src']
content = list()
for meta in soup.select('meta'):
    if meta.get('name') == 'description':
        content.append(str(meta.get('content')))
print(content)








# 初始化机器人小丁
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=a44e4261fb1679f1bc85a214ee98a9a9a1678b6d2b70ceb010712b414cde5ea5'
xiaoding = DingtalkChatbot(webhook)
xiaoding.send_markdown(title='统计日报', text="# 郑鹏伟租送数据同步【{}】\n\n".format(zusong_people['day'][0])
                            +"---\n\n"
                          #  +"&ensp;&ensp;钉钉钉，大家好，我是小鱼！美好的一天从我的问候开始:各位早上好![微笑]\n\n"
                            # +"&ensp;&ensp;今天是{}【{}】<{}>\n\n".format(data['day_new'][0],data['Week_Day'][0],data['holiday_judge_day_now'][0])
                            + "---\n\n"
#                            +"【今日天气情况】\n\n"
#                           +'>'+tianqi_split[0]+"\n\n"
#                            +'>'+test1
#                            +'>'+tianqi_split[2]+"\n\n"
#                            +'>'+test2
#                            +'>'+'小鱼'+tianqi_split[8]+tianqi_split[9]+"："+tianqi_split[10]+"[耶]"+"\n\n"
#                            + "***************************\n\n"
                            + "【昨日数据一览】\n\n"
                            #+"> 下面统计的是昨日{}\n\n > 【{}】<{}>的数据：\n\n(环比的定义：当日某个指标数据与上个星期同一星期天数的比值)\n\n(对比：昨天比前天)\n\n".format(zusong_people['day'][0])
                            +"### 1.租送数据\n\n"
                            +" ①**{}**:昨日{}单，{}人，{}元，\n\n".format(zusong_people['day'][0],zusong_people['ds'][0],zusong_people['dr'][0],zusong_people['pm'][0])
                            +"### 2.畅享卡数据\n\n"
                                          + " ①**{}**:昨日自购红包产生的订单{}单，红包的总使用金额{}元，红包产生的订单金额{}元，\n\n".format(cang['day'][0],
                                                                                 cang['ds'][0],
                                                                                 cang['hpm'][0],
                                                                                 cang['hspm'][0])
                                          + " ②**{}**:昨日折扣补贴{}单，折扣补贴金额{}元，折扣补贴产生的订单金额{}元，\n\n".format(cangz['day'][0],
                                                                                 cangz['ds'][0],
                                                                                 cangz['zkpt'][0],
                                                                                 cangz['zkpm'][0])
                                          + " ③**{}**:昨日折扣补贴{}单，红包补贴金额{}元，红包补贴产生的订单金额{}元，\n\n".format(cangh['day'][0],
                                                                                 cangh['ds'][0],
                                                                                 cangh['hbpt'][0],
                                                                                 cangh['hbpm'][0])
                                          #      +"④{}：昨日客单价{}".format(data['name'][0]/data1['name'][0])
                        #     +"### 2.新增用户数据\n\n"
                        #    +"#### 2.1 分端新增用户统计\n\n"
                        #    +"#### 2.2 新用户加款率\n\n"
                        #    +"#### 2.4 新用户撤单率\n\n"
                        #   +"### 3.老用户数据\n\n"
                        #    +"#### 3.1 老用户活跃数据\n\n"
                       #     +"#### 3.2 老用户加款率\n\n"
                       #     +"#### 3.3 老用户下单率\n\n"
                       #     +"#### 3.4 老用户撤单率\n\n"
                       #     +"#### 3.5 老用户客单价\n\n"
                        #    +"#### (指标未完待续)\n\n"
                          #  +"> 目前处于数据测试阶段，数据可能存在一定误差，如有疑问可查询后台详情或咨询数据人员\n\n"
                         #   + "***************************\n\n"
                            +"【每日赏析】\n\n"
                            +"> ![screenshot]({})\n\n".format(image)
                            +"> {}\n\n".format(content[0])
                            +"&nbsp;\n\n"
                           ,is_at_all=False)

#自动图片还未测试 +
# url = ' http://www.bing.com/HPImageArchive.aspx?format=js&idx=' + '20190922' + '&n=1&nc=1469612460690&pid=hp&video=1'
# html = urllib.request.urlopen(url).read().decode('utf-8')
#
# photoData = json.loads(html)
# # 这是壁纸的 url
# photoUrl = 'https://cn.bing.com' + photoData['images'][0]['url']
# photoReason = photoData['images'][0]['copyright']
# photoReason = photoReason.split(' ')[0]
# photo = urllib.request.urlopen(photoUrl).read()
