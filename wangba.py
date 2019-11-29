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


SELECT DATE_FORMAT(usertimer,'%Y%m%d') `day`, COUNT(DISTINCT userid) FROM zhw_dl 
WHERE DATE_FORMAT(usertimer,'%Y%m%d') = {} AND 
userid IN (SELECT jkx_userid FROM zhw_user WHERE id 
IN(SELECT user_id FROM `zhw_user_wangba` WHERE pid=7354519 
OR business_uid IN(13010550, 13010506, 21289853, 1842442, 2399952))) GROUP BY `day`
ORDER BY `day`

'''
'''

SELECT DATE_FORMAT(usertimer,'%Y%m%d') `day`, 
COUNT(DISTINCT userid) FROM zhw_dl WHERE usertimer 
BETWEEN '2019-07-01 00:00:00' AND '2019-08-10 00:00:00' 
AND userid 
IN (SELECT jkx_userid FROM zhw_user WHERE id IN(SELECT user_id FROM `zhw_user_wangba` WHERE pid=7354519 OR business_uid 
IN(13010550, 13010506, 21289853, 1842442, 2399952))) GROUP BY `day`
ORDER BY
`day`



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
wangba_sql = '''SELECT DATE_FORMAT(usertimer,'%Y%m%d') `day`, COUNT(DISTINCT userid) FROM zhw_dl 
WHERE DATE_FORMAT(usertimer,'%Y%m%d') = {} AND 
userid IN (SELECT jkx_userid FROM zhw_user WHERE id 
IN(SELECT user_id FROM `zhw_user_wangba` WHERE pid=7354519 
OR business_uid IN(13010550, 13010506, 21289853, 1842442, 2399952))) GROUP BY `day`
ORDER BY `day`'''.format(day_now)
wangba=pd.read_sql(wangba_sql,con=db)
wangba.columns = ['day','dl']







#array = np.random.randn(5,4)
#pd.DataFrame(array)
#计算订单金额B指标，t为今日，字段t-1日订单金额，t-2日订单金额，t-8日订单金额，(t-2)/(t-1)-1为对比，com bit 对比    (t-8)/(t-1)-1为环比 per bit环比
#A是昨天，B是前天，C是上周同一天查出来的量



#租送


'''







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
xiaoding.send_markdown(title='统计日报', text="# 网吧渠道数据同步【{}】\n\n".format(wangba['day'][0])
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
                            +"### 1.网吧活跃数据\n\n"
                            +" ①**{}**:昨日网吧活跃{}人\n\n".format(wangba['day'][0],wangba['dl'][0])
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
