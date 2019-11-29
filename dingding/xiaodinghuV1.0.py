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
'''核心指标及每日通报
提醒的模板---每天早上8:00定时发送前一天的数据
比如，今天是2019年9月9日星期一，发送的是9月8日星期日的数据，模板如下：
2019年9月8日（星期日）
1、全网数据
①订单金额B，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③新增用户数C，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
④下单率d，环比（+/-x%）,对比（+/-x%）；
⑤撤单率e，环比（+/-x%）,对比（+/-x%）；
⑥客单价f，环比（+/-y元）,对比（+/-y元）
2、新增用户数据
2.1分端新增用户统计
①官网新增用户数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②客户端新增用户数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③APP新增用户数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
④M站新增用户数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
⑤其他平台新增用户数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）
2.2新用户加款率
①官网新用户加款率x%，环比（+/-x%）,对比（+/-x%）；
②客户端新用户加款率x%，环比（+/-x%）,对比（+/-x%）；
③APP新用户加款率x%，环比（+/-x%）,对比（+/-x%）；
④M站新用户加款率x%，环比（+/-x%）,对比（+/-x%）；
⑤其他平台新用户加款率x%，环比（+/-x%）,对比（+/-x%）
2.3新用户下单率
①官网新用户下单率x%，环比（+/-x%）,对比（+/-x%）；
②客户端新用户下单率x%，环比（+/-x%）,对比（+/-x%）；
③APP新用户下单率x%，环比（+/-x%）,对比（+/-x%）；
④M站新用户下单率x%，环比（+/-x%）,对比（+/-x%）；
⑤其他平台新用户下单率x%，环比（+/-x%）,对比（+/-x%）；
2.4新用户撤单率x%，环比（+/-x%）,对比（+/-x%）；
2.5新用户客单价x元，环比（+/-y元）,对比（+/-y元）
3、老用户数据
3.1老用户活跃数据
①官网老用户活跃数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②客户端老用户活跃数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③APP老用户活跃数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
④M站老用户活跃数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
⑤其他平台老用户活跃数A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）
3.2老用户加款率
①官网老用户加款率x%，环比（+/-x%）,对比（+/-x%）；
②客户端老用户加款率x%，环比（+/-x%）,对比（+/-x%）；
③APP老用户加款率x%，环比（+/-x%）,对比（+/-x%）；
④M站老用户加款率x%，环比（+/-x%）,对比（+/-x%）；
⑤其他平台老用户加款率x%，环比（+/-x%）,对比（+/-x%）
3.3老用户下单率
①官网老用户下单率x%，环比（+/-x%）,对比（+/-x%）；
②客户端老用户下单率x%，环比（+/-x%）,对比（+/-x%）；
③APP老用户下单率x%，环比（+/-x%）,对比（+/-x%）；
④M站老用户下单率x%，环比（+/-x%）,对比（+/-x%）；
⑤其他平台老用户下单率x%，环比（+/-x%）,对比（+/-x%）
3.4老用户撤单率x%，环比（+/-x%）,对比（+/-x%）
3.5老用户客单价y元，，环比（+/-y元）,对比（+/-y元）
4、TOP10游戏情况
4.1穿越火线
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
4.2英雄联盟
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
4.3绝地求生
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
4.4王者荣耀
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
4.5逆战
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
4.6穿越火线：枪战王者
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
4.7火影忍者手游
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
4.8QQ阅读
①订单金额A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
②订单量A，环比（+/-x%，+/-具体的数值），对比（+/-x%，+/-具体的数值）；
③客单价y元，，环比（+/-y元）,对比（+/-y元）；
④撤单率x%，环比（+/-x%）,对比（+/-x%）
备注：
1、环比的定义：当日某个指标数据与上个星期同一星期天数的比值
比如：2019年9月8日的数据，环比的是2019年9月1日的数据
具体的计算逻辑是B/A-1，以百分比显示，保留两位小数
比如：2019年9月8日的订单金额2265991.36，2019年9月1日的订单金额2318257.49，
环比的计算结果=2265991.36/2318257.49-1=-2.25%，差值为-56621
2、对比的定义：当日某个指标数据与当日前一天的数值比较
比如：2019年9月8日的数据，对比的是2019年9月7日的数据
具体的计算逻辑是B/A-1，以百分比显示，保留两位小数
比如：2019年9月8日的订单金额2265991.36，2019年9月7日的订单金额2547981.95，
环比的计算结果=2265991.36/2547981.95-1=-11.07%，差值为-281991，差值保留整数位数
显示结果为：
1订单金额2265991.36，环比（-2.25%，-56621），对比（-11.07%，-281991）
这个计算逻辑针对订单金额、订单数量、新增用户数等指标，客单价、下单率、撤单率除外
3、客单价
客单价的计算逻辑是差值，以差值结果显示
比如：2019年9月8日的全网客单价5.51，2019年9月1日的客单价5.63，2019年9月7
日的客单价5.54，则环比的计算逻辑是5.51-5.63=-0.12元，对比的计算逻辑是5.51-5.54=-0.03
元，所以结果展示如下：
客单价5.51元，，环比（-0.12元）,对比（-0.03元）；
4、下单率和撤单率
与客单价的计算方法一致，显示的结果是差值，差值是百分比
比如：2019年9月8日的全网下单率59.61%，2019年9月1日的下单率61.81%，2019年
9月7日的下单率58.2%，则环比的计算逻辑是59.61%-61.81%=-2.20%，对比的计算逻辑是
59.61%-58.2%=+1.41%元，所以结果展示如下：
下单率59.61%，环比（-2.20%）,对比（+1.41%）；
5、数值格式
订单金额、订单数量、注册用户数、活跃用户数，以整数显示，差值仍以整数显示，四舍五
入
所有的百分比保留两位小数
客单价保留两位小数，差值比较也保留两位小数，客单价写上单位“元”，其他的数值不用
写单位
6、显示格式
1除撤单率这一指标外，其他数据均是+用绿色显示，-用红色显示，对应的数值也以同样
的颜色显示；
2撤单率，+用红色显示，-用绿色显示，对应的数据也以同样的颜色显
'''



#------------------------参数变量区----------------------------
cf = configparser.ConfigParser()
# cf.read("E:/zuhaowan working/config.ini")
cf.read("/usr/model/zhw_product/config/config.ini")
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



#-------------------函数区间--------------------
def jugde_data(data):
    if (data >= 0 ):
        new_data = '增长' + str(format(data * 100 ,'.2f')) + '%'+'[鲜花]'
    elif (data < 0):
        new_data = '下降' + str(format(data * 100, '.2f')) + '%'+'[残花]'
    else:
        new_data = 'Error'
    return new_data

def jugde_game_data(data):
    if (data >= 0 ):
        new_data = '增长' + str(format(data * 100 ,'.2f')) + '%'+'[残花]'
    elif (data < 0):
        new_data = '下降' + str(format(data * 100, '.2f')) + '%'+'[鲜花]'
    else:
        new_data = 'Error'
    return new_data

def jugde_amount_data(data):
    if (data >= 0 ):
        new_data = '增量' + str(data)
    elif (data < 0 ):
        new_data = '差值' + str(data)
    else:
        new_data = 'Error'
    return new_data





def holiday_judge(date):
  # 是否节假日
  vop_url_request = urllib.request.Request(server_url + date)
  vop_response = urllib.request.urlopen(vop_url_request)
  vop_data = json.loads(vop_response.read())
  holiday = int(vop_data[date])
  time.sleep(1)

  if holiday == 0:
      data = '工作日'
  elif holiday == 1:
      data = '节假日'
  elif holiday == 2:
      data = '节假日'
  else:
      data = 'Error'
  return data


#np.random.randint(2, high=9, size=None, dtype='l')








#list=[1,2,3,4]


#array = np.random.randn(5,4)
#pd.DataFrame(array)
#计算订单金额B指标，t为今日，字段t-1日订单金额，t-2日订单金额，t-8日订单金额，(t-2)/(t-1)-1为对比，com bit 对比    (t-8)/(t-1)-1为环比 per bit环比
#A是昨天，B是前天，C是上周同一天查出来的量
sql = '''SELECT  {},'订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT  sum(pm) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT  sum(pm) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT  sum(pm) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3'''.format(now,day_now,day_now,day_last_db,day_last_hb)
#订单量
sql1 = '''SELECT  {},'订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B FROM
(SELECT  count(*) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT  count(*) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT  count(*) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3'''.format(now,day_now,day_now,day_last_db,day_last_hb)
#订单人数
sql3 = '''SELECT  {},'订单人数',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT  count(DISTINCT userid) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT  count(DISTINCT userid) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT  count(DISTINCT userid) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3'''.format(now,day_now,day_now,day_last_db,day_last_hb)
#新增人数
sql2 = '''SELECT  {},'注册人数',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT  count(jkx_userid) as A
FROM zhw_user WHERE DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t1,
(SELECT  count(jkx_userid) as B
FROM zhw_user WHERE DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t2,
(SELECT  count(jkx_userid) as C
FROM zhw_user WHERE DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t3'''.format(now,day_now,day_now,day_last_db,day_last_hb)
#QQ阅读
QQsql = '''SELECT {},'QQ阅读订单金额：',{},A FROM
(SELECT  sum(pm) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
AND gameid=639
)t1'''.format(now,day_now,day_now)

QQdata=pd.read_sql(QQsql,con=db)
#QQ阅读

#订单金额
data = pd.read_sql(sql,con=db)
data.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']
#订单量
data1 = pd.read_sql(sql1,con=db)
data1.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']
#注册人数
data2 = pd.read_sql(sql2,con=db)
data2.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']
#订单人数
data3 = pd.read_sql(sql3,con=db)
data3.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']
#客单价
percustomert1=str(format(float(data['t-1'][0])/float(data1['t-1'][0]) ,'.2f'))
percustomert2=str(format(float(data['t-2'][0])/float(data1['t-2'][0]) ,'.2f'))
percustomert8=str(format(float(data['t-8'][0])/float(data1['t-8'][0]) ,'.2f'))




#追加一行数据
data.append(data2)


#订单金额
data['com_bit_new'] = data['com_bit'].apply(lambda x: '对比'+jugde_data(x))
data['A-C_new']=data['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
data['A-B_new']=data['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
data['per_bit_new'] = data['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# data['holiday_judge_day_last'] = data['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
# #判断是否为节假日
# data['holiday_judge_day_now'] = data['day'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
# #判断今日是否为节假日

#订单量
data1['com_bit_new'] = data1['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
data1['per_bit_new'] = data1['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
data1['A-C_new']=data1['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))#环比差值
data1['A-B_new']=data1['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))#对比差值
# data1['holiday_judge_day_last'] = data1['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))#判断前天是否为节假日
# data1['holiday_judge_day_now'] = data1['day'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))#判断昨天是否为节假日
#注册人数
data2['com_bit_new'] = data2['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
data2['per_bit_new'] = data2['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
data2['A-C_new']=data2['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))#环比差值
data2['A-B_new']=data2['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))#对比差值
# data2['holiday_judge_day_last'] = data2['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
# data2['holiday_judge_day_now'] = data2['day'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#订单人数
data3['com_bit_new'] = data3['com_bit'].apply(lambda x: '对比'+jugde_data(x))
data3['per_bit_new'] = data3['per_bit'].apply(lambda x: '环比'+jugde_data(x))
data3['A-C_new']=data3['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
data3['A-B_new']=data3['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
# data3['holiday_judge_day_last'] = data3['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
# data3['holiday_judge_day_now'] = data3['day'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#客单价
percustomert2new=jugde_amount_data(float(format(float(percustomert1)-float(percustomert2) ,'.2f')))
percustomert8new=jugde_amount_data(float(format(float(percustomert1)-float(percustomert8) ,'.2f')))
#官网注册人数
official_website_sql = '''SELECT  {},'官网注册人数',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT COUNT(jkx_userid) as A
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx=1
)t1,
(SELECT COUNT(jkx_userid) as B
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx=1
)t2,
(SELECT COUNT(jkx_userid) as C
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx=1
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
official_website_people=pd.read_sql(official_website_sql,con=db)
official_website_people.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

official_website_people['com_bit_new'] = official_website_people['com_bit'].apply(lambda x: '对比'+jugde_data(x))
official_website_people['A-C_new']=official_website_people['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
official_website_people['A-B_new']=official_website_people['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
official_website_people['per_bit_new'] = official_website_people['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# official_website_people['holiday_judge_day_last'] = official_website_people['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#APP注册人数
APP_sql = '''SELECT  {},'APP注册人数',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT COUNT(jkx_userid) as A
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(2,3)
)t1,
(SELECT COUNT(jkx_userid) as B
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(2,3)
)t2,
(SELECT COUNT(jkx_userid) as C
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(2,3)
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
APP_people=pd.read_sql(APP_sql,con=db)
APP_people.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

APP_people['com_bit_new'] = APP_people['com_bit'].apply(lambda x: '对比'+jugde_data(x))
APP_people['A-C_new']=APP_people['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
APP_people['A-B_new']=APP_people['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
APP_people['per_bit_new'] = APP_people['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# APP_people['holiday_judge_day_last'] = APP_people['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#M站注册人数
M_sql = '''SELECT  {},'M站注册人数',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT COUNT(jkx_userid) as A
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(4,5)
)t1,
(SELECT COUNT(jkx_userid) as B
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(4,5)
)t2,
(SELECT COUNT(jkx_userid) as C
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(4,5)
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
M_people=pd.read_sql(M_sql,con=db)
M_people.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

M_people['com_bit_new'] = M_people['com_bit'].apply(lambda x: '对比'+jugde_data(x))
M_people['A-C_new']=M_people['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
M_people['A-B_new']=M_people['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
M_people['per_bit_new'] = M_people['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# M_people['holiday_judge_day_last'] = M_people['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#客户端
Client_sql = '''SELECT  {},'客户端注册人数',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT COUNT(jkx_userid) as A
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx=6
)t1,
(SELECT COUNT(jkx_userid) as B
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx=6
)t2,
(SELECT COUNT(jkx_userid) as C
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx=6
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Client_people=pd.read_sql(Client_sql,con=db)
Client_people.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Client_people['com_bit_new'] = Client_people['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Client_people['A-C_new']=Client_people['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Client_people['A-B_new']=Client_people['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Client_people['per_bit_new'] = Client_people['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Client_people['holiday_judge_day_last'] = Client_people['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#其他平台
Other_sql = '''SELECT  {},'其他渠道注册人数',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT COUNT(jkx_userid) as A
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(7,8,9,10,11)
)t1,
(SELECT COUNT(jkx_userid) as B
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(7,8,9,10,11)
)t2,
(SELECT COUNT(jkx_userid) as C
FROM zhw_user WHERE  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND jkx_lx in(7,8,9,10,11)
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Other_people=pd.read_sql(Other_sql,con=db)
Other_people.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Other_people['com_bit_new'] = Other_people['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Other_people['A-C_new']=Other_people['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Other_people['A-B_new']=Other_people['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Other_people['per_bit_new'] = Other_people['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Other_people['holiday_judge_day_last'] = Other_people['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#----------------------增加列
#orther=pd.DataFrame()
#orther['day']=Client_people['day']
#orther['name']=Client_people['name']
#orther['day_last']=Client_people['day_last']
#orther['t-1']=Client_people['t-1']
#orther['t-2']=Client_people['t-2']
#orther['t-8']=Client_people['t-8']
#orther['com_bit']=Client_people['com_bit']
#orther['A-C']=Client_people['A-C']
#orther['per_bit']=Client_people['per_bit']
#orther['A-B']=Client_people['A-B']
#增加列
# Top10游戏的情况
#CF
CF_sql = '''SELECT  {},'CF订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
CF_money=pd.read_sql(CF_sql,con=db)
CF_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

CF_money['com_bit_new'] = CF_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
CF_money['A-C_new']=CF_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
CF_money['A-B_new']=CF_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
CF_money['per_bit_new'] = CF_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# CF_money['holiday_judge_day_last'] = CF_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
CF_dd_sql = '''SELECT  {},'CF游戏订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
CF_dd=pd.read_sql(CF_dd_sql,con=db)
CF_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
CF_dd['A-C_new']=CF_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
CF_dd['A-B_new']=CF_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# CF_dd['holiday_judge_day_last'] = CF_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerCF1=str(format(float(CF_money['t-1'][0])/float(CF_dd['t-1'][0]) ,'.2f'))
percustomerCF2=str(format(float(CF_money['t-2'][0])/float(CF_dd['t-2'][0]) ,'.2f'))
percustomerCF8=str(format(float(CF_money['t-8'][0])/float(CF_dd['t-8'][0]) ,'.2f'))
percustomertCF2new=jugde_amount_data(float(format(float(percustomerCF1)-float(percustomerCF2) ,'.2f')))
percustomertCF8new=jugde_amount_data(float(format(float(percustomerCF1)-float(percustomerCF8) ,'.2f')))
#CF撤单率
CFc_sql = '''SELECT  {},'CF游戏撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=11
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
CFc_dd=pd.read_sql(CFc_sql,con=db)
CFc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
CFc_dd['t-8cl_new']=CFc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
CFc_dd['t-2cl_new']=CFc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# CFc_dd['holiday_judge_day_last'] = CFc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
CFcl= float(format(CFc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#英雄联盟
Yx_sql = '''SELECT  {},'英雄联盟订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Yx_money=pd.read_sql(Yx_sql,con=db)
Yx_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Yx_money['com_bit_new'] = Yx_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Yx_money['A-C_new']=Yx_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Yx_money['A-B_new']=Yx_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Yx_money['per_bit_new'] = Yx_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Yx_money['holiday_judge_day_last'] = Yx_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
Yx_dd_sql = '''SELECT  {},'英雄联盟订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Yx_dd=pd.read_sql(Yx_dd_sql,con=db)
Yx_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Yx_dd['com_bit_new'] = Yx_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Yx_dd['A-C_new']=Yx_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Yx_dd['A-B_new']=Yx_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Yx_dd['per_bit_new'] = Yx_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Yx_dd['holiday_judge_day_last'] = Yx_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#英雄联盟客单价
percustomerYx1=str(format(float(Yx_money['t-1'][0])/float(Yx_dd['t-1'][0]) ,'.2f'))
percustomerYx2=str(format(float(Yx_money['t-2'][0])/float(Yx_dd['t-2'][0]) ,'.2f'))
percustomerYx8=str(format(float(Yx_money['t-8'][0])/float(Yx_dd['t-8'][0]) ,'.2f'))
percustomertYx2new=jugde_amount_data(float(format(float(percustomerYx1)-float(percustomerYx2) ,'.2f')))
percustomertYx8new=jugde_amount_data(float(format(float(percustomerYx1)-float(percustomerYx8) ,'.2f')))
#英雄联盟撤单率
Yxc_sql = '''SELECT  {},'英雄联盟撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=17
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Yxc_dd=pd.read_sql(Yxc_sql,con=db)
Yxc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Yxc_dd['t-8cl_new']=Yxc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Yxc_dd['t-2cl_new']=Yxc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# Yxc_dd['holiday_judge_day_last'] = Yxc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Yxcl= float(format(Yxc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------




#绝地求生
Jd_sql = '''SELECT  {},'绝地求生订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Jd_money=pd.read_sql(Jd_sql,con=db)
Jd_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Jd_money['com_bit_new'] = Jd_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Jd_money['A-C_new']=Jd_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Jd_money['A-B_new']=Jd_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Jd_money['per_bit_new'] = Jd_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Jd_money['holiday_judge_day_last'] = Jd_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#绝地求生订单量
Jd_dd_sql = '''SELECT  {},'绝地求生订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Jd_dd=pd.read_sql(Jd_dd_sql,con=db)
Jd_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Jd_dd['com_bit_new'] = Jd_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Jd_dd['A-C_new']=Jd_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Jd_dd['A-B_new']=Jd_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Jd_dd['per_bit_new'] = Jd_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Jd_dd['holiday_judge_day_last'] = Jd_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerJd1=str(format(float(Jd_money['t-1'][0])/float(Jd_dd['t-1'][0]) ,'.2f'))
percustomerJd2=str(format(float(Jd_money['t-2'][0])/float(Jd_dd['t-2'][0]) ,'.2f'))
percustomerJd8=str(format(float(Jd_money['t-8'][0])/float(Jd_dd['t-8'][0]) ,'.2f'))
percustomertJd2new=jugde_amount_data(float(format(float(percustomerJd1)-float(percustomerJd2) ,'.2f')))
percustomertJd8new=jugde_amount_data(float(format(float(percustomerJd1)-float(percustomerJd8) ,'.2f')))
#CF撤单率
Jdc_sql = '''SELECT  {},'绝地求生撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=581
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Jdc_dd=pd.read_sql(Jdc_sql,con=db)
Jdc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Jdc_dd['t-8cl_new']=Jdc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Jdc_dd['t-2cl_new']=Jdc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# Jdc_dd['holiday_judge_day_last'] = Jdc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Jdcl= float(format(Jdc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#逆战订单金额
Nz_sql = '''SELECT  {},'逆战订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Nz_money=pd.read_sql(Nz_sql,con=db)
Nz_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Nz_money['com_bit_new'] = Nz_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Nz_money['A-C_new']=Nz_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Nz_money['A-B_new']=Nz_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Nz_money['per_bit_new'] = Nz_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Nz_money['holiday_judge_day_last'] = Nz_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#逆战订单量
Nz_dd_sql = '''SELECT  {},'逆战订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Nz_dd=pd.read_sql(Nz_dd_sql,con=db)
Nz_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Nz_dd['com_bit_new'] = Nz_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Nz_dd['A-C_new']=Nz_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Nz_dd['A-B_new']=Nz_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Nz_dd['per_bit_new'] = Nz_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Nz_dd['holiday_judge_day_last'] = Nz_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerNz1=str(format(float(Nz_money['t-1'][0])/float(Nz_dd['t-1'][0]) ,'.2f'))
percustomerNz2=str(format(float(Nz_money['t-2'][0])/float(Nz_dd['t-2'][0]) ,'.2f'))
percustomerNz8=str(format(float(Nz_money['t-8'][0])/float(Nz_dd['t-8'][0]) ,'.2f'))
percustomertNz2new=jugde_amount_data(float(format(float(percustomerNz1)-float(percustomerNz2) ,'.2f')))
percustomertNz8new=jugde_amount_data(float(format(float(percustomerNz1)-float(percustomerNz8) ,'.2f')))
#逆战撤单率
Nzc_sql = '''SELECT  {},'逆战撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=24
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Nzc_dd=pd.read_sql(Nzc_sql,con=db)
Nzc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Nzc_dd['t-8cl_new']=Nzc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Nzc_dd['t-2cl_new']=Nzc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# Nzc_dd['holiday_judge_day_last'] = Nzc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Nzcl= float(format(Nzc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------



#王者荣耀
Wz_sql = '''SELECT  {},'王者订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Wz_money=pd.read_sql(Wz_sql,con=db)
Wz_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Wz_money['com_bit_new'] = Wz_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Wz_money['A-C_new']=Wz_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Wz_money['A-B_new']=Wz_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Wz_money['per_bit_new'] = Wz_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Wz_money['holiday_judge_day_last'] = Wz_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
Wz_dd_sql = '''SELECT  {},'王者订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Wz_dd=pd.read_sql(Wz_dd_sql,con=db)
Wz_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Wz_dd['com_bit_new'] = Wz_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Wz_dd['A-C_new']=Wz_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Wz_dd['A-B_new']=Wz_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Wz_dd['per_bit_new'] = Wz_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Wz_dd['holiday_judge_day_last'] = Wz_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerWz1=str(format(float(Wz_money['t-1'][0])/float(Wz_dd['t-1'][0]) ,'.2f'))
percustomerWz2=str(format(float(Wz_money['t-2'][0])/float(Wz_dd['t-2'][0]) ,'.2f'))
percustomerWz8=str(format(float(Wz_money['t-8'][0])/float(Wz_dd['t-8'][0]) ,'.2f'))
percustomertWz2new=jugde_amount_data(float(format(float(percustomerWz1)-float(percustomerWz2) ,'.2f')))
percustomertWz8new=jugde_amount_data(float(format(float(percustomerWz1)-float(percustomerWz8) ,'.2f')))
#王者撤单率
Wzc_sql = '''SELECT  {},'王者撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=443
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Wzc_dd=pd.read_sql(Wzc_sql,con=db)
Wzc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Wzc_dd['t-8cl_new']=Wzc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Wzc_dd['t-2cl_new']=Wzc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# Wzc_dd['holiday_judge_day_last'] = Wzc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Wzcl= float(format(Wzc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------



#火影忍者概况
Hy_sql = '''SELECT  {},'火影订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Hy_money=pd.read_sql(Hy_sql,con=db)
Hy_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Hy_money['com_bit_new'] = Hy_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Hy_money['A-C_new']=Hy_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Hy_money['A-B_new']=Hy_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Hy_money['per_bit_new'] = Hy_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Hy_money['holiday_judge_day_last'] = Hy_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
Hy_dd_sql = '''SELECT  {},'火影订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Hy_dd=pd.read_sql(Hy_dd_sql,con=db)
Hy_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Hy_dd['com_bit_new'] = Hy_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Hy_dd['A-C_new']=Hy_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Hy_dd['A-B_new']=Hy_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Hy_dd['per_bit_new'] = Hy_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Hy_dd['holiday_judge_day_last'] = Hy_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerHy1=str(format(float(Hy_money['t-1'][0])/float(Hy_dd['t-1'][0]) ,'.2f'))
percustomerHy2=str(format(float(Hy_money['t-2'][0])/float(Hy_dd['t-2'][0]) ,'.2f'))
percustomerHy8=str(format(float(Hy_money['t-8'][0])/float(Hy_dd['t-8'][0]) ,'.2f'))
percustomertHy2new=jugde_amount_data(float(format(float(percustomerHy1)-float(percustomerHy2) ,'.2f')))
percustomertHy8new=jugde_amount_data(float(format(float(percustomerHy1)-float(percustomerHy8) ,'.2f')))
#CF撤单率
Hyc_sql = '''SELECT  {},'火影撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=560
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Hyc_dd=pd.read_sql(Hyc_sql,con=db)
Hyc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Hyc_dd['t-8cl_new']=Hyc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Hyc_dd['t-2cl_new']=Hyc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# Hyc_dd['holiday_judge_day_last'] = Hyc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Hycl= float(format(Hyc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#CF手游

CFs_sql = '''SELECT  {},'CF手游订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
CFs_money=pd.read_sql(CFs_sql,con=db)
CFs_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

CFs_money['com_bit_new'] = CFs_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
CFs_money['A-C_new']=CFs_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
CFs_money['A-B_new']=CFs_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
CFs_money['per_bit_new'] = CFs_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# CFs_money['holiday_judge_day_last'] =CFs_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
CFs_dd_sql = '''SELECT  {},'CF手游订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
CFs_dd=pd.read_sql(CFs_dd_sql,con=db)
CFs_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

CFs_dd['com_bit_new'] = CFs_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
CFs_dd['A-C_new']=CFs_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
CFs_dd['A-B_new']=CFs_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
CFs_dd['per_bit_new'] = CFs_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# CFs_dd['holiday_judge_day_last'] = CFs_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerCFs1=str(format(float(CFs_money['t-1'][0])/float(CFs_dd['t-1'][0]) ,'.2f'))
percustomerCFs2=str(format(float(CFs_money['t-2'][0])/float(CFs_dd['t-2'][0]) ,'.2f'))
percustomerCFs8=str(format(float(CFs_money['t-8'][0])/float(CFs_dd['t-8'][0]) ,'.2f'))
percustomertCFs2new=jugde_amount_data(float(format(float(percustomerCFs1)-float(percustomerCFs2) ,'.2f')))
percustomertCFs8new=jugde_amount_data(float(format(float(percustomerCFs1)-float(percustomerCFs8) ,'.2f')))
#CF撤单率
CFsc_sql = '''SELECT  {},'CF手游撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=446
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
CFsc_dd=pd.read_sql(CFsc_sql,con=db)
CFsc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
CFsc_dd['t-8cl_new']=CFsc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
CFsc_dd['t-2cl_new']=CFsc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# CFsc_dd['holiday_judge_day_last'] = CFsc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
CFscl= float(format(CFsc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#反恐精英ol

Fk_sql = '''SELECT  {},'反恐精英订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Fk_money=pd.read_sql(Fk_sql,con=db)
Fk_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Fk_money['com_bit_new'] = Fk_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Fk_money['A-C_new']=Fk_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Fk_money['A-B_new']=Fk_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Fk_money['per_bit_new'] = Fk_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Fk_money['holiday_judge_day_last'] =Fk_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
Fk_dd_sql = '''SELECT  {},'反恐精英订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Fk_dd=pd.read_sql(Fk_dd_sql,con=db)
Fk_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Fk_dd['com_bit_new'] = Fk_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Fk_dd['A-C_new']=Fk_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Fk_dd['A-B_new']=Fk_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Fk_dd['per_bit_new'] = Fk_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Fk_dd['holiday_judge_day_last'] = Fk_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerFk1=str(format(float(Fk_money['t-1'][0])/float(Fk_dd['t-1'][0]) ,'.2f'))
percustomerFk2=str(format(float(Fk_money['t-2'][0])/float(Fk_dd['t-2'][0]) ,'.2f'))
percustomerFk8=str(format(float(Fk_money['t-8'][0])/float(Fk_dd['t-8'][0]) ,'.2f'))
percustomertFk2new=jugde_amount_data(float(format(float(percustomerFk1)-float(percustomerFk2) ,'.2f')))
percustomertFk8new=jugde_amount_data(float(format(float(percustomerFk1)-float(percustomerFk8) ,'.2f')))
#CF撤单率
Fkc_sql = '''SELECT  {},'反恐精英撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=22
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Fkc_dd=pd.read_sql(Fkc_sql,con=db)
Fkc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Fkc_dd['t-8cl_new']=Fkc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Fkc_dd['t-2cl_new']=Fkc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# Fkc_dd['holiday_judge_day_last'] = Fkc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Fkcl= float(format(Fkc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#QQ飞车
QQF_sql = '''SELECT  {},'QQ飞车订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
QQF_money=pd.read_sql(QQF_sql,con=db)
QQF_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

QQF_money['com_bit_new'] = QQF_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
QQF_money['A-C_new']=QQF_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
QQF_money['A-B_new']=QQF_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
QQF_money['per_bit_new'] = QQF_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# QQF_money['holiday_judge_day_last'] =QQF_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
QQF_dd_sql = '''SELECT  {},'QQ飞车订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
QQF_dd=pd.read_sql(QQF_dd_sql,con=db)
QQF_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

QQF_dd['com_bit_new'] = QQF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
QQF_dd['A-C_new']=QQF_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
QQF_dd['A-B_new']=QQF_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
QQF_dd['per_bit_new'] = QQF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# QQF_dd['holiday_judge_day_last'] = QQF_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerQQF1=str(format(float(QQF_money['t-1'][0])/float(QQF_dd['t-1'][0]) ,'.2f'))
percustomerQQF2=str(format(float(QQF_money['t-2'][0])/float(QQF_dd['t-2'][0]) ,'.2f'))
percustomerQQF8=str(format(float(QQF_money['t-8'][0])/float(QQF_dd['t-8'][0]) ,'.2f'))
percustomertQQF2new=jugde_amount_data(float(format(float(percustomerQQF1)-float(percustomerQQF2) ,'.2f')))
percustomertQQF8new=jugde_amount_data(float(format(float(percustomerQQF1)-float(percustomerQQF8) ,'.2f')))
#CF撤单率
QQFc_sql = '''SELECT  {},'QQ飞车撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=25
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
QQFc_dd=pd.read_sql(QQFc_sql,con=db)
QQFc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
QQFc_dd['t-8cl_new']=QQFc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
QQFc_dd['t-2cl_new']=QQFc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# QQFc_dd['holiday_judge_day_last'] = QQFc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
QQFcl= float(format(QQFc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------

#侠盗列车手

Xd_sql = '''SELECT  {},'侠盗猎车手订单金额',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t1,
(SELECT sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t2,
(SELECT sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Xd_money=pd.read_sql(Xd_sql,con=db)
Xd_money.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Xd_money['com_bit_new'] = Xd_money['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Xd_money['A-C_new']=Xd_money['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Xd_money['A-B_new']=Xd_money['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Xd_money['per_bit_new'] = Xd_money['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Xd_money['holiday_judge_day_last'] =Xd_money['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#CF订单量
Xd_dd_sql = '''SELECT  {},'侠盗订单量',{},A,B,C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t1,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t2,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t3
'''.format(now,day_now,day_now,day_last_db,day_last_hb)
Xd_dd=pd.read_sql(Xd_dd_sql,con=db)
Xd_dd.columns = ['day','name','day_last','t-1','t-2','t-8','com_bit','A-C','per_bit','A-B']

Xd_dd['com_bit_new'] = Xd_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))
Xd_dd['A-C_new']=Xd_dd['A-C'].apply(lambda x:'环比'+jugde_amount_data(x))
Xd_dd['A-B_new']=Xd_dd['A-B'].apply(lambda x:'对比'+jugde_amount_data(x))
Xd_dd['per_bit_new'] = Xd_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))
# Xd_dd['holiday_judge_day_last'] = Xd_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))

percustomerXd1=str(format(float(Xd_money['t-1'][0])/float(Xd_dd['t-1'][0]) ,'.2f'))
percustomerXd2=str(format(float(Xd_money['t-2'][0])/float(Xd_dd['t-2'][0]) ,'.2f'))
percustomerXd8=str(format(float(Xd_money['t-8'][0])/float(Xd_dd['t-8'][0]) ,'.2f'))
percustomertXd2new=jugde_amount_data(float(format(float(percustomerXd1)-float(percustomerXd2) ,'.2f')))
percustomertXd8new=jugde_amount_data(float(format(float(percustomerXd1)-float(percustomerXd8) ,'.2f')))
#侠盗猎车手撤单率
Xdc_sql = '''SELECT  {},'侠盗撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid=441
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Xdc_dd=pd.read_sql(Xdc_sql,con=db)
Xdc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Xdc_dd['t-8cl_new']=Xdc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Xdc_dd['t-2cl_new']=Xdc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
# Xdc_dd['holiday_judge_day_last'] = Xdc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Xdcl= float(format(Xdc_dd['t-1cl'][0]*100   ,'.2f'))
#------------------------



week_day_weight = {
        1: '星期一',    #星期一
        2: '星期二',    #星期二
        3: '星期三',    #星期三
        4: '星期四',    #星期四
        5: '星期五',    #星期五
        6: '星期六',    #星期六
        7: '星期天',    #星期天
    }

#判断今日是周几是周几
data['Week_Day'] = data['day'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data['Week_Day_last'] = data['day_last'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data['day_new'] = data['day'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))
data['day_last_new'] = data['day_last'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))

#
data1['Week_Day'] = data1['day'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data1['Week_Day_last'] = data1['day_last'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data1['day_new'] = data1['day'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))
data1['day_last_new'] = data1['day_last'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))


data2['Week_Day'] = data2['day'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data2['Week_Day_last'] = data2['day_last'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data2['day_new'] = data2['day'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))
data2['day_last_new'] = data2['day_last'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))


data3['Week_Day'] = data3['day'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data3['Week_Day_last'] = data3['day_last'].apply(lambda x: week_day_weight[int(to_datetime(x,format="%Y%m%d").isoweekday())])
data3['day_new'] = data3['day'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))
data3['day_last_new'] = data3['day_last'].apply(lambda x: to_datetime(x,format="%Y%m%d").strftime('%Y-%m-%d'))


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
# tianqi = re.search(par,html).group(2)
# tianqi_split = re.split(r'( |。|墨迹|建议您)\s*',tianqi)
# a = tianqi_split[0] + tianqi_split[2] + tianqi_split[4] + tianqi_split[8]

# url = "https://api.seniverse.com/v3/weather/daily.json?key=SHkIfiQX_2r9wbggm&location=zhengzhou&language=zh-Hans&unit=c&start=0&days=5"
# html = urllib.request.urlopen(url).read().decode("utf-8")
# r = json.loads(html)
# test1 = '最高温度：{}度，最低温度:{}度\n\n'.format(r['results'][0]['daily'][0]['high'],r['results'][0]['daily'][0]['low'])

# url = "https://api.seniverse.com/v3/life/suggestion.json?key=SHkIfiQX_2r9wbggm&location=zhengzhou&language=zh-Hans"
# html = urllib.request.urlopen(url).read().decode("utf-8")
# r = json.loads(html)
# test2 = '洗车指数：{}，运动指数：{}\n\n'.format(r['results'][0]['suggestion']['car_washing']['brief'],r['results'][0]['suggestion']['sport']['brief'])

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
xiaoding.send_markdown(title='统计日报', text="# 租号玩核心指标统计【{}】\n\n".format(data['day_new'][0])
                            +"---\n\n"
                            + "---\n\n"
                            + "【昨日数据一览】\n\n"
                            +"> 下面统计的是昨日{}\n\n > 【{}】的数据：\n\n(环比的定义：当日某个指标数据与上个星期同一星期天数的比值)\n\n(对比：昨天比前天)\n\n".format(data['day_last_new'][0],data['Week_Day_last'][0])
                            +"### 1.全网数据\n\n"
                            +" ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(data['name'][0],data['t-1'][0],data['per_bit_new'][0],data['A-C_new'][0],data['com_bit_new'][0],data['A-B_new'][0])
                            +"②**{}**:昨日{}单，{},{}单,{},{}单\n\n".format(data1['name'][0],data1['t-1'][0],data1['per_bit_new'][0],data1['A-C_new'][0],data1['com_bit_new'][0],data1['A-B_new'][0])
                            +" ③{}:昨日{}人，{}，{}人,{},{}人\n\n".format(data2['name'][0],data2['t-1'][0],data2['per_bit_new'][0],data2['A-C_new'][0],data2['com_bit_new'][0],data2['A-B_new'][0])
                            +" ④**{}**:昨日{}人，{}，{}人,{},{}人\n\n".format(data3['name'][0],data3['t-1'][0],data3['per_bit_new'][0],data3['A-C_new'][0],data3['com_bit_new'][0],data3['A-B_new'][0])
                            + " ⑤客单价{}元，环比{}元,对比{}元\n\n".format(percustomert1, percustomert2new,percustomert8new)
                            +" ⑥{}{}元\n\n".format(QQdata['_col1'][0], QQdata['A'][0])
                            +"【以下是各端新增注册情况】\n\n"
                            + " ①**{}**:昨日{}人，{}，{}人，{}，{}人\n\n".format(official_website_people['name'][0], official_website_people['t-1'][0], official_website_people['per_bit_new'][0],official_website_people['A-C_new'][0], official_website_people['com_bit_new'][0],official_website_people['A-B_new'][0])
                                          + " ②**{}**:昨日{}人，{}，{}人，{}，{}人\n\n".format(
    APP_people['name'][0], APP_people['t-1'][0], APP_people['per_bit_new'][0],
    APP_people['A-C_new'][0], APP_people['com_bit_new'][0],
    APP_people['A-B_new'][0])
                                          + " ③**{}**:昨日{}人，{}，{}人，{}，{}人\n\n".format(
    M_people['name'][0], M_people['t-1'][0], M_people['per_bit_new'][0],
    M_people['A-C_new'][0], M_people['com_bit_new'][0],
    M_people['A-B_new'][0])
                                          + " ④**{}**:昨日{}人，{}，{}人，{}，{}人\n\n".format(
    Client_people['name'][0], Client_people['t-1'][0], Client_people['per_bit_new'][0],
    Client_people['A-C_new'][0], Client_people['com_bit_new'][0],
    Client_people['A-B_new'][0])
                                          + " ⑤**{}**:昨日{}人，{}，{}人，{}，{}人\n\n".format(
    Other_people['name'][0], Other_people['t-1'][0], Other_people['per_bit_new'][0],
    Other_people['A-C_new'][0], Other_people['com_bit_new'][0],
    Other_people['A-B_new'][0])
                                          +"【以下是CF情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    CF_money['name'][0], CF_money['t-1'][0], CF_money['per_bit_new'][0],
    CF_money['A-C_new'][0], CF_money['com_bit_new'][0],
    CF_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    CF_dd['name'][0], CF_dd['t-1'][0], CF_dd['per_bit_new'][0],
    CF_dd['A-C_new'][0], CF_dd['com_bit_new'][0],
    CF_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    CFc_dd['name'][0], CFcl, CFc_dd['t-8cl_new'][0],
    CFc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerCF1, percustomertCF2new,
                                                                              percustomertCF8new)
                                          +"【以下是英雄联盟的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    Yx_money['name'][0], Yx_money['t-1'][0], Yx_money['per_bit_new'][0],
    Yx_money['A-C_new'][0], Yx_money['com_bit_new'][0],
    Yx_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    Yx_dd['name'][0], Yx_dd['t-1'][0], Yx_dd['per_bit_new'][0],
    Yx_dd['A-C_new'][0], Yx_dd['com_bit_new'][0],
    Yx_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    Yxc_dd['name'][0], Yxcl, Yxc_dd['t-8cl_new'][0],
    Yxc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerYx1, percustomertYx2new,
                                                                              percustomertYx8new)
                                          +"【以下是绝地求生的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    Jd_money['name'][0], Jd_money['t-1'][0], Jd_money['per_bit_new'][0],
    Jd_money['A-C_new'][0], Jd_money['com_bit_new'][0],
    Jd_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    Jd_dd['name'][0], Jd_dd['t-1'][0], Jd_dd['per_bit_new'][0],
    Jd_dd['A-C_new'][0], Jd_dd['com_bit_new'][0],
    Jd_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    Jdc_dd['name'][0], Jdcl, Jdc_dd['t-8cl_new'][0],
    Jdc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerJd1, percustomertJd2new,
                                                                              percustomertJd8new)
                                          + "【以下是逆战的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    Nz_money['name'][0], Nz_money['t-1'][0], Nz_money['per_bit_new'][0],
    Nz_money['A-C_new'][0], Nz_money['com_bit_new'][0],
    Nz_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    Nz_dd['name'][0], Nz_dd['t-1'][0], Nz_dd['per_bit_new'][0],
    Nz_dd['A-C_new'][0], Nz_dd['com_bit_new'][0],
    Nz_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    Nzc_dd['name'][0], Nzcl, Nzc_dd['t-8cl_new'][0],
    Nzc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerNz1, percustomertNz2new,
                                                                              percustomertNz8new)
                                          + "【以下是王者的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    Wz_money['name'][0], Wz_money['t-1'][0], Wz_money['per_bit_new'][0],
    Wz_money['A-C_new'][0], Wz_money['com_bit_new'][0],
    Wz_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    Wz_dd['name'][0], Wz_dd['t-1'][0], Wz_dd['per_bit_new'][0],
    Wz_dd['A-C_new'][0], Wz_dd['com_bit_new'][0],
    Wz_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    Wzc_dd['name'][0], Wzcl, Wzc_dd['t-8cl_new'][0],
    Wzc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerWz1, percustomertWz2new,
                                                                              percustomertWz8new)
                                          + "【以下是穿越手游的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    CFs_money['name'][0], CFs_money['t-1'][0], CFs_money['per_bit_new'][0],
    CFs_money['A-C_new'][0], CFs_money['com_bit_new'][0],
    CFs_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    CFs_dd['name'][0], CFs_dd['t-1'][0], CFs_dd['per_bit_new'][0],
    CFs_dd['A-C_new'][0], CFs_dd['com_bit_new'][0],
    CFs_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    CFsc_dd['name'][0], CFscl, CFsc_dd['t-8cl_new'][0],
    CFsc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerCFs1, percustomertCFs2new,
                                                                              percustomertCFs8new)
                                          + "【以下是反恐的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    Fk_money['name'][0], Fk_money['t-1'][0], Fk_money['per_bit_new'][0],
    Fk_money['A-C_new'][0], Fk_money['com_bit_new'][0],
    Fk_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    Fk_dd['name'][0], Fk_dd['t-1'][0], Fk_dd['per_bit_new'][0],
    Fk_dd['A-C_new'][0], Fk_dd['com_bit_new'][0],
    Fk_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    Fkc_dd['name'][0], Fkcl, Fkc_dd['t-8cl_new'][0],
    Fkc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerFk1, percustomertFk2new,
                                                                              percustomertFk8new)
                                          + "【以下是QQ飞车的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    QQF_money['name'][0], QQF_money['t-1'][0], QQF_money['per_bit_new'][0],
    QQF_money['A-C_new'][0], QQF_money['com_bit_new'][0],
    QQF_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    QQF_dd['name'][0], QQF_dd['t-1'][0], QQF_dd['per_bit_new'][0],
    QQF_dd['A-C_new'][0], QQF_dd['com_bit_new'][0],
    QQF_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    QQFc_dd['name'][0], QQFcl, QQFc_dd['t-8cl_new'][0],
    QQFc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerQQF1, percustomertQQF2new,
                                                                              percustomertQQF8new)

                                          + "【以下是侠盗猎车手的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    Xd_money['name'][0], Xd_money['t-1'][0], Xd_money['per_bit_new'][0],
    Xd_money['A-C_new'][0], Xd_money['com_bit_new'][0],
    Xd_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    Xd_dd['name'][0], Xd_dd['t-1'][0], Xd_dd['per_bit_new'][0],
    Xd_dd['A-C_new'][0], Xd_dd['com_bit_new'][0],
    Xd_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    Xdc_dd['name'][0], Xdcl, Xdc_dd['t-8cl_new'][0],
    Xdc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerXd1, percustomertXd2new,
                                                                              percustomertXd8new)
                                          + "【以下是火影忍者的情况】\n\n"
                                          + " ①**{}**:昨日{}元，{}，{}元，{}，{}元\n\n".format(
    Hy_money['name'][0], Hy_money['t-1'][0], Hy_money['per_bit_new'][0],
    Hy_money['A-C_new'][0], Hy_money['com_bit_new'][0],
    Hy_money['A-B_new'][0])
                                          + " ②**{}**:昨日{}单，{}，{}单，{}，{}单\n\n".format(
    Hy_dd['name'][0], Hy_dd['t-1'][0], Hy_dd['per_bit_new'][0],
    Hy_dd['A-C_new'][0], Hy_dd['com_bit_new'][0],
    Hy_dd['A-B_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    Hyc_dd['name'][0], Hycl, Hyc_dd['t-8cl_new'][0],
    Hyc_dd['t-2cl_new'][0])
                                          + " ④客单价{}元，环比{}元,对比{}元\n\n".format(percustomerHy1, percustomertHy2new,
                                                                              percustomertHy8new)

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
                            +"> 目前处于数据测试阶段，数据可能存在一定误差，如有疑问可查询后台详情或咨询数据人员\n\n"
                            + "***************************\n\n"
                            +"【每日赏析】\n\n"
                            +"> ![screenshot]({})\n\n".format(image)
                            +"> {}\n\n".format(content[0])
                            +"&nbsp;\n\n"
                           ,is_at_all=False)


