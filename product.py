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
'''撤单预警规则-产品运营
1、预警数值：后台计算出的异常时间值
2、预警时间：异常时间点，时间间隔4小时
3、信息展现：例：绝地求生撤单率异常，撤单率xx%，环比增长1%，对比昨天增长2%。
4、预警内容
（1）今日平台撤单异常预警
①全网撤单率
②客户端撤单率
③官网撤单率
④app撤单率
（2）今日新用户订单撤单率异常预警
①全网新用户撤单率
②客户端新用户撤单率
③官网新用户撤单率
④APP新用户撤单率
（3）各游戏撤单异常预警
穿越火线、英雄联盟、王者荣耀、绝地求生、火影忍者（手游）、逆战、穿越火线:枪战王者、反恐精
英OL、侠盗猎车手Online、生死狙击、和平精英、CSGO
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
#订单量

#全网撤单率
Qwc_sql = '''SELECT  {},'全网撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Qwc_dd=pd.read_sql(Qwc_sql,con=db)
Qwc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Qwc_dd['t-8cl_new']=Qwc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Qwc_dd['t-2cl_new']=Qwc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
Qwc_dd['holiday_judge_day_last'] = Qwc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Qwcl= float(format(Qwc_dd['t-1cl'][0]*100   ,'.2f'))


#官网撤单率
Gwc_sql = '''SELECT  {},'官网撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=1
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=1
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=1
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=1
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=1
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=1
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Gwc_dd=pd.read_sql(Gwc_sql,con=db)
Gwc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Gwc_dd['t-8cl_new']=Gwc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Gwc_dd['t-2cl_new']=Gwc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
Gwc_dd['holiday_judge_day_last'] = Gwc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Gwcl= float(format(Gwc_dd['t-1cl'][0]*100   ,'.2f'))




#APP撤单率

#APP撤单率
APPc_sql = '''SELECT  {},'APP撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from in(2,21,22)
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from in(2,21,22)
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from in(2,21,22)
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from in(2,21,22)
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from in(2,21,22)
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from in(2,21,22)
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
APPc_dd=pd.read_sql(APPc_sql,con=db)
APPc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
APPc_dd['t-8cl_new']=APPc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
APPc_dd['t-2cl_new']=APPc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
APPc_dd['holiday_judge_day_last'] = APPc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
APPcl= float(format(APPc_dd['t-1cl'][0]*100   ,'.2f'))

#客户端撤单率
Kuc_sql = '''SELECT  {},'客户端撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=13
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=13
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=13
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=13
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=13
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND add_from=13
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)
Kuc_dd=pd.read_sql(Kuc_sql,con=db)
Kuc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Kuc_dd['t-8cl_new']=Kuc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Kuc_dd['t-2cl_new']=Kuc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
Kuc_dd['holiday_judge_day_last'] = Kuc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Kucl= float(format(Kuc_dd['t-1cl'][0]*100   ,'.2f'))
#(2）今日新用户订单撤单率异常预警
#①全网新用户撤单率
#②客户端新用户撤单率
#③官网新用户撤单率
#④APP新用户撤单率
'''
SELECT count(*) as A
FROM zhw_dingdan d,zhw_user u
WHERE add_time BETWEEN "2019-09-20 00:00:00" 
and "2019-09-20 23:59:59"
AND d.userid=u.jkx_userid
AND  jkx_timer BETWEEN "2019-09-20 00:00:00" 
and "2019-09-20 23:59:59"
'''
#全网新用户撤单率
Qwxc_sql = '''SELECT  {},'全网撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_db,day_last_db,day_last_hb,day_last_hb,day_last_hb,day_last_hb)
Qwxc_dd=pd.read_sql(Qwxc_sql,con=db)
Qwxc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Qwxc_dd['t-8cl_new']=Qwxc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Qwxc_dd['t-2cl_new']=Qwxc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
Qwxc_dd['holiday_judge_day_last'] = Qwxc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Qwxcl= float(format(Qwxc_dd['t-1cl'][0]*100   ,'.2f'))


#官网新用户撤单率
Gwxc_sql = '''SELECT  {},'官网撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from=1
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
AND d.add_from=1
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from=1
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and u.jkx_lx=1
and zt=3
AND d.add_from=1
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from=1
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from=1
and zt=3
)t3c
'''.format(now,day_now,day_now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_db,day_last_db,day_last_hb,day_last_hb,day_last_hb,day_last_hb)
Gwxc_dd=pd.read_sql(Gwxc_sql,con=db)
Gwxc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Gwxc_dd['t-8cl_new']=Gwxc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Gwxc_dd['t-2cl_new']=Gwxc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
Gwxc_dd['holiday_judge_day_last'] = Gwxc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Gwxcl= float(format(Gwxc_dd['t-1cl'][0]*100   ,'.2f'))




#APP撤单率



#APP新用户撤单率
APPxc_sql = '''SELECT  {},'官网撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from in(2,21,22) 
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
AND d.add_from in(2,21,22) 
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from in(2,21,22) 
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
AND d.add_from in(2,21,22) 
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from in(2,21,22) 
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
AND d.add_from in(2,21,22) 
)t3c
'''.format(now,day_now,day_now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_db,day_last_db,day_last_hb,day_last_hb,day_last_hb,day_last_hb)
APPxc_dd=pd.read_sql(APPxc_sql,con=db)
APPxc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
APPxc_dd['t-8cl_new']=APPxc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
APPxc_dd['t-2cl_new']=APPxc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
APPxc_dd['holiday_judge_day_last'] = APPxc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
APPxcl= float(format(APPxc_dd['t-1cl'][0]*100   ,'.2f'))

#客户端新用户撤单率
Kuxc_sql = '''SELECT  {},'客户端撤单率',{},A,Ac,Ac/A,B,Bc,Ac/A-Bc/B,C,Cc,Ac/A-Cc/C,A/B-1 as com_bit,A-C,A/C-1 as per_bit,A-B  FROM
(SELECT count(*) as A
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from=13
)t1,
(SELECT count(*) as Ac
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
AND d.add_from=13
)t1c,
(SELECT count(*) as B
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from=13
)t2,
(SELECT count(*) as Bc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
AND d.add_from=13
)t2c,
(SELECT count(*) as C
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
AND d.add_from=13
)t3,
(SELECT count(*) as Cc
FROM zhw_dingdan d,zhw_user u WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
AND d.userid=u.jkx_userid
AND  DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
and zt=3
AND d.add_from=13
)t3c
'''.format(now,day_now,day_now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_db,day_last_db,day_last_hb,day_last_hb,day_last_hb,day_last_hb)
Kuxc_dd=pd.read_sql(Kuxc_sql,con=db)
Kuxc_dd.columns = ['day','name','day_last','t-1','t-1c','t-1cl','t-2','t-2c','t-2cl','t-8','t-8c','t-8cl','com_bit','A-C','per_bit','A-B']
# 今天，名字，前天，昨天的订单量，昨天的撤单量，前天的订单量，前天的撤单量，昨天减去前天的撤单率差值，8天前的订单量，8天前的撤单量，昨天的撤单率减去8天前的撤单率差值
#CF_dd['com_bit_new'] = CF_dd['com_bit'].apply(lambda x: '对比'+jugde_data(x))#对比
Kuxc_dd['t-8cl_new']=Kuxc_dd['t-8cl'].apply(lambda x:'环比'+jugde_game_data(x))#差值
Kuxc_dd['t-2cl_new']=Kuxc_dd['t-2cl'].apply(lambda x:'对比'+jugde_game_data(x))#差值
#CF_dd['per_bit_new'] = CF_dd['per_bit'].apply(lambda x: '环比'+jugde_data(x))#环比
Kuxc_dd['holiday_judge_day_last'] = Kuxc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Kuxcl= float(format(Kuxc_dd['t-1cl'][0]*100   ,'.2f'))



















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
CFc_dd['holiday_judge_day_last'] = CFc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
CFcl= float(format(CFc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
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
Yxc_dd['holiday_judge_day_last'] = Yxc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Yxcl= float(format(Yxc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------




#绝地求生撤单率
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
Jdc_dd['holiday_judge_day_last'] = Jdc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Jdcl= float(format(Jdc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
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
Nzc_dd['holiday_judge_day_last'] = Nzc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Nzcl= float(format(Nzc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------



#王者荣耀撤单率
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
Wzc_dd['holiday_judge_day_last'] = Wzc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Wzcl= float(format(Wzc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------



#火影忍者撤单率
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
Hyc_dd['holiday_judge_day_last'] = Hyc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Hycl= float(format(Hyc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#CF手游撤单率
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
CFsc_dd['holiday_judge_day_last'] = CFsc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
CFscl= float(format(CFsc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#反恐精英ol撤单率
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
Fkc_dd['holiday_judge_day_last'] = Fkc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
Fkcl= float(format(Fkc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------
#QQ飞车撤单率
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
QQFc_dd['holiday_judge_day_last'] = QQFc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
QQFcl= float(format(QQFc_dd['t-1cl'][0]*100   ,'.2f'))
#---------------------------

#侠盗列车手撤单率
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
Xdc_dd['holiday_judge_day_last'] = Xdc_dd['day_last'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
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

'''
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
xiaoding.send_markdown(title='统计日报', text="# 租号玩核心指标统计【{}】\n\n".format(now)
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
                           # +"> 下面统计的是昨日{}\n\n > 【{}】<{}>的数据：\n\n(环比的定义：当日某个指标数据与上个星期同一星期天数的比值)\n\n(对比：昨天比前天)\n\n".format(now)
                                          + " ①**{}**:昨日{}%，{}，{}\n\n".format(
    Qwc_dd['name'][0], Qwcl, Qwc_dd['t-8cl_new'][0],
    Qwc_dd['t-2cl_new'][0])
                                          + " ②**{}**:昨日{}%，{}，{}\n\n".format(
    Gwc_dd['name'][0], Gwcl, Gwc_dd['t-8cl_new'][0],
    Gwc_dd['t-2cl_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    APPc_dd['name'][0], APPcl, APPc_dd['t-8cl_new'][0],
    APPc_dd['t-2cl_new'][0])
                            +"### 1.全网新用户数据\n\n"
                                          + " ①**{}**:昨日{}%，{}，{}\n\n".format(
    Qwxc_dd['name'][0], Qwxcl, Qwxc_dd['t-8cl_new'][0],
    Qwxc_dd['t-2cl_new'][0])
                                          + " ②**{}**:昨日{}%，{}，{}\n\n".format(
    Gwxc_dd['name'][0], Gwxcl, Gwxc_dd['t-8cl_new'][0],
    Gwxc_dd['t-2cl_new'][0])
                                          + " ③**{}**:昨日{}%，{}，{}\n\n".format(
    APPxc_dd['name'][0], APPxcl, APPxc_dd['t-8cl_new'][0],
    APPxc_dd['t-2cl_new'][0])

                                +"【以下是CF情况】\n\n"
                                       + " **{}**:昨日{}%，{}，{}\n\n".format(
    CFc_dd['name'][0], CFcl, CFc_dd['t-8cl_new'][0],
    CFc_dd['t-2cl_new'][0])

                                          +"【以下是英雄联盟的情况】\n\n"
                                       + " **{}**:昨日{}%，{}，{}\n\n".format(
    Yxc_dd['name'][0], Yxcl, Yxc_dd['t-8cl_new'][0],
    Yxc_dd['t-2cl_new'][0])

                                          +"【以下是绝地求生的情况】\n\n"
                                        + " **{}**:昨日{}%，{}，{}\n\n".format(
    Jdc_dd['name'][0], Jdcl, Jdc_dd['t-8cl_new'][0],
    Jdc_dd['t-2cl_new'][0])

                                          + "【以下是逆战的情况】\n\n"

                                          + " **{}**:昨日{}%，{}，{}\n\n".format(
    Nzc_dd['name'][0], Nzcl, Nzc_dd['t-8cl_new'][0],
    Nzc_dd['t-2cl_new'][0])

                                          + "【以下是王者的情况】\n\n"

                                          + " **{}**:昨日{}%，{}，{}\n\n".format(
    Wzc_dd['name'][0], Wzcl, Wzc_dd['t-8cl_new'][0],
    Wzc_dd['t-2cl_new'][0])

                                          + "【以下是穿越手游的情况】\n\n"
                                       + " **{}**:昨日{}%，{}，{}\n\n".format(
    CFsc_dd['name'][0], CFscl, CFsc_dd['t-8cl_new'][0],
    CFsc_dd['t-2cl_new'][0])

                                          + "【以下是反恐的情况】\n\n"
                                    + " **{}**:昨日{}%，{}，{}\n\n".format(
    Fkc_dd['name'][0], Fkcl, Fkc_dd['t-8cl_new'][0],
    Fkc_dd['t-2cl_new'][0])

                                          + "【以下是QQ飞车的情况】\n\n"

                                          + " **{}**:昨日{}%，{}，{}\n\n".format(
    QQFc_dd['name'][0], QQFcl, QQFc_dd['t-8cl_new'][0],
    QQFc_dd['t-2cl_new'][0])


                                          + "【以下是侠盗猎车手的情况】\n\n"

                                          + " **{}**:昨日{}%，{}，{}\n\n".format(
    Xdc_dd['name'][0], Xdcl, Xdc_dd['t-8cl_new'][0],
    Xdc_dd['t-2cl_new'][0])

                                          + "【以下是火影忍者的情况】\n\n"

                                          + " **{}**:昨日{}%，{}，{}\n\n".format(
    Hyc_dd['name'][0], Hycl, Hyc_dd['t-8cl_new'][0],
    Hyc_dd['t-2cl_new'][0])

                                          #      +"④{}：昨日客单价{}".format(data['name'][0]/data1['name'][0])
                        #     +"### 2.新增用户数据\n\n"
                        #    +"#### 2.1 分端新增用户统计\n\n"
                        #    +"#### 2.2 新用户加款率\n\n"
                        #    +"#### 2.4 新用户撤单率\n\n"
                        #   +"### 3.老用户数据\n\n"
                       #     +"#### 3.2 老用户加款率\n\n"
                        #    +"#### 3.1 老用户活跃数据\n\n"
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
