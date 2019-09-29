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
import configparser

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
    if data >= 0 :
        new_data = '增长' + str(format(data * 100 ,'.2f')) + '%'
    elif data < 0:
        new_data = '下降' + str(format(data * 100, '.2f')) + '%'
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
  elif holiday >= 1:
      data = '节假日'
  else:
      data = 'Error'
  return data

#-------------------数据提取--------------------
#计算订单金额B指标，t为今日，字段t-1日订单金额，t-2日订单金额，t-8日订单金额，(t-2)/(t-1)-1为对比，com bit 对比    (t-8)/(t-1)-1为环比 per bit环比
#A是昨天，B是前天，C是上周同一天查出来的量

#-------------------整体指标--------------------
sql1 = '''SELECT  {},{},{},'订单金额','A0001','元',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT  sum(pm) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT  sum(pm) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT  sum(pm) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)

#订单量
sql2 = '''SELECT  {},{},{},'订单量','A0002','单',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B FROM
(SELECT  count(*) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT  count(*) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT  count(*) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)
#订单人数
sql3 = '''SELECT  {},{},{},'订单人数','A0003','人',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT  count(DISTINCT userid) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT  count(DISTINCT userid) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT  count(DISTINCT userid) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)
#新增人数
sql4 = '''SELECT  {},{},{},'新增人数','A0004','人',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT  count(jkx_userid) as A
FROM zhw_user WHERE DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t1,
(SELECT  count(jkx_userid) as B
FROM zhw_user WHERE DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t2,
(SELECT  count(jkx_userid) as C
FROM zhw_user WHERE DATE_FORMAT(jkx_timer,'%Y%m%d') = {}
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)
#QQ阅读
sql5 = '''SELECT {},{},{},'QQ阅读订单金额','A0005','元',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B FROM
(SELECT  sum(pm) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
AND gameid=639
)t1,
(SELECT  sum(pm) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
AND gameid=639
)t2,
(SELECT  sum(pm) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
AND gameid=639
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)
#客单价
sql6 = '''SELECT  {},{},{},'客单价','A0006','元',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT  sum(pm)/count(*) as A
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t1,
(SELECT  sum(pm)/count(*) as B
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t2,
(SELECT  sum(pm)/count(*) as C
FROM zhw_dingdan WHERE DATE_FORMAT(add_time,'%Y%m%d') = {}
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)



#-------------------各维度注册人数--------------------
sql7 = '''SELECT  {},{},{},'官网注册人数','A0007','人',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
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
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)

#APP注册人数
sql8 = '''SELECT  {},{},{},'APP注册人数','A0008','人',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
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
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)

#M站注册人数
sql9 = '''SELECT  {},{},{},'M站注册人数','A0009','人',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
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
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)

#客户端
sql10 = '''SELECT  {},{},{},'客户端','A0010','人',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
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
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)

#其他平台
sql11 = '''SELECT  {},{},{},'其他平台','A0011','人',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
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
)t3'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)




#----------------------TOP10游戏---------------------
# Top10游戏的情况
#订单金额
sql12 = '''SELECT  {},{},{},'游戏订单金额',t1.gameid,'元',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT gameid,sum(pm) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t1 
left join
(SELECT gameid,sum(pm) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t2 
on t1.gameid = t2.gameid
left join
(SELECT gameid,sum(pm) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t3 
on t1.gameid = t3.gameid
'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)

#订单量
sql13 = '''SELECT  {},{},{},'游戏订单量',t1.gameid,'单',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT gameid,count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t1 
left join
(SELECT gameid,count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t2 
on t1.gameid = t2.gameid
left join
(SELECT gameid,count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t3 
on t1.gameid = t3.gameid
'''.format(day_now,day_last_db,day_last_hb,day_now,day_last_db,day_last_hb)


#撤单率
sql14 = '''SELECT  {},{},{},'撤单率',t1.gameid,'',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT gameid,sum(case when zt = 3 then 1 else 0 end)/count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t1 
left join
(SELECT gameid,sum(case when zt = 3 then 1 else 0 end)/count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t2 
on t1.gameid = t2.gameid
left join
(SELECT gameid,sum(case when zt = 3 then 1 else 0 end)/count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t3 
on t1.gameid = t3.gameid
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)

#客单价
sql15 = '''SELECT  {},{},{},'客单价',t1.gameid,'元',A,A/C-1 as per_bit ,A - C,A/B-1 as com_bit,A - B  FROM
(SELECT gameid,sum(pm)/count(*) as A
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t1 
left join
(SELECT gameid,sum(pm)/count(*) as B
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t2 
on t1.gameid = t2.gameid
left join
(SELECT gameid,sum(pm)/count(*) as C
FROM zhw_dingdan WHERE  DATE_FORMAT(add_time,'%Y%m%d') = {}
and gameid in (11,17,581,24,443,560,446,22,25,441)
GROUP BY 1
)t3 
on t1.gameid = t3.gameid
'''.format(now,day_now,day_now,day_now,day_last_db,day_last_db,day_last_hb,day_last_hb)

game_id = {
        11: 'CF',
        17: '英雄联盟',
        581: '绝地求生',
        24: '逆战',
        443: '王者荣耀',
        560: 'CF手游',
        446: '反恐精英',
        22: 'QQ飞车',
        25: '侠盗猎车手',
        441: '火影忍者'
    }
data1 = pd.read_sql(sql1,con=db)
data2 = pd.read_sql(sql2,con=db)
data3 = pd.read_sql(sql3,con=db)
data4 = pd.read_sql(sql4,con=db)
data5 = pd.read_sql(sql5,con=db)
data6 = pd.read_sql(sql6,con=db)
data7 = pd.read_sql(sql7,con=db)
data8 = pd.read_sql(sql8,con=db)
data9 = pd.read_sql(sql9,con=db)
data10 = pd.read_sql(sql10,con=db)
data11 = pd.read_sql(sql11,con=db)
data_1 = pd.concat([data1,data2,data3,data4,data5,data6,data7,data8,data9,data10,data11],axis=0)
data_1.columns = ['day_now','day_last_db','day_last_hb','name','code','day_now_data','dw','com_bit','A-C','per_bit','A-B']

data12 = pd.read_sql(sql12,con=db)
data13 = pd.read_sql(sql13,con=db)
data14 = pd.read_sql(sql14,con=db)
data15 = pd.read_sql(sql15,con=db)

data_2 = pd.concat([data12,data13,data14,data15],axis=0)
data_2.columns = ['day_now','day_last_db','day_last_hb','name','code','day_now_data','dw','com_bit','A-C','per_bit','A-B']

data = pd.concat([data_1,data_2],axis=0)



data['name'] = data.code.apply(lambda x: '对比'+jugde_data(x))
data['com_bit_new'] = data['com_bit'].apply(lambda x: '对比'+jugde_data(x))
data['A-C_new']=data['A-C'].apply(lambda x:'环比'+jugde_data(x))
data['A-B_new']=data['A-B'].apply(lambda x:'对比'+jugde_data(x))
data['per_bit_new'] = data['per_bit'].apply(lambda x: '环比'+jugde_data(x))
data['holiday_judge_day_last'] = data['day_last_db'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#判断是否为节假日
data['holiday_judge_day_now'] = data['day_now'].apply(lambda x: holiday_judge(to_datetime(x,format="%Y/%m/%d").strftime('%Y%m%d')))
#判断今日是否为节假日
data = pd.concat([data1,data2,data3,data4,data5,data6,data7,data8,data9,data10,data11,data12,data13,data14],axis=0)
data.columns = ['day_now','day_last_db','day_last_hb','name','code','day_now_data','dw','com_bit','A-C','per_bit','A-B']
