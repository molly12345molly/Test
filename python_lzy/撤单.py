#!/usr/bin/python
# -*- coding: utf-8 -*-
'''小时级封装今日各游戏的撤单情况'''
#!/usr/bin/python
# -*- coding: utf-8 -*-
import time, datetime
import calendar
import pandas as pd
import numpy as np
from pandas import to_datetime
import pymysql
from sqlalchemy import create_engine
import jieba.analyse
#------------------------参数变量区----------------------------
db = pymysql.connect("am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com","yunying","Mo2O68koWe3UVjn3","zhwdb" )
#连接我们的数据库
now = (datetime.datetime.now()).strftime('%Y%m%d') #今日日期
day_now = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime('%Y%m%d') #t-1
day_last_db = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime('%Y%m%d') #t-2
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days = 8)).strftime('%Y%m%d') #t-8
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days = 30)).strftime('%Y%m%d') #t-30

day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H') #h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours = 1)).strftime('%Y%m%d%H') #h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours = 2)).strftime('%Y%m%d%H') #h-2
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours = 9)).strftime('%Y%m%d%H') #h-12
#-------------------主程序--------------------
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
game_name = pd.read_sql('''SELECT gameid,game_name from game
''',con=cnx)
game_name.columns = ['gameid','game_name']


#

sql = '''
	SELECT gameid,a as 一个月撤单量,b as 一个月订单, round(a/b*100,2) as 撤单率 FROM (
  SELECT gameid,sum(CASE WHEN zt=3 THEN 1 else 0 END) as a,COUNT(DISTINCT id) as b 
  from zhw_dingdan t1
  WHERE huserid ='giveout'
  and DATE_FORMAT(add_time,'%Y%m') = 201910
  GROUP BY 1)
'''
chedan_reason_2 = pd.read_sql(sql,con=db)

chedan_reason_2 = pd.merge(chedan_reason_2, game_name, how='left',  left_on='gameid', right_on='gameid')

chedan_reason_2 = chedan_reason_2.ix[:,['game_name','一个月撤单量','一个月订单','撤单率']]

chedan_reason_2.to_csv("E:/高危货架.csv", index=False, encoding='utf_8_sig')
