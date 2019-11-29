#!/usr/bin/python
# -*- coding: utf-8 -*-
# 创建者：zhw_lzy
# 创建：2019-10-26
# 数据刷新频次: 月
# 备注：增值服务表--增值服务月封存表（货架＋用户级）包含限时货架、置顶货架、小喇叭、总增值服务
#***********************************************
# 更新 2019-10-27 zhw_xyxf 新增程序抬头设计
# 更新 2019-10-27 zhw_xyxf 调整程序结构做数据整合
# 更新依赖表：    源表：1、zhw_ext_service_count
#                       2、zhw_trumpet
#                       3、zhw_hao_timelimit
#                       4、zhw_hao
#                       5、zhw_dingdan
#                       6、zhw_hao_top
#
#
#              生成表：1、hid_buytop_month
#                      2、hid_buytimelimit_month
#                      3、hid_buytrumpet_month
#                      4、hid_service_month
#                      5、user_buytop_month
#                      6、user_buytimelimit_month
#                      7、user_buytrumpet_month
#                      8、user_service_month
#---------------------------------------------------------------
#*****************相关程序包导入*********************
import pandas as pd
import pymysql
import datetime
import time
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

#*****************参数区*********************
# 配置运营数据库地址
zhwdb = pymysql.connect(host = "am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com",user = "yunying",passwd = "Mo2O68koWe3UVjn3",db = "zhwdb" )
#写入目标数据库地址
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)

# 日期约束
# 更新日期，时间戳
Now_Date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") #当前时间
now = (datetime.datetime.now()).strftime('%Y%m%d') #今日日期
day_now = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime('%Y%m%d') #t-1
day_last_db = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime('%Y%m%d') #t-2
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days = 8)).strftime('%Y%m%d') #t-8
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days = 30)).strftime('%Y%m%d') #t-30

day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H') #h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours = 1)).strftime('%Y%m%d%H') #h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours = 2)).strftime('%Y%m%d%H') #h-2
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours = 9)).strftime('%Y%m%d%H') #h-12

last_month = (datetime.date.today() - relativedelta(months=+1)).strftime('%Y%m') #t-1 #上月日期
last_month_1 = (datetime.date.today() - relativedelta(months=+2)).strftime('%Y%m') #t-2
#*****************多表合一*********************
# 主表：label_user_day_20191101
# join表1：user_30_make_day_20191101 近30天行为信息
# join表2：user_make_month_20191101 上月行为信息
label_user = pd.read_sql('''
select 
t1.user_id
,sex
,address_code
,birthday
,age
,user_name
,user_qq
,taobao
,user_phone
,phone_area
,zc_lx
,user_dj
,user_status
,is_fx
,registered_time
,registration_time
,usable_money
,freeze_money
,deal_money
,account_shelf
,order_length
,order_number
,pm_sum_30d
,pm_sum_true_30d
,dd_count_30d
,dd_count_true_30d
,pm_max_30d
,pct_30d
,pct_true_30d
,cd_count_30d
,cd_rate_30d
,revoke_count_30d
,revoke_rate_30d
,zq_sum_30d
,zq_sum_true_30d
,zq_avg_30d
,zq_avg_true_30d
,max_yx_count_30d
,max_pf_count_30d
,pm_sum_lastm
,pm_sum_true_lastm
,dd_count_lastm
,dd_count_true_lastm
,pm_max_lastm
,pct_lastm
,pct_true_lastm
,cd_count_lastm
,cd_rate_lastm
,revoke_count_lastm
,revoke_rate_lastm
,zq_sum_lastm
,zq_sum_true_lastm
,zq_avg_lastm
,zq_avg_true_lastm
,max_yx_count_lastm
,max_pf_count_lastm
from label_user_day_20191101 t1
left join
user_30_make_day t2
on t1.user_id = t2.user_id
left join 
user_rent_201910 t3
on t1.user_id = t3.user_id
''', con=cnx)


label_user = label_user.fillna(value=0)


#写入目标数据库地址
label_user.to_sql(name='user_all_label_month_{}', con=cnx, if_exists = 'replace', index=False)


