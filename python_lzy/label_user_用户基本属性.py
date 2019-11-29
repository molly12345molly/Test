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

#*****************基础货架/用户信息*********************
# 近30天有登陆的用户基本属性
label_user = pd.read_sql('''
SELECT
  zhw_user.jkx_userid as user_id,
	DATE_FORMAT(zhw_user.jkx_timer,'%Y%m%d') as registered_time,
	case when substring ( zhw_user.jkx_usercard,11,4 ) > 0 then substring ( zhw_user.jkx_usercard,11,4 ) else '0000' end as birthday,
	case when zhw_xy_user.sex = 0 then '男' when zhw_xy_user.sex = 1 then '女' else '其他' end as sex,
	case when TIMESTAMPDIFF(YEAR,substring(zhw_user.jkx_usercard,7,8 ),now()) >0 then TIMESTAMPDIFF(YEAR,substring(zhw_user.jkx_usercard,7,8 ),now()) else 0 end as age,
	zhw_user.DATEDIFF( now( ),zhw_user.jkx_timer ) as registration_time,
	zhw_user.jkx_usermoney as usable_money,
	zhw_user.jkx_userdjmoney as freeze_money,
	zhw_user.jkx_userjymoney as deal_money,	
	case when sum( zhw_dingdan.zq) > 0 then sum( zhw_dingdan.zq) else 0 end  as rent_length,
	count( zhw_dingdan.id) as rent_number,	
	COALESCE(substring ( zhw_user.jkx_usercard,1,6 ),0) address_code,
	COALESCE(zhw_user.jkx_username,0) as user_name,
	COALESCE(zhw_user.jkx_userqq,0)  as user_qq,
	COALESCE(zhw_user.taobao,0) as taobao,
	COALESCE(zhw_user.jkx_userphone,0) as user_phone,
	COALESCE(zhw_user.phone_area,0) as phone_area,
	zhw_user.jkx_lx as zc_lx,
	zhw_user.jkx_userdj as user_dj,
	zhw_user.jkx_userstatus as user_status,
	zhw_user.is_fx as is_fx,
	zhw_user.account_shelf as account_shelf,
	zhw_user.block as block
	FROM	
	(select DISTINCT userid
	FROM zhw_dl 
	WHERE DATE_FORMAT(usertimer,'%Y%m%d') BETWEEN {} and {}) as dl
	LEFT join zhw_user on dl.userid = zhw_user.jkx_userid
	LEFT JOIN zhw_xy_user ON zhw_user.jkx_userid = zhw_xy_user.jkx_userid
	LEFT join zhw_dingdan on zhw_dingdan.huserid= dl.userid
	GROUP BY zhw_user.jkx_userid
'''.format(day_last_30,day_now), con=zhwdb)


# 添加当前时间为时间标识
# hlm_service_sum.columns = ['month_id','hid_id','Buy_cnt','Buy_money']
label_user['update_time'] = Now_Date



#写入目标数据库地址
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)

label_user.to_sql(name='label_user_day_{}'.format(now), con=cnx, if_exists = 'replace', index=False)
