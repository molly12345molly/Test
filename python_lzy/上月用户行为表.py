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
str = '_lastm'
#*****************基础货架/用户信息*********************
# 上月用户行为
label_user = pd.read_sql('''
SELECT
  DATE_FORMAT( zhw_dingdan.add_time, '%Y%m' ) AS month_id,
  zhw_dingdan.userid AS user_id,
  sum( zhw_dingdan.pm ) AS pm_sum{0},
  sum(case when zhw_dingdan.zt='2' then zhw_dingdan.pm END ) AS pm_sum_true{0},
  count( zhw_dingdan.id ) AS dd_count{0},
  count(case when zhw_dingdan.zt='2' then zhw_dingdan.id END ) AS dd_count_true{0},
  max( zhw_dingdan.pm ) AS pm_max{0},
  sum( zhw_dingdan.pm )/ count( zhw_dingdan.id ) AS pct{0},
  sum(case when zhw_dingdan.zt='2' then zhw_dingdan.pm END )/count(case when zhw_dingdan.zt='2' then zhw_dingdan.id END ) as pct_true{0},
  count(case when zhw_dingdan.zt='3' then zhw_dingdan.id END ) AS cd_count{0},
  round(count(case when zhw_dingdan.zt='3' then zhw_dingdan.id END )/count( zhw_dingdan.id ) ,2) as cd_rate{0},
  count( zhw_ts.did ) AS revoke_count{0},
  round( count( zhw_ts.did )/ count( zhw_dingdan.id ), 2 ) AS revoke_rate{0},
  sum(zhw_dingdan.zq) as zq_sum{0},
  sum(case when zhw_dingdan.zt='2'then  zhw_dingdan.zq END) as zq_sum_true{0},
  round(sum(zhw_dingdan.zq)/count( zhw_dingdan.id ),2) as zq_avg{0},
  round(sum(case when zhw_dingdan.zt='2'then  zhw_dingdan.zq END)/count(case when zhw_dingdan.zt='2' then zhw_dingdan.id END )) as zq_avg_true{0},
  COALESCE(max(zhw_dingdan.yx),0) as max_yx_count{0},
  COALESCE(max(zhw_dingdan.pf),0) as max_pf_count{0}
FROM
  zhw_dingdan
  LEFT JOIN zhw_ts ON zhw_dingdan.id = zhw_ts.did
WHERE
  DATE_FORMAT( zhw_dingdan.add_time,'%Y%m') = {1}
  #and zhw_dingdan.gameid ='17'
GROUP BY 1,2
'''.format(str,last_month), con=zhwdb)


# 添加当前时间为时间标识
label_user['update_time'] = Now_Date



#写入目标数据库地址
label_user.to_sql(name='user_make_month_{}'.format(now), con=cnx, if_exists = 'replace', index=False)




