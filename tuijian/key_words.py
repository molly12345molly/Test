#!/usr/bin/python
# -*- coding: utf-8 -*-
# 创建者：zhw_xyxf
# 创建：2019-11-01
# 数据刷新频次: 小时级
# 备注：推荐关键词（业务+算法制定）
#***********************************************
# 更新 2019-11-01 zhw_xyxf 算法验证
# 更新 2019-10-27 zhw_xyxf 基于用户行为的推荐
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
import time, datetime
import calendar
import pandas as pd
import numpy as np
from pandas import to_datetime
import pymysql
from sqlalchemy import create_engine
import jieba.analyse
import configparser
import os

#------------------------参数变量区----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d') #今日日期
day_now = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime('%Y%m%d') #t-1
day_last_db = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime('%Y%m%d') #t-2
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days = 8)).strftime('%Y%m%d') #t-8
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days = 30)).strftime('%Y%m%d') #t-30

day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H') #h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours = 1)).strftime('%Y%m%d%H') #h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours = 2)).strftime('%Y%m%d%H') #h-2
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours = 9)).strftime('%Y%m%d%H') #h-12

#------------------------数据库变量区----------------------------
conf = configparser.ConfigParser()

db_host = conf['YunYing_Mysql-Database']['host']
db_user = conf['YunYing_Mysql-Database']['user']
db_password = conf['YunYing_Mysql-Database']['password']
db_db = conf['YunYing_Mysql-Database']['db']
db = pymysql.connect(db_host,db_user,db_password,db_db)

cnx_host = conf['sjwj-Database']['host']
cnx_user = conf['sjwj-Database']['user']
cnx_password = conf['sjwj-Database']['password']
cnx_db = conf['sjwj-Database']['db']
cnx = pymysql.connect(db_host,db_user,db_password,db_db)
