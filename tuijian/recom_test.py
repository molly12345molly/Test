#!/usr/bin/python
# -*- coding: utf-8 -*-
'''英雄联盟推荐基于用户的推荐
gameid = 17
分为两个部分：1、历史订单偏好的用户推荐（通用）
'''

import time, datetime
import calendar
import pandas as pd
import numpy as np
from pandas import to_datetime
import pymysql
from sqlalchemy import create_engine
import jieba.analyse
from dateutil.relativedelta import relativedelta
import configparser
import yagmail
import redis
import warnings
warnings.filterwarnings("ignore")
#------------------------数据库配置----------------------------
cf = configparser.ConfigParser()
cf.read("E:/zuhaowan working/config.ini")
# cf.read("/usr/model/zhw_product/config/config.ini")
host = cf.get("Mysql-Database-test","host")
user = cf.get("Mysql-Database-test","user")
password = cf.get("Mysql-Database-test","password")
DB = cf.get("Mysql-Database-test","DB")
db = pymysql.connect(host,user,password,DB)

host = cf.get("Mysql-sjwj","host")
user = cf.get("Mysql-sjwj","user")
password = cf.get("Mysql-sjwj","password")
DB = cf.get("Mysql-sjwj","DB")
port = cf.get("Mysql-sjwj","port")
cnx = create_engine("mysql+pymysql://"+user+":"+password+"@"+host+":"+port+"/"+DB, echo=False)

redis_host = cf.get("test_redis","host")
redis_port = cf.get("test_redis","port")
# redis_password = cf.get("test_redis","password")
redis_db = cf.get("test_redis","user_db")
#------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d') #今日日期
day_now = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime('%Y%m%d') #t-1
day_last_db = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime('%Y%m%d') #t-2
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days = 8)).strftime('%Y%m%d') #t-8
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days = 30)).strftime('%Y%m%d') #t-30
day_last_60 = (datetime.datetime.now() - datetime.timedelta(days = 60)).strftime('%Y%m%d') #t-60

day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H') #h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours = 1)).strftime('%Y%m%d%H') #h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours = 2)).strftime('%Y%m%d%H') #h-2
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours = 9)).strftime('%Y%m%d%H') #h-12

last_month = (datetime.date.today() - relativedelta(months=+1)).strftime('%Y%m') #t-1 #上月日期
last_month_1 = (datetime.date.today() - relativedelta(months=+2)).strftime('%Y%m') #t-2

#------------------------核心皮肤词库----------------------------
#英雄联盟
# pf_word = ['至臻','龙瞎','摄魂','斩星','海克斯','冰雪节','电玩','IG','龙虾','魔剑','穿星','龙刀','哥特','花木兰','年限','庆典','冰原','周年','黑龙','冠军','蓝龙','剑仙','全英雄']
#

#----------------函数区------------
def jug_data(data,word):
    """关键词转特征"""
    if data.find(word) >= 0:
        result = 1
    else:
        result = 0
    return result

def recall_cate_by_lol_usertohid(result,host,port,dict,db):
    host = host
    port = port
    cnt = 0
    # 建立redis 连接池
    pool = redis.ConnectionPool(host = host, port = port,db=db)
    # 建立redis客户端
    client = redis.Redis(connection_pool=pool)
    pipe = client.pipeline()
    for i in range(len(result)):
        print(i)
        pipe.hset(dict, str(result.user_id.values[i]), str(result.推荐货架_new.values[i]))
        cnt += 1
        if cnt % 50000 == 0:
            pipe.execute()
    pipe.execute()
#-------------------推荐2 英雄联盟基于用户的推荐系统--------------------
#用户最近一次订单记录
# '区服','段位分层','租金档位','皮肤数量分层'
sql = '''
SELECT distinct b.id from 
(SELECT * from zhw_dingdan
where gameid = 17)a
inner join 
(SELECT id,jkx_userid from zhw_user
)b
on a.userid = b.jkx_userid
where b.id is not null
'''
data = pd.read_sql(sql,con=db)
# new_data = data.iloc[:,0:7]

sql = '''
SELECT distinct a.hid from 
(SELECT * from zhw_dingdan
where gameid = 17)a
inner join 
(SELECT id,jkx_userid from zhw_user
)b
on a.userid = b.jkx_userid
where b.id is not null
'''
hid = pd.read_sql(sql,con=db)



df = pd.DataFrame()
for i in data.id.values:
    iRandom = []  # 定义随机数列表
    listRandom = list(range(1, 800))  # 生成整数型1-60的列表组合
    random.shuffle(listRandom)  # 打乱列表顺序
    iRandom = listRandom[0:100]  # 截取打乱后的前10个值，赋值给新列表iRandom

    b = str()
    for j in iRandom:
        a = str(hid.hid.values[j])
        b = a + ',' + b
    to_sql = pd.DataFrame({'user_id': [i],
                           '推荐货架_new': [b]})
    df = pd.concat([to_sql, df], axis=0)


target_dict = "user_to_hao_17_all_test"

recall_cate_by_lol_usertohid(df,redis_host,redis_port,target_dict,3)


df = pd.DataFrame()
for i in hid.hid.values:
    iRandom = []  # 定义随机数列表
    listRandom = list(range(1, 800))  # 生成整数型1-60的列表组合
    random.shuffle(listRandom)  # 打乱列表顺序
    iRandom = listRandom[0:100]  # 截取打乱后的前10个值，赋值给新列表iRandom

    b = str()
    for j in iRandom:
        a = str(hid.hid.values[j])
        b = a + ',' + b
    to_sql = pd.DataFrame({'user_id': [i],
                           '推荐货架_new': [b]})
    df = pd.concat([to_sql, df], axis=0)


target_dict = "hao_to_hao_17_all_test"

recall_cate_by_lol_usertohid(df,redis_host,redis_port,target_dict,4)



sql = '''
SELECT distinct a.hid from 
(SELECT * from zhw_dingdan
where gameid = 17)a
inner join 
(SELECT id,jkx_userid from zhw_user
)b
on a.userid = b.jkx_userid
where b.id is not null
'''
hid = pd.read_sql(sql,con=db)
#------------------推荐 通用推荐账号池--------------------
sql = '''
select zhw_hao.*,chedan.count_chedan,dingdan.count_dingdan,round(chedan.count_chedan/dingdan.count_dingdan,2) as rate
  FROM (select hid,count(id) as count_dingdan
  from zhwdb.zhw_dingdan
  where  gameid='17' 
  and DATE_FORMAT( zhw_dingdan.add_time,'%Y%m') >= {0} 
  GROUP BY hid) as dingdan
  left  join  (select   hid, count(id) as count_chedan
  from zhwdb.zhw_dingdan where  gameid='17' and zt='3' 
  and DATE_FORMAT( zhw_dingdan.add_time,'%Y%m') >= {0} 
  GROUP BY hid) as chedan
  on dingdan.hid=chedan.hid 
  left join (select id as hid,  yxqu as 区服,  
  case when pid = 33 then '1'
  when pid = 53 then '2'
  when pid = 7682 then '3' end 游戏区
  ,pid as 游戏区编号
  ,case when dw like '%黑铁%' then '0'
  when dw like '%黄铜%' then '1'
  when dw like '%白银%' then '2'
  when dw like '%黄金%' then '3'
  when dw like '%铂金%' then '4'
  when dw like '%钻石%' then '5'
  when dw like '%宗师%' then '6'
  when dw like '%最强王者%' then '7'
  else '8'end 段位分层
  ,dw as 段位,  dwk as 段位框,    c as 真实出租次数,  c_rank as 显示出租次数,  
   	 case when pf <= 10 and pf > 0 then '0'
	when  pf >10 and pf < 100 then '1'
	when pf >= 100 and pf < 200 then '2'
	when pf >= 200 and pf < 300 then '3'
	when pf >= 300 and pf < 400 then '4'
	when pf >= 500 and pf < 600 then '5'
	when pf >= 600 and pf < 700 then '6'
	when pf >= 700 and pf < 800 then '7'
	when pf >= 800 and pf < 900 then '8'
	when pf >= 900 and pf < 1000 then '9'
	when pf >= 1000 and pf < 2000 then '10'
	else '0' end
	as 皮肤数量分层
	,pf as 皮肤数量
,SUBSTRING(yx,1,3) as 英雄数量
  ,case when pmoney >= 10  then '9'
  when pmoney >= 8 and pmoney < 10 then '8'
  when pmoney >= 5 and pmoney < 8 then '7'
  when pmoney >= 4 and pmoney < 5 then '6'
  when pmoney >= 3 and pmoney < 4 then '5'
  when pmoney >= 2 and pmoney < 3 then '4'
  when pmoney >= 1 and pmoney < 2 then '3'
  when pmoney < 1 then '2' else '1' end as 租金档位,pmoney as 时租, pmoney as 每小时租金,  pn as 标题,  youxi as 简要描述
  FROM zhwdb.zhw_hao 
  where gid=17
  and zt in(0,1)) as  zhw_hao
  on dingdan.hid= zhw_hao.hid
'''.format(201901)
tuijian_hid = pd.read_sql(sql,con=db)


# tuijian_hid['str'] = tuijian_hid['标题'] + tuijian_hid['简要描述']
# for i in pf_word:
#     tuijian_hid[i] = tuijian_hid['str'].apply(lambda x:jug_data(x,i))

tuijian_hid = tuijian_hid.ix[:,['hid', '区服','段位分层', '皮肤数量分层', '英雄数量','租金档位','时租']]


#通过区、段位、金额、皮肤数量进行关联
all_data = pd.merge(new_data,tuijian_hid,on=('区服','段位分层','租金档位','皮肤数量分层'))

# all_data.sort_values(by="hid_x",ascending=True,inplace=False)
# all_data.to_sql(name='recommender_hid', con=cnx, if_exists = 'replace', index=False,chunksize = 100000)
# print('SQL写入完毕')

all_data.sort_values(by="皮肤数量分层",ascending=False,inplace=True)

#合并推荐货架
all_data_2 = all_data.ix[:,['id','hid_x','hid_y']]
all_data_2 = all_data_2.astype(str)
all_data_2['userid_hid'] = all_data_2['id'] +str(',') +all_data_2['hid_x']

#删除相同的货架
drop_index = all_data_2.loc[all_data_2.hid_x == all_data_2.hid_y].index
all_data_2 = all_data_2.drop(index = drop_index,axis = 0)
all_data_2 = all_data_2.drop_duplicates(['userid_hid','hid_y'],keep='last')
all_data_3 = all_data_2['hid_y'].groupby(all_data_2['userid_hid']).aggregate(lambda x:','.join(x))

hid = pd.DataFrame(all_data_3.index)
tuijian_hid = pd.DataFrame(all_data_3.values)

data = pd.concat([hid,tuijian_hid],axis=1)
data.columns = ['用户','推荐货架']

data['user_id'] = data['用户'].str.split(',', 1).str[0]
data['订单货架'] = data['用户'].str.split(',', 1).str[1]

data['推荐货架_new'] = data['订单货架'] +str(',')+ data['推荐货架']
result = data.ix[:,['user_id','推荐货架_new']]

result['推荐货架_new'] = result['推荐货架_new'].apply(lambda x:None if len(str(x)) <= 40 else x)
result['lx'] = '01' #01通用
result['yx'] = '01' #英雄联盟
result.dropna(axis=0, how='any', inplace=True)
result.to_sql(name='user_to_hao_17_all', con=cnx, if_exists = 'replace', index=False,chunksize = 100000)
print('usertohid写入完毕')


target_dict = "user_to_hao_17_all"
recall_cate_by_lol_usertohid(result,redis_host,redis_port,target_dict)
# print('usertohid redis写入完毕')

now_H = (datetime.datetime.now()).strftime('%H') #h
yag = yagmail.SMTP(user='2736187789@qq.com', password='arwzonfgbfufdgdb', host='smtp.qq.com')
subject = '{}点英雄联盟推荐数据更新提醒'.format(day_now_H)
user = ['j3n_h3lxlpb1h@dingtalk.com', 'lizhengyuan7703@dingtalk.com']
yag.send(to=user, subject=subject, contents='OK!')
print('钉邮推送判断完毕')

# 在name对应的hash中根据key获取value
# host = redis_host  # "127.0.0.1"
# port = redis_port  # 6379
#
# # 建立redis 连接池
# pool = redis.ConnectionPool(host=host, port=port)
# # 建立redis客户端
# client = redis.Redis(connection_pool=pool)
#
#
# print(client.hlen("user_to_hid_lol_ty"))
# # 删除指定name对应的key所在的键值对
# client.hdel("user_to_hid_lol_ty")
#
#
# client.delete("user_to_hid_lol_ty")
#
# print(client.hget("user_to_hid_lol_ty",'10977920|13369410755'))
# # client.hgetall("user_to_hid_lol_ty")
# pipe = client.pipeline()
# pipe.hset(dict, str(result.user_id.values[1]), str(result.推荐货架_new.values[1]))

