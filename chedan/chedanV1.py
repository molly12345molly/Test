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
import ConfigParser
import os
#------------------------参数变量区----------------------------
# 配置运营数据库地址
zhwdb = pymysql.connect(host = "am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com",user = "yunying",passwd = "ck2KyZ5Gsb54tzC4",db = "zhwdb" )
#写入目标数据库地址
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)


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
#撤单天情况封装
# Cd_H_sql = '''select t1.*,Cd_H / Dd_H as Cd_H_Bit from
# (SELECT DATE_FORMAT(add_time,'%Y%m%d%H') as Date_H,gameid,count(*) as Dd_H,sum(case when zt=3 then 1 else 0 end) as Cd_H
# FROM zhw_dingdan
# WHERE  DATE_FORMAT(add_time,'%Y%m%d') BETWEEN {} and {}
# GROUP BY 1,2
# )t1'''.format(day_last_30,day_now)
# Now_Cd_Data = pd.read_sql(Cd_H_sql,con=db)
# Now_Cd_Data.columns = ['时间（小时）','游戏Id','订单量（小时）','撤单量（小时）','撤单比率（小时）']
#
# cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
# Now_Cd_Data.to_sql(name='Cd_Game_Day_H', con=cnx, if_exists = 'replace', index=False)
#
#
# #撤单明细和撤单大类的映射关系提取
# chedan_reason = pd.read_sql('''
# select case when lx in ('不想玩了或其它理由不玩了','租错号了','账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','复活币不足','裁决之廉','安全问题错误','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','自己的号要撤销','自己要玩','上号器自动投诉（账号密码错误）','游戏账号未实名认证','因财产密码','steam客服已冻结该帐户','会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了','使用外挂 By 上号器','通过篡改上号器文件恶意破解,错误代码：1008 By 上号器','租客违规操作','租方打排位','租方开外挂','上号器自动投诉（使用外挂）','提示有外挂残留','250','ZD主动防御','无法登陆（非密码错误问题）','一直云检测','无法下载上号器','不输入账号密码','安装不了上号器','客服仲裁错误') then lx else '其他' end lx_new,re,count(*)  from zhw_ts
# where DATE_FORMAT(t,'%Y%m%d') BETWEEN {} and  {}
# GROUP BY 1,2
# order by 1
# '''.format(day_last_hb,now),con=db)
#
# # chedan_data['re_list'] = chedan_data['re'].groupby(chedan_data.hid).aggregate(lambda x:','.join(x))
# # chedan_data['lx_list'] = chedan_data['lx'].groupby(chedan_data.hid).aggregate(lambda x:','.join(x))
#
# re = chedan_reason['re'].groupby(chedan_reason.lx_new).aggregate(lambda x:','.join(x))
#
# lx = pd.DataFrame(re.index)
# lx_list = pd.DataFrame(re.values)
#
# list_data = pd.concat([lx,lx_list],axis=1)
# list_data.columns = ['lx','lx_list']
#
# test = list_data
#
# def get_words(data):
#     words = jieba.analyse.extract_tags(data, topK=10, withWeight=True, allowPOS=())
#     return words
#
# test['lx_keywords'] = test['lx_list'].apply(lambda x: get_words(x))
# data = pd.concat([test['lx'],test['lx_keywords']],axis=1)
# data.columns = ['lx','lx_keywords']
#
# data.to_csv('E:/data.csv', index=False, encoding='utf_8_sig')

#12小时的货架撤单跟踪分析
Cd_H_sql = '''
select {2} as Date_H,s1.hid,huserid,phone,zh,gameid,timelimit_hao,cd_cnt,dingdan_cnt,cd_cnt/dingdan_cnt as cd_bit
from 
(SELECT hid,t1.huserid,phone,t2.zh,gameid,case when timelimit_id > 0 then 1 else 0 end as timelimit_hao,COUNT(DISTINCT t1.id) as dingdan_cnt 
FROM zhw_dingdan t1
left join 
zhw_hao t2
on t1.hid = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
and (t2.zt = 0 or t2.zt = 1)
and t1.hid <> 38
GROUP BY 1,2,3,4
)S1
inner join
(
SELECT t2.hid,count(distinct t2.id) as cd_cnt from zhw_ts t1
inner join zhw_dingdan t2
on t1.did = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
AND t2.zt=3
and t2.hid <> 38
and lx in  
('账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','裁决之廉','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','上号器自动投诉（账号密码错误）','会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了')
GROUP BY 1
)S2
on S1.hid = S2.hid
HAVING cd_bit = 1
and dingdan_cnt = 2
order by dingdan_cnt desc

union 

select {2} as Date_H,s1.hid,huserid,phone,zh,gameid,timelimit_hao,cd_cnt,dingdan_cnt,cd_cnt/dingdan_cnt as cd_bit
from 
(SELECT hid,t1.huserid,phone,t2.zh,gameid,case when timelimit_id > 0 then 1 else 0 end as timelimit_hao,COUNT(DISTINCT t1.id) as dingdan_cnt 
FROM zhw_dingdan t1
left join 
zhw_hao t2
on t1.hid = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
and (t2.zt = 0 or t2.zt = 1)
and t1.hid <> 38
GROUP BY 1,2,3,4
)S1
inner join
(
SELECT t2.hid,count(distinct t2.id) as cd_cnt from zhw_ts t1
inner join zhw_dingdan t2
on t1.did = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
AND t2.zt=3
and t2.hid <> 38
and lx in  
('账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','裁决之廉','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','上号器自动投诉（账号密码错误）','会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了')
GROUP BY 1
)S2
on S1.hid = S2.hid
HAVING cd_bit >= 0.6
and dingdan_cnt BETWEEN 3 and 5
order by dingdan_cnt desc

union 

select {2} as Date_H,s1.hid,huserid,phone,zh,gameid,timelimit_hao,cd_cnt,dingdan_cnt,cd_cnt/dingdan_cnt as cd_bit
from 
(SELECT hid,t1.huserid,phone,t2.zh,gameid,case when timelimit_id > 0 then 1 else 0 end as timelimit_hao,COUNT(DISTINCT t1.id) as dingdan_cnt 
FROM zhw_dingdan t1
left join 
zhw_hao t2
on t1.hid = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
and (t2.zt = 0 or t2.zt = 1)
and t1.hid <> 38
GROUP BY 1,2,3,4
)S1
inner join
(
SELECT t2.hid,count(distinct t2.id) as cd_cnt from zhw_ts t1
inner join zhw_dingdan t2
on t1.did = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
AND t2.zt=3
and t2.hid <> 38
and lx in  
('账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','裁决之廉','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','上号器自动投诉（账号密码错误）','会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了')
GROUP BY 1
)S2
on S1.hid = S2.hid
HAVING cd_bit >= 0.5
and dingdan_cnt >= 6
order by dingdan_cnt desc
'''.format(day_now_H_Last_13,day_now_H_Last,day_now_H)
Now_Cd_Data = pd.read_sql(Cd_H_sql,con=db)
Now_Cd_Data.columns = ['捕获时段','货架号','用户名','联系电话','游戏账号','gameid','是否限时货架','撤单量（12小时）','订单量（12小时）','真实撤单比']

# cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
# Now_Cd_Data.to_sql(name='Cd_Hid_Day_H', con=cnx, if_exists = 'append', index=False)

#货架连续异常周期判断1
db = pymysql.connect("am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com","yunying","ck2KyZ5Gsb54tzC4","zhwdb" )
sql = '''
SELECT t2.hid,DATE_FORMAT(add_time,'%Y%m%d%H') as Date from zhw_ts t1
inner join zhw_dingdan t2
on t1.did = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
AND t2.zt=3
and t2.hid <> 38
GROUP BY 1,2
order by 2 desc
'''.format(day_now_H_Last_13,day_now_H_Last)
hid_reason_lx = pd.read_sql(sql,con=db)
hid_reason_lx['Date'] = hid_reason_lx['Date'].astype('str')
Date = hid_reason_lx['Date'].groupby(hid_reason_lx.hid).aggregate(lambda x:','.join(x))
# Date = hid_reason_lx.groupby(by='hid').apply(lambda x:[','.join(x['Date'])])

def iscon(L):
    a=max(L)
    b=min(L)
    if a-b+1==len(set(L)) and len(L)>1:
        return True
    else:
        return False

def f(a):
    b=[]
    c=[]
    for i in range(len(a)):
        j=len(a)
        while j>i:
            if iscon(a[i:j]):
                b.append(a[i:j])
                c.append(j-i+1)
                break
            j-=1
    for k in range(len(c)):
        if c[k] == max(c):
            return b[k]

hid = pd.DataFrame(Date.index)
Datetime = pd.DataFrame(Date.values)

list_data = pd.concat([hid,Datetime],axis=1)
list_data.columns = ['hid','Datetime']

hid = list(hid_reason_lx.drop_duplicates(subset=None, keep='first', inplace=False).hid)

result = pd.DataFrame()

for i in hid:
    a = list(hid_reason_lx[hid_reason_lx['hid'] == i]['Date'])
    s = list(map(int, a))
    if (len(s) >= 2) and (f(s) is not None):
        hid_len = len(f(s))
        hid_max = max(f(s))
    else:
        hid_len = 0
        hid_max = 0
    data = pd.DataFrame({'hid': [i],
                         'hid_len': [hid_len],
                         'hid_max': [hid_max]})
    result = pd.concat([data, result], axis=0)

result['len'] = result.apply(lambda x:x.hid_len if str(x.hid_max) == day_now_H_Last else 0,axis = 1)
result = result.drop_duplicates(subset=None, keep='first', inplace=False)

#游戏账号连续异常次数计算
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
try:
    hid_cnt = pd.read_sql('''SELECT distinct 游戏账号,持续异常周期（小时） from cd_reason_day_12H
    where 捕获时段 = {}
    '''.format(day_now_H_Last),con=cnx)
    hid_cnt.columns = ['游戏账号','持续异常周期']
except:
    hid_cnt = pd.DataFrame({'游戏账号': [],
                         '持续异常周期': []})
    pass

#高危货架撤单原因分析
sql = '''
SELECT t2.hid,case when lx in ('不想玩了或其它理由不玩了','租错号了','账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','复活币不足','裁决之廉','安全问题错误','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','自己的号要撤销','自己要玩','上号器自动投诉（账号密码错误）','游戏账号未实名认证','因财产密码','steam客服已冻结该帐户','会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了','使用外挂 By 上号器','通过篡改上号器文件恶意破解,错误代码：1008 By 上号器','租客违规操作','租方打排位','租方开外挂','上号器自动投诉（使用外挂）','提示有外挂残留','250','ZD主动防御','无法登陆（非密码错误问题）','一直云检测','无法下载上号器','不输入账号密码','安装不了上号器','客服仲裁错误') then lx else '其他' end lx,re from zhw_ts t1
inner join zhw_dingdan t2
on t1.did = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
AND t2.zt=3
and t2.hid <> 38
GROUP BY 1,2,3
'''.format(day_now_H_Last_13,day_now_H_Last)
chedan_reason_2 = pd.read_sql(sql,con=db)


chedan_reason_2 = pd.merge(Now_Cd_Data, chedan_reason_2, how='inner',  left_on='货架号', right_on='hid')


re = chedan_reason_2['re'].groupby(chedan_reason_2.hid).aggregate(lambda x:','.join(x))
lx = chedan_reason_2['lx'].groupby(chedan_reason_2.hid).aggregate(lambda x:','.join(x))


hid = pd.DataFrame(re.index)
re_list = pd.DataFrame(re.values)
lx_list = pd.DataFrame(lx.values)

list_data = pd.concat([hid,lx_list,re_list],axis=1)
list_data.columns = ['hid','lx_list','re_list']

chedan_reason_2.drop_duplicates(subset=['hid'],keep='first',inplace=True)

chedan_reason_2 = pd.merge(chedan_reason_2, list_data, how='inner',  left_on='货架号', right_on='hid')


def get_words(data):
    words = jieba.analyse.extract_tags(data, topK=3, withWeight=False, allowPOS=())
    return words

chedan_reason_2['re_keywords'] = chedan_reason_2['re_list'].apply(lambda x: get_words(x))
chedan_reason_2['lx_keywords'] = chedan_reason_2['lx_list'].apply(lambda x: get_words(x))

# del chedan_reason_2['lx_list']
# del chedan_reason_2['re_list']

# import re
# import jieba
# from aip import AipNlp
#
# APP_ID = "17550925"
# API_KEY = "CEeaE21795ByZHVMc9bw08xv"
# SECRET_KEY = "GEWr5Rl8OCgGIQ990PCLhOqGoCikSiPY"
#
# client = AipNlp(APP_ID, API_KEY, SECRET_KEY)
#
# CORD_ID_RE = re.compile(r'\d{5,}')

# def baidu_sim(s1: str, s2: str, judge_rate: float = 0.9):
#     """ 如果有可选参数 """
#     options = {}
#     options["model"] = "CNN"
#     is_sim = False
#     try:
#         """ 带参数调用短文本相似度 """
#         score = client.simnet(s1, s2, options).get('score', 0.0)
#         print(f"score -> {score}")
#         is_sim = (score >= judge_rate)
#         print(is_sim)
#     except Exception as e:
#         print(e)
#     return is_sim
#

reason_weight = {
        # '租客恶意行为':['租客','挂机','恶意','违规'],
        # '不想玩了或其他理由不玩了':['不想','不了','退款','退款','不玩','谢谢', '不好意思','租错','错号'],
        # '账号问题（描述不符）':['不符','符合','没有','啊', '段位'],
        # '账号问题（密码错误）':['账号','密码','错误'],
        #  '外挂识别':['外挂', '残留','提示','错误代码','篡改','破解','文件','通过'],
        # '账号问题(被封、被冻结、裁决)':['小黑','裁决','被封','封号','冻结','QQ','qq','登陆','无法','账号','禁赛','挂机'],
        # '一直云检测': ['检测','不了', '一直', '游戏', '小黑', '登录','上去', '进不去'],  #
        # '信誉积分不足': ['信誉', '积分', '排位', '不了', '不足', '不够', '80'],  #
        # '游戏账号未实名认证': ['实名', '认证', '不了', '游戏', '验证',  '防沉迷', '时间','邮箱'],
        # '账号问题（其他）':['无法','登陆'],
        # 'TP检测16-2/36-2': [ '检测', 'TP', 'tp', '登录'],


    '250':['250','251'],    #
         'QQ冻结（QQ暂时无法登陆）':['冻结', 'QQ', '登录', '啊啊啊', '账号', '无法', '不了', 'qq', '退款', '登陆'],    #
         'TP检测16-2/36-2': ['小黑', '检测', '啊啊啊', '不了', '游戏', '排位', 'TP', '退款', 'tp', '登录'],    #
         'ZD主动防御':['主动防御', 'ZD', 'md5'],    #
        '排队':['排队'],
            '维护':['维护','游戏'],
         'steam客服已冻结该帐户':['不了', '游戏', '账号', '退款', '封禁', '登录', '啊啊啊', '帐号', '封号', '无法'],    #
         '一直云检测':['检测', '啊啊啊', '不了', '一直', '游戏', '小黑', '登录', '退款', '上去', '进不去'],    #
         '上号器自动投诉（qq冻结）':['QQ', '冻结', '暂时'],    #
         '上号器自动投诉（账号密码错误）':['密码', '错误'],    #
         '不想玩了或其它理由不玩了':['不想', '啊啊啊', '不了', '退款', '游戏', '不玩', '排位', '谢谢', '不好意思', '段位'],    #
         '不输入账号密码':['输入', '密码', '啊啊啊', '登录', '账号密码', '不了', '帐号密码', '一直', '退款', '游戏'],    #
         '会员时间到期':['会员', '到期', '啊啊啊', '没有', '时间', '不了', '退款', '账号', '过期', '游戏'],    #
         '使用外挂 By 上号器':['MD5', 'exe', 'dll', 'Users', '路径', '浏览器', 'rkr', '外挂', 'Administrator', '360'],    #
         '信誉积分不足':[ '不足','信誉', '积分', '排位', '不了', '不足', '不够', '匹配', '退款', '人机', '80'],    #
         '其他':['游戏', '维护', '不了', '啊啊啊', '小黑', '退款', '更新', '登录', '退钱', '小时'],    #
         '号被封了':['小黑', '封号', '封', '被封', '被封号', '账号', '游戏', '封号', '登录', '帐号'],    #
         '因财产密码':['密码', '财产', '错误', '1234', '啊啊啊', '游戏', '输入', '不了', 'F9', '无法'],    #
         '复活币不足':['复活', '没有', '啊啊啊', '一个', '怎么', '猎场', '不足', '游戏', '不了', '退款'],    #
         '安全问题错误':['安全', '啊啊啊', '问题', '错误', '登录', '答案', '进不去', '验证码', '无法', '游戏'],    #
         '安装不了上号器':['不了', '号器', '啊啊啊', '游戏', '安装', '退款', '登录', '电脑', '下载', '无法'],    #
         '客服仲裁错误':['撤单', '租客', '密码', '客服', '游戏', '恶意', '账号', '投诉', '登录', '账号密码'],    #
         '提示有外挂残留':['外挂', '残留', '小黑', '提示', '啊啊啊', '不了', '游戏', '排位', '退款', '登录'],    #
         '无法下载上号器':['号器', '不了', '下载', '啊啊啊', '退款', '游戏', '无法', '登录', '电脑', '登不上'],
         '无法登陆（非密码错误问题）':['登录', '游戏', '啊啊啊', '不了', '无法', '登陆', '退款', '上去', '登不上', '不上'],
         '游戏账号未实名认证':['实名', '认证', '不了', '游戏', '验证', '啊啊啊', '防沉迷', '时间', '退款', '邮箱'],
         '租客违规操作':['租客', '挂机', '排位', '信誉', '扣分', '恶意', 'QQ', '客服', '账号', '游戏'],
         '租方开外挂':['租客', '挂机', '恶意', '撤单', '信誉', '账号', '游戏', '密码', '排位', '扣分'],
         '租方打排位':['排位', '租客', '挂机', '租方', '信誉', '恶意', '排位赛', '扣分', '撤单', '违规'],
         '租错号了':['错号', '租错', '啊啊啊', '不好意思', '排位', '退款', '组错', '谢谢', '段位', '对不起'],
         '自己的号要撤销':['测试', '租客', '下架', '排位', '密码', '账号', '挂机', '冻结', '自己', '撤单'],
         '自己要玩':['自己', '不好意思', '下架', '租客', '玩要', '抱歉', '号主', '撤单', '我要', '谢谢'],
         '被挤号（顶号）了':['挤', '游戏', '有人', '退款', '啊啊啊', '一直', '退钱', '不了', '登录', '账号'],
         '裁决之廉':['排位', '啊啊啊', '裁决', '不了', '信誉', '竞技', '冷却', '游戏', '退款', '匹配'],
         '账号密码错误':['密码', '错误', '账号密码', '登录', '啊啊啊', '退款', '登不上', '上去', '帐号密码', '不了'],
         '账号描述/段位不符':[ '段位', '排位', '描述', '账号', '没有', '不符', '符合'],
         '账号禁赛':['禁赛', '账号', '啊啊啊', '不了', '封号', '10', '游戏', '下线', '时间', '健康'],
         '账号被封':['挂机', '租客', '信誉', '账号', '扣分', '恶意', '积分', '导致', '游戏', '禁赛'],
        '通过篡改上号器文件恶意破解,错误代码：1008 By 上号器':['错误代码', '号器', '1008', '篡改', '破解', '恶意', '文件', '通过'],
}


# def jud_reason(word2):
#     print(word2)
#     result = pd.DataFrame()
#     for i in reason_weight.keys():
#         word1 = reason_weight[i]
#         try:
#             score = client.simnet(word1,word2).get('score', 0.0)
#             data = pd.DataFrame({'id_name': [i],
#                              'score': [score]})
#             result = pd.concat([data, result], axis=0)
#         except Exception as e:
#             score = 0.0
#     reason = result[result['score'] == result['score'].max()]
#     reason_score = str(reason.id_name[0])
#     return reason_score
#
# chedan_reason_2['lx_reasons'] = chedan_reason_2['lx_list'].apply(lambda x: jud_reason(str(x)))
# chedan_reason_2['re_reasons'] = chedan_reason_2['re_list'].apply(lambda x: jud_reason(str(x)))
#
# def jug_data(data):
#     if len(data) > 50:
#         data = ''
#     else:
#         data = data
#     return data
#
# chedan_reason_2['lx_reasons'] = chedan_reason_2['lx_reasons'].apply(lambda x:jug_data(x))
# chedan_reason_2['re_reasons'] = chedan_reason_2['re_reasons'].apply(lambda x:jug_data(x))
#
#
#
# chedan_reason_2.ix[:,['捕获时段', '货架号', '是否限时货架', '撤单量（12小时）', '订单量（12小时）', '真实撤单比','lx_reasons','re_reasons']].to_csv('chedan_reason_2.csv', index=False, encoding='utf_8_sig')
#
# cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
# A = chedan_reason_2.ix[:,['捕获时段', '货架号', '是否限时货架', '撤单量（12小时）', '订单量（12小时）', '真实撤单比','lx_reasons','re_reasons']]
# A.to_sql(name='cd_reason_day_hourse', con=cnx, if_exists = 'replace', index=False)



def get_word_vector(s1,s2):
    """
    :param s1: 句子1
    :param s2: 句子2
    :return: 返回句子的余弦相似度
    """
    # 分词
    s1 = str(s1)
    s2 = str(s2)
    cut1 = jieba.cut(s1)
    cut2 = jieba.cut(s2)
    list_word1 = (','.join(cut1)).split(',')
    list_word2 = (','.join(cut2)).split(',')

    # 列出所有的词,取并集
    key_word = list(set(list_word1 + list_word2))
    # 给定形状和类型的用0填充的矩阵存储向量
    word_vector1 = np.zeros(len(key_word))
    word_vector2 = np.zeros(len(key_word))

    # 计算词频
    # 依次确定向量的每个位置的值
    for i in range(len(key_word)):
        # 遍历key_word中每个词在句子中的出现次数
        for j in range(len(list_word1)):
            if key_word[i] == list_word1[j]:
                word_vector1[i] += 1
        for k in range(len(list_word2)):
            if key_word[i] == list_word2[k]:
                word_vector2[i] += 1

    # 输出向量
    print(word_vector1)
    print(word_vector2)
    return word_vector1, word_vector2

def cos_dist(vec1,vec2):
    """
    :param vec1: 向量1
    :param vec2: 向量2
    :return: 返回两个向量的余弦相似度
    """
    dist1=float(np.dot(vec1,vec2)/(np.linalg.norm(vec1)*np.linalg.norm(vec2)))
    return dist1

def filter_html(html):
    """
    :param html: html
    :return: 返回去掉html的纯净文本
    """
    dr = re.compile(r'<[^>]+>',re.S)
    dd = dr.sub('',html).strip()
    return dd

def jud_reason_yx(word2):
    word2 = str(word2)
    result = pd.DataFrame()
    for i in reason_weight.keys():
        word1 = reason_weight[i]
        vec1, vec2 = get_word_vector(word1, word2)
        sorce = cos_dist(vec1, vec2)
        if sorce >= 0:
            data = pd.DataFrame({'id_name': [i],
                                 'score': [sorce]})
            result = pd.concat([data,result],axis=0)
    reason = result[result['score'] == result['score'].max()]
    reason_name = str(reason.id_name[0])
    # reason_score = str(reason.score[0])
    return reason_name


chedan_reason_2['lx_reasons'] = chedan_reason_2['lx_list'].apply(lambda x:jud_reason_yx(x))
chedan_reason_2['re_reasons'] = chedan_reason_2['re_keywords'].apply(lambda x:jud_reason_yx(x))


def jug_data(data):
    if len(data) > 40:
        data = '原因待跟踪'
    else:
        data = data
    return data

chedan_reason_2['lx_reasons'] = chedan_reason_2['lx_reasons'].apply(lambda x:jug_data(x))
chedan_reason_2['re_reasons'] = chedan_reason_2['re_reasons'].apply(lambda x:jug_data(x))


#同一用户多条投诉（恶意用户多条投诉）
db = pymysql.connect("am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com","yunying","ck2KyZ5Gsb54tzC4","zhwdb" )
ey_people_sql = '''
SELECT hid,t1.userid,t1.huserid,count(*) as cnt from zhw_ts t1
inner join zhw_dingdan t2
on t1.did = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
and DATE_FORMAT(t,'%Y%m%d%H') BETWEEN {0} and {1}
and t1.zt = 2
AND t2.zt=3
and t2.hid <> 38
and t1.userid <> t1.huserid
GROUP BY 1,2,3
HAVING cnt >= 2
'''.format(day_now_H_Last_13,day_now_H_Last)
ey_people = pd.read_sql(ey_people_sql,con=db)
ey_people.columns = ['投诉货架','投诉用户','号主用户','连续次数']

ey_people_cnt = ey_people['投诉用户'].groupby(ey_people['投诉货架']).aggregate(lambda x:','.join(x))
hid = pd.DataFrame(ey_people_cnt.index)
userid = pd.DataFrame(ey_people_cnt.values)

data = pd.concat([hid,userid],axis=1)
data.columns = ['投诉货架','投诉用户']
#结果整合
chedan_reason_2 = pd.merge(chedan_reason_2, data, how='left',  left_on='货架号', right_on='投诉货架')
chedan_reason_2['联系电话'] = chedan_reason_2['联系电话'].fillna(value='无')
chedan_reason_2 = chedan_reason_2.fillna(value=0)

chedan_reason_2['lx_reasons'] = chedan_reason_2.apply(lambda x:str('用户发起{}多次撤单').format(x['投诉用户']) if x.lx_reasons == '账号描述/段位不符' and x['投诉用户'] != 0 else x.lx_reasons,axis=1)
chedan_reason_2['lx_reasons'] = chedan_reason_2.apply(lambda x:str('描述不符可能原因:')+x.re_reasons if x.lx_reasons == '账号描述/段位不符' and x['投诉用户'] != 0 else x.lx_reasons,axis=1)

chedan_reason_2 = pd.merge(chedan_reason_2, result, how='inner',  left_on='货架号', right_on='hid')



#游戏名称
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
game_name = pd.read_sql('''SELECT gameid,game_name from dim_game_type
''',con=cnx)
game_name.columns = ['gameid','game_name']

chedan_reason_2 = pd.merge(chedan_reason_2, game_name, how='left',  left_on='gameid', right_on='gameid')
chedan_reason_2 = pd.merge(chedan_reason_2, hid_cnt, how='left',  left_on='游戏账号', right_on='游戏账号')

jkx_userphone = '''
SELECT jkx_userphone from zhw_ts t1
inner join zhw_dingdan t2
on t1.did = t2.id
where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN {0} and {1}
and DATE_FORMAT(t,'%Y%m%d%H') BETWEEN {0} and {1}
and t1.zt = 2
AND t2.zt=3
and t2.hid <> 38
and t1.userid <> t1.huserid
GROUP BY 1,2,3
HAVING cnt >= 2
'''.format(day_now_H_Last_13,day_now_H_Last)
ey_people = pd.read_sql(ey_people_sql,con=db)
ey_people.columns = ['投诉货架','投诉用户','号主用户','连续次数']


chedan_reason_2['持续异常周期（小时）'] = chedan_reason_2.apply(lambda x:int(x['持续异常周期'])+1 if x['持续异常周期'] >= 0 else 0,axis=1)
print(chedan_reason_2.head(5))

chedan_reason_2.drop_duplicates(subset=['hid'],keep='first',inplace=True)

cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
A = chedan_reason_2.ix[:,['捕获时段','game_name','货架号','用户名','游戏账号','是否限时货架', '撤单量（12小时）', '订单量（12小时）', '真实撤单比','lx_reasons','持续异常周期（小时）']]
print('数据提取完毕')
A.columns = ['捕获时段','游戏名称', '货架号','货架账号','游戏账号', '是否限时货架', '撤单量（一周期）', '订单量（一周期）', '真实撤单比','核心原因','持续异常周期（小时）']
A.to_csv("/usr/model/zhw_product/anay/{}点高危货架.csv".format(day_now_H), index=False, encoding='utf_8_sig')
print('csv文件保存完毕')
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
A.to_sql(name='cd_reason_day_12H', con=cnx, if_exists = 'append', index=False)
print('SQL写入完毕')



report = '''
SELECT t1.`捕获时段`,t1.`游戏名称`,t1.`核心原因` from 
(SELECT `捕获时段`,`游戏名称`,`核心原因`,count(*) as now_cnt from cd_reason_day_12h
where `捕获时段` = {}
GROUP BY 1,2,3)t1
left join
(SELECT `捕获时段`,`游戏名称`,`核心原因`,count(*) as last_cnt from cd_reason_day_12h
where `捕获时段` = {}
GROUP BY 1,2,3)t2
on t1.`游戏名称`=t2.`游戏名称` and t1.`核心原因` = t2.`核心原因`
where now_cnt >10
and now_cnt / last_cnt >= 2
'''.format(day_now_H_Last,day_now_H)
report = pd.read_sql(report,con=cnx)


cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)
report.to_sql(name='cd_reason_day_12H_log', con=cnx, if_exists = 'append', index=False)
print('预警日志已经写入')




now_H = (datetime.datetime.now()).strftime('%H') #h
#钉钉邮件提醒
import yagmail
if now_H == '04' or now_H == '12' or now_H == '20'or len(A) > 600:
    yag = yagmail.SMTP(user='2736187789@qq.com', password='arwzonfgbfufdgdb', host='smtp.qq.com')
    subject = '{}点高危撤单用户通报(共计{}条)'.format(day_now_H,len(A))
    user = ['j3n_h3lxlpb1h@dingtalk.com', 'lizhengyuan7703@dingtalk.com', 'sjjd10756@dingtalk.com','zhengpengwei1508@dingtalk.com']
    yag.send(to=user, subject=subject, contents='当前时间段高危撤单货架提醒，请及时处理',
             attachments=["/usr/model/zhw_product/anay/{}点高危货架.csv".format(day_now_H)])
print('钉邮推送判断完毕')


