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
from sklearn import preprocessing
#------------------------参数变量区----------------------------
db = pymysql.connect("am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com","yunying","ck2KyZ5Gsb54tzC4","zhwdb" )
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
#货架提取
sql = '''
select zhw_hao.*,chedan.count_chedan,dingdan.count_dingdan,round(chedan.count_chedan/dingdan.count_dingdan,2) as rate
  FROM (select hid,count(id) as count_dingdan
  from zhwdb.zhw_dingdan
  where  gameid='17' 
  and DATE_FORMAT( zhw_dingdan.add_time,'%Y%m') >= 201909 
  GROUP BY hid) as dingdan
  left  join  (select   hid, count(id) as count_chedan
  from zhwdb.zhw_dingdan where  gameid='17' and zt='3' 
  and DATE_FORMAT( zhw_dingdan.add_time,'%Y%m') >= 201909 
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
  ,dw as 段位,  dwk as 段位框,    c as 真实出租次数,  c_rank as 显示出租次数,  pf as 皮肤数量,SUBSTRING(yx,1,3) as 英雄数量
  ,case when pmoney >= 3 and pmoney < 10 then '4'
  when pmoney >= 1.8 and pmoney < 3 then '3'
  when pmoney < 1.8 then '2' else '1' end as 租金档位, pmoney as 每小时租金,  pn as 标题,  youxi as 简要描述
  FROM zhwdb.zhw_hao 
  where gid=17
  and zt in(0,1)) as  zhw_hao
  on dingdan.hid= zhw_hao.hid
  HAVING rate < 0.15
  and 英雄数量 < '146'
  and 英雄数量 >= '10'
  and 皮肤数量 > 400
  and count_dingdan >= 10
'''
data = pd.read_sql(sql,con=db)

#搭建关键皮肤词库
# list = str(data['标题'].T.tolist())
# a = jieba.analyse.extract_tags(list, topK=50, withWeight=False, allowPOS=())
#
# c = str(data['简要描述'].T.tolist())
# b = jieba.analyse.extract_tags(c, topK=50, withWeight=False, allowPOS=())
pf_word = ['至臻','龙瞎','摄魂','斩星','海克斯','冰雪节','电玩','IG','龙虾','魔剑','穿星','龙刀','哥特','花木兰','年限','庆典','冰原','周年','黑龙','冠军','蓝龙','剑仙','全英雄']

# np.savetxt("E:/pf_word.txt", pf_word)
# file=open('E:/pf_word.txt','w')
# file.write(str(pf_word));
# file.close()
# s = jieba.analyse.extract_tags(data['标题'][2], topK=30, withWeight=False, allowPOS=())
# char = data['标题'][2]
def jug_data(data,word):
    if data.find(word) >= 0:
        result = 1
    else:
        result = 0
    return result


data['str'] = data['标题'] + data['简要描述']
for i in pf_word:
    data[i] = data['str'].apply(lambda x:jug_data(x,i))


data['等级'] = data['段位'].str.split('级 ').str[0]
data['排位段位'] = data['段位'].str.split('级 ').str[1]

new_data = data.ix[:,['hid', '区服','段位分层', '皮肤数量', '英雄数量',
       '租金档位',
                         # , 'count_chedan', 'count_dingdan', 'rate']]
       '至臻','龙瞎', '摄魂', '斩星', '海克斯', '冰雪节', '电玩', 'IG', '龙虾', '魔剑', '穿星', '龙刀',
       '哥特', '花木兰', '年限', '庆典', '冰原', '周年', '黑龙', '冠军', '蓝龙', '剑仙', '全英雄']]


data.sort_values(by="count_dingdan",ascending=False,inplace=True)
#维度编码
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder


label_encoder = LabelEncoder()
integer_encoded = label_encoder.fit_transform(new_data['区服'].astype(str))
new_data['区服_new'] = integer_encoded


# label_encoder = LabelEncoder()
# encoded = label_encoder.fit_transform(new_data['排位段位'].astype(str))
# new_data['排位段位_new'] = encoded
#
#
# label_encoder = LabelEncoder()
# encoded = label_encoder.fit_transform(new_data['段位框'].astype(str))
# new_data['段位框_new'] = encoded


data = new_data.ix[:,['hid', '区服_new','段位分层', '皮肤数量', '英雄数量',
       '租金档位','至臻', '摄魂', '斩星', '海克斯', '冰雪节', '电玩', 'IG', '龙虾', '魔剑', '穿星', '龙刀',
        '哥特', '花木兰', '年限', '庆典', '冰原', '周年', '黑龙', '冠军', '蓝龙', '剑仙', '全英雄']]

hid = data.iloc[:,0]
# qf = data.iloc[:,1]
X_train = data.iloc[:,1:]
min_max_sacler = preprocessing.MinMaxScaler()
train = min_max_sacler.fit_transform(X_train)
# train = min_max_sacler.transform(X_train)
train_pd = pd.DataFrame(train)
B_data = pd.concat([hid,train_pd],axis= 1)

# origin_data = pd.concat([hid,pd.DataFrame(min_max_sacler.inverse_transform(train_pd))],axis = 1)
# origin_data.columns = ['hid','区服_new', '排位段位_new', '段位框_new', '皮肤数量', '英雄数量', '每小时租金',
#        'count_chedan', 'count_dingdan', 'rate', '至臻', '摄魂', '斩星', '海克斯', '冰雪节',
#        '电玩', 'IG', '龙虾', '魔剑', '穿星', '龙刀', '哥特', '花木兰', '年限', '庆典', '冰原', '周年',
#        '黑龙', '冠军', '蓝龙', '剑仙', '全英雄']

def cos_dist(vec1,vec2):
    """
    :param vec1: 向量1
    :param vec2: 向量2
    :return: 返回两个向量的余弦相似度
    """
    dist1=float(np.dot(vec1,vec2)/(np.linalg.norm(vec1)*np.linalg.norm(vec2)))
    return dist1


def euclidean(p, q):
    # 如果两数据集数目不同，计算两者之间都对应有的数
    same = 0
    for i in p:
        if i in q:
            same += 1

    # 计算欧几里德距离,并将其标准化
    e = sum([(p[i] - q[i]) ** 2 for i in range(same)])
    return 1 / (1 + e ** .5)



# B_data = round(B_data,4)

# len = B_data.shape[0] + 1
# # hid = B_data.iloc[0, 0]
# hid = B_data['hid']
# s = pd.DataFrame()
# for i in hid.head(1):
#     print(i)
#     vec1 = B_data[B_data['hid'] == i].iloc[0, 1:].astype(float).values
#     h = i
#     result = pd.DataFrame()
#     for j in range(len):
#         try:
#             print(j)
#             vec2 = B_data.iloc[j,1:].astype(float).values
#             sorce = euclidean(vec1, vec2)
#             if sorce >= 0:
#                 d = pd.DataFrame({'id_name': B_data.iloc[j, 0],
#                                   'score': [sorce]})
#                 result = pd.concat([d, result], axis=0)
#         except:
#             pass
#         result.sort_values(by="score",ascending=False,inplace=True)
#         hid_list = result['id_name'].head(3).values
#         hid_tuijian = pd.DataFrame({'hid': i,
#                           'tuijian': [hid_list]})
#     s = pd.concat([s,hid_tuijian],axis=0)

def get_max_hid(vec1,data):
    vec1 = vec1
    result = pd.DataFrame()
    L = data.shape[0] + 1
    for j in range(L):
        try:
            vec2 = data.iloc[j,1:].astype(float).values
            sorce = cos_dist(vec1, vec2)
            if sorce >= 0:
                d = pd.DataFrame({'id_name': B_data.iloc[j, 0],
                                  'score': [sorce]})
                result = pd.concat([d, result], axis=0)
        except:
            pass
    result.sort_values(by="score",ascending=False,inplace=True)
    hid_list = result['id_name'].head(50).values
    return hid_list

C_data = B_data.head(500)
C_data['tj_hid_list'] = C_data.apply(lambda x: get_max_hid(x.iloc[1:],B_data),axis=1)

# C_data.to_csv('E:/tuijian.csv',index=False)

vec = B_data.iloc[0,1:].astype(float)
h = B_data.iloc[0,0].astype(float)

result.sort_values(by="score",ascending=False)

B_data['lx_reasons'] = B_data['lx_list'].apply(lambda x:jud_score(x))

#
#
# A = data.ix[:,['区服_new','排位段位_new']]
#
# df = A.drop_duplicates(subset=['区服_new','排位段位_new'],keep='first')


