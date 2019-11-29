import pandas as pd
import pymysql
import datetime
import time
from sqlalchemy import create_engine
from time import gmtime, strftime


# 配置运营数据库地址
zhwdb = pymysql.connect(host = "am-2ze6pi5ns41pp7e5x90650.ads.aliyuncs.com",user = "yunying",passwd = "Mo2O68koWe3UVjn3",db = "zhwdb" )

# 日期约束
# 更新日期，时间戳
today = datetime.date.today()
first = today.replace(day=1)
month_now = first.strftime("%Y%m") #当前月
lastMonth = first - datetime.timedelta(days=1)
now_time = datetime.datetime.now()
Now_Date = now_time.strftime("%Y-%m-%d %H:%M:%S") #当前时间
#可以手动也可以设置
# month_m_last=lastMonth.strftime("%Y%m") #上个月
month_m_last = '201908' # 收到输入时间参数


# d_time = db.colim(db.DateTime,bullable= False)
# d_time = time.strftime("%Y%m%d%H", time.localtime())


# 账户标签数据抽取

# hlm_complaints – 用户--投诉
ulm_complaints = pd.read_sql ( '''
   SELECT s1.date,s1.hao_id,s1.lx,s1.lxnumber,s2.tsnumber,round(s1.lxnumber/s2.tsnumber,2) as rate
FROM(
SELECT  DATE_FORMAT(zhw_dingdan.add_time,'%Y%m') as date,zhw_dingdan.hid AS hao_id,COUNT( ts.did ) AS lxnumber,
case
when  ts.lx in('不想玩了或其它理由不玩了','租错号了')  then  '用户问题'
  when  ts.lx in('账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','复活币不足','裁决之廉','安全问题错误','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','自己的号要撤销','自己要玩','上号器自动投诉（账号密码错误）','游戏账号未实名认证','因财产密码','steam客服已冻结该帐户','会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了')  then  '账号问题'
  when  ts.lx in('使用外挂 By 上号器','通过篡改上号器文件恶意破解,错误代码：1008 By 上号器','租客违规操作','租方打排位','租方开外挂','上号器自动投诉（使用外挂）','提示有外挂残留','250','ZD主动防御')  then  '恶意行为'
  when  ts.lx in('无法登陆（非密码错误问题）','一直云检测','无法下载上号器','不输入账号密码','安装不了上号器')  then  '平台问题'
  when  ts.lx in('客服仲裁错误')  then  '客服问题'
  else '其他' end as  lx,
count(ts.id ) as number
FROM zhw_dingdan
 JOIN zhw_ts AS ts ON ts.did = zhw_dingdan.id
WHERE  DATE_FORMAT(zhw_dingdan.add_time,'%Y%m') ={0}
GROUP BY zhw_dingdan.hid, case
when  ts.lx in('不想玩了或其它理由不玩了','租错号了')  then  '用户问题'
  when  ts.lx in('账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','复活币不足','裁决之廉','安全问题错误','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','自己的号要撤销','自己要玩','上号器自动投诉（账号密码错误）','游戏账号未实名认证','因财产密码','steam客服已冻结该帐户','会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了')  then  '账号问题'
  when  ts.lx in('使用外挂 By 上号器','通过篡改上号器文件恶意破解,错误代码：1008 By 上号器','租客违规操作','租方打排位','租方开外挂','上号器自动投诉（使用外挂）','提示有外挂残留','250','ZD主动防御')  then  '恶意行为'
  when  ts.lx in('无法登陆（非密码错误问题）','一直云检测','无法下载上号器','不输入账号密码','安装不了上号器')  then  '平台问题'
  when  ts.lx in('客服仲裁错误')  then  '客服问题'
  else '其他' end 	) s1
  JOIN (
SELECT  zhw_dingdan.hid AS hao_id, COUNT( ts.did ) tsnumber
FROM zhw_dingdan  JOIN zhw_ts AS ts ON ts.did = zhw_dingdan.id
WHERE  DATE_FORMAT(zhw_dingdan.add_time,'%Y%m') ={1}
GROUP BY zhw_dingdan.hid
ORDER BY zhw_dingdan.hid
	) s2 ON s1.hao_id = s2.hao_id
	order by s1.date,s1.hao_id,rate desc;

    '''.format(month_m_last,month_m_last),con=zhwdb)


ulm_complaints['up_time'] = Now_Date


#写入目标数据库地址
cnx = create_engine('mysql+pymysql://datawj:oKSq77hSJKMX825GFL@rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com:3306/datawj', echo=False)

# 将新建的DataFrame储存为MySQL中的数据表，不储存index列(index=False)
# if_exists:
# 1.fail:如果表存在，啥也不做
# 2.replace:如果表存在，删了表，再建立一个新表，把数据插入
# 3.append:如果表存在，把数据插入，如果表不存在创建一个表！！

ulm_complaints.to_sql(name='ulm_complaints', con=cnx, if_exists = 'append', index=False)