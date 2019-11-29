import requests
from lxml import html

# 创建 session 对象。这个对象会保存所有的登录会话请求。
session_requests = requests.session()

# 提取在登录时所使用的 csrf 标记
login_url = "https://www.zuhaowan.com/ahzhw2018/Data/newAllUserDingdanChart.html"
result = session_requests.get(login_url)

tree = html.fromstring(result.text)
authenticity_token = list(set(tree.xpath("//input[@name='csrfmiddlewaretoken']/@value")))[0]

payload = {
    "username": "<你的用户名>",
    "password": "<你的密码>",
    "csrfmiddlewaretoken": authenticity_token # 在源代码中，有一个名为 “csrfmiddlewaretoken” 的隐藏输入标签。
}
# 执行登录
result = session_requests.post(
    login_url,
    data = payload,
    headers = dict(referer=login_url)
)


# 已经登录成功了，然后从 bitbucket dashboard 页面上爬取内容。
url = 'https://bitbucket.org/dashboard/overview'
result = session_requests.get(
    url,
    headers = dict(referer = url)
)


