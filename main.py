# -*- coding: utf-8 -*-
import random
import time
import pandas as pd
from datetime import date, timedelta, datetime
import json
import re
import kvs
import logging
import shutil

###### for logging and handler##
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('debug.log')
handler.setLevel(logging.INFO)   # to log

handler2 = logging.StreamHandler()
handler2.setLevel(logging.INFO)  # to console

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(handler2)
######## logging end ###########


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 UBrowser/5.6.14087.908 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4'
}  # User-Agent only required

url = 'http://www.kvstool.com/MC/'

form = {
    'LoginFormShown': 'N',
    'Function': 'AV',  # for availability
    'From': 'PEK',
    'To': 'SHA',
    'Via': '',
    'Date_D': '07',  # Date
    'Date_M': '04',  # Month
    'Time': '',  # none for default, XX for XX:00
    'Airline': '',
    'Direct': 'Y',  # Direct Only: Y or N
    #'NoCodeshares': 'Y',  # NO Codeshares: Y or N
    'Flight': '',
    'iPhone': ''
}

#cookies = {
    #    'MC_AuthCookie': 'BXQHdnd3DwdzZhB8Ghd%2BewB5bXoOdgB8YRN7HG4CCQgxYAgJdg0EAHpsd2BvAnALcwoIHAUND0IXGXQFAwh1cAMMMAYAAwYMd3B%2FBwE%3D'
    #   'MC_AuthCookie': 'B3Z5dgQEdQEFZhULHW0JfAQPGnR4cXEIYWZ%2BbxRydHc2awh0Dgh3dgoWAhIfAn4PAnoNb3t2ezlqHwN1BXJxdXByQwUBcHEPenEJAw8%3D'
#}

cookie_fp = open('cookie.json', 'r')
cookies = json.load(cookie_fp)
cookie_fp.close()



# Auth for KVS login, obtained from Charles

# log 2017.4.3 kvs改版后不支持no codeshare， 需要改变findall规则，自行去掉共享代码； 同时采样时间调整为10+[10,30]：

# Time point: PEK - SHA
# MU:  08  14  18  22  incl FM in codeshare eg MU/FM 9018  正则式开头 两字母 空格 3-4数字，正好提出FM航班号
# CA:  08  18   没有codeshare
# HU:  08       没有codeshare
# CZ:  06  18  有codeshare from MU:  CZ/MU xxxx  正则式需要提取 开头不是/ + 两字母 空格 3-4数字，去掉MU的共享航班

# 20170404
# SHA - PEK
# MU:  08  14  18
# CA:  08  20
# HU:  08
# CZ:  08  20   大量codeshare from MU

#Note: CAAC时刻表换季时间： Last SAT in Mar, Last SUN in Oct

# 目前实现了MU和CA， HU和CZ占比不到5%，忽略 （MU可以提前从CZ共享来的部分航班，除0625第一班CZ以外）

# todo 构建配置文件

airline_time_tensor = {
    'PEKSHA': {
        'MU': ['08', '14', '18', '22'],
        'CA': ['08', '18']
    },
    'SHAPEK': {
        'MU': ['08', '14', '18'],
        'CA': ['08', '18']
    }
}

# 构建日期：
tomorrow = date.today()+timedelta(1)    #采集当前日期后一天航班
mth = str(tomorrow.month)
dy = str(tomorrow.day)
form['Date_M'] = mth if len(mth) is 2 else '0'+mth
form['Date_D'] = dy if len(dy) is 2 else '0'+dy

flight_result = []

for citypair in airline_time_tensor:
    form['From'] = citypair[0:3]
    form['To'] = citypair[3:6]

    for airline in airline_time_tensor[citypair]:
        form['Airline'] = airline
        for flight_time in airline_time_tensor[citypair][airline]:
            form['Time'] = flight_time
            time.sleep(10 + random.randint(10, 30))

            flight_result.extend(kvs.kvs_flight_data(url, headers, form, cookies))


  #去重复
flight_result_cleaned = list(set(flight_result))
flight_result_cleaned.sort(key=flight_result.index)

#保存时间
time_now = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S.%m")

# 保存文本数据
fp_name = "data/RAW-"+airline_time_tensor.keys()[0]+form['Date_M']+form['Date_D']+'-'+time_now+'.log'
fp = open(fp_name, 'w')
try:
    fp.write(json.dumps(flight_result_cleaned))
except:
    logger.exception("file "+fp_name+" writing error")
else:
    logger.info('%d results written done: %s', len(flight_result_cleaned), fp_name)
finally:
    fp.close()


# 数据结构化
# DataFrame结构：
#   yr    mt   dy    cs         fn       dp      dtm        dt          ar       atm
#   年    月    日    Callsign   航班号    出发地  出发航站楼   起飞时间     目的地    目的地航站楼
#
#   at          jet     so      ft              av
#   到达时间     机型     stop    flight time     availability
#
#   dtime
#   data collecting time

column = ['cs', 'fn', 'dp', 'dtm', 'dt', 'ar', 'atm', 'at', 'jet', 'so', 'ft', 'av']
# yr mt dy dtime统一插入
# cl = ['yr', 'mt', 'dy', 'cs', 'fn', 'dp', 'dtm', 'dt', 'ar', 'atm', 'at', 'jet', 'so', 'ft', 'av', 'dtime']
# 全部columns


data_flight_fare = []

for data in flight_result_cleaned:
    data_fare = re.findall('\w.+[\d\?\w]\Z', data)[0]   #从字符串末尾开始寻找舱位记录
    data_ff_cleaned = re.split('\s+',data.replace(data_fare,'fare').replace('/', ' '))
    #将舱位段替换成'fare', 将PEK/2中的/替换成空格,再把字符串用空字符分割
    data_ff_cleaned[9] = int(data_ff_cleaned[9])  # stop变成int
    data_ff_cleaned[11] = data_fare

    data_flight_fare.append(data_ff_cleaned)

dff_df = pd.DataFrame(data_flight_fare, columns=column)
dff_df.insert(0, 'yr', tomorrow.year)
dff_df.insert(1, 'mt', tomorrow.month)
dff_df.insert(2, 'dy', tomorrow.day)
dff_df.insert(len(dff_df.columns), 'dtime', time_now)


# 保存DF到json (追加）
df_fp_name = 'PEKSHA.json'

df = pd.read_json(df_fp_name).sort_index()
df.append(dff_df, ignore_index=True).to_json(df_fp_name)

logger.info('%d results added done: %s, total %d', len(dff_df), df_fp_name, len(df)+len(dff_df))

# 备份json
dfbak_name = "data/"+airline_time_tensor.keys()[0]+'-bak-'+str(tomorrow.year)+form['Date_M']+form['Date_D']+'.json'
if tomorrow.day % 3 is 0:
    shutil.copy(df_fp_name, dfbak_name)
    logger.info('json backup done: %s', dfbak_name)









