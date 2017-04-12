# -*- coding: utf-8 -*-
import re
import requests
from bs4 import BeautifulSoup
import logging

###### for logging and handler##
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('debug.log')
handler.setLevel(logging.INFO)  # to log

handler2 = logging.StreamHandler()
handler2.setLevel(logging.INFO)  # to console

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(handler2)
######## logging end ###########


def kvs_flight_data(url, headers, form, cookies):
    try:
        response = requests.post(url, headers=headers, data=form, cookies=cookies)
        response.raise_for_status()
    except:
        logger.exception('requests ERROR while fetching %s %s to %s on %s-%s @%s',
                         form['Airline'], form['From'], form['To'], form['Date_M'], form['Date_D'], form['Time'])
    else:
        logger.info('requests done: %s %s - %s on %s-%s @%s',
                    form['Airline'], form['From'], form['To'], form['Date_M'], form['Date_D'], form['Time'])
        response_clean = BeautifulSoup(response.text, 'lxml')

    result_added = re.findall(
        form['Airline']+'\s\d{3,4}\s+\w+/\d\s+\d{2}:\d{2}\s+\w{3}/\d\s+\d{2}:\d{2}\s+\w+/\d\s+\d{2}:\d{2}\s+.+',
        response_clean.text)

    # 直接用航空公司代号作为正则式开头，只能提取出非代码共享航班
    # \d+/\d 改成\w+/\d, 适应77W这种国航代号

    if form['Airline'] is 'MU':
        result_added.extend(re.findall(
        '[CF][ZM]\s\d{3,4}\s+\w+/\d\s+\d{2}:\d{2}\s+\w{3}/\d\s+\d{2}:\d{2}\s+\w+/\d\s+\d{2}:\d{2}\s+.+',
        response_clean.text))
    # 提取CZ和FM共享给MU的航班
    # |不知道怎么用，匹配到两边字符串就停止了，所以现在的表达式会提取出CM和FZ

    if len(result_added) > 0:
        logger.info('%d results extracted to %s %s - %s on %s-%s @%s', len(result_added), form['Airline'], form['From'],
                    form['To'],
                    form['Date_M'], form['Date_D'], form['Time'])
    else:
        logger.warning('%d results added to %s - %s on %s-%s @%s \n raw data:\n %s', len(result_added), form['From'],
                       form['To'], form['Date_M'], form['Date_D'], form['Time'], response_clean.text)

    return result_added
