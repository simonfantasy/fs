# -*- coding: utf-8 -*-
import re
import requests
from bs4 import BeautifulSoup
import logging
import pandas as pd
import random


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
        err_msg = response_clean.text[200:]
        err_msg = re.sub('\r', '\n', err_msg)
        err_msg = re.sub('\t', '\n', err_msg)
        err_msg = re.search('.+', err_msg)
        # 20170421 added err_msg
        logger.warning('%d results added to %s - %s on %s-%s @%s \n error message:\t %s', len(result_added), form['From'],
                       form['To'], form['Date_M'], form['Date_D'], form['Time'], err_msg.group())

    return result_added


def jet_clean():

    jet = pd.read_json('PEKSHA.json').loc[:, ['cs', 'jet']].drop_duplicates()
    jet_mat = pd.read_json('jet_mat.json').sort_index()

    # todo 将jet中的新项目加入jet_mat并保存json，warning新机型，并要补充座位数
    return 0


def kvs_flight_robot(url, headers, form, cookies):
    try:
        response = requests.post(url, headers=headers, data=form, cookies=cookies)
        response.raise_for_status()
    except:
        logger.exception('[ROBOT] requests ERROR while fetching %s %s to %s on %s-%s @%s',
                         form['Airline'], form['From'], form['To'], form['Date_M'], form['Date_D'], form['Time'])
    else:
        logger.info('[ROBOT] requests done: %s %s - %s on %s-%s @default',
                    form['Airline'], form['From'], form['To'], form['Date_M'], form['Date_D'])


def robot_form():
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
        'Flight': '',
        'iPhone': ''
    }
    r_from = ['PEK', 'SIN', 'HKG', 'JFK', 'CDG', 'DXB', 'AMS', 'BKK']
    r_to = ['PVG', 'FRA', 'LHR', 'IST', 'DOH', 'NRT', 'TPE', 'ICN']
    # 2017-04-21: added robot action
    form['From'] = r_from[random.randint(0, 7)]
    form['To'] = r_to[random.randint(0, 7)]
    if random.random() > 0.5:
        form['From'], form['To'] = form['To'], form['From']
    mth = str(random.randint(1, 12))
    dy = str(random.randint(1, 28))
    form['Date_M'] = mth if len(mth) is 2 else '0' + mth
    form['Date_D'] = dy if len(dy) is 2 else '0' + dy

    return form

