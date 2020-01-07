from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import re
from datetime import date, timedelta
import datetime
import time
import pandas as pd
import time
from tushare_data import ts_data

class wx_timer():
    def __init__(self):
        date_str = (date.today()).strftime('%Y%m%d')

        self.m_s_time = int(time.mktime(time.strptime(date_str + " 09:30:00", '%Y%m%d %H:%M:%S')))
        self.m_e_time = int(time.mktime(time.strptime(date_str + " 11:30:00", '%Y%m%d %H:%M:%S')))
        self.n_s_time = int(time.mktime(time.strptime(date_str + " 13:00:00", '%Y%m%d %H:%M:%S')))
        self.n_e_time = int(time.mktime(time.strptime(date_str + " 15:00:00", '%Y%m%d %H:%M:%S')))

        # self.m_s_time = datetime.datetime.strptime(date_str + " 09:30:00", '%Y-%m-%d %H:%M:%S')
        # self.m_e_time = datetime.datetime.strptime(date_str + " 11:30:00", '%Y-%m-%d %H:%M:%S')
        # self.n_s_time = datetime.datetime.strptime(date_str + " 13:00:00", '%Y-%m-%d %H:%M:%S')
        # self.n_e_time = datetime.datetime.strptime(date_str + " 15:00:00", '%Y-%m-%d %H:%M:%S')


    # 判断 某个日期 是否交易日, date 默认是当日
    def is_trading_date(self, date_str=''):
        ts = ts_data()
        if date_str == '':
            date_str = (date.today()).strftime('%Y%m%d')

        if ts.is_open(date_str=date_str):
            return True
        else:
            return False

    def is_trading_time(self, t_stamp = 0):
        # now_time = datetime.datetime.now()
        now_time = int(time.time())
        if now_time <= self.m_e_time and now_time >= self.m_s_time :
            return True
        elif now_time <= self.n_e_time and now_time >= self.n_s_time:
            return True
        else:
            return False

    # 9:30之前，返回 [-1, 9:30]
    # 9:30 - 11:30 , 返回 [2, 原时间]
    # 11:30  - 13:00 之间，返回 [-3, 13:00]
    # 13:00 - 15:00 之间，返回 [4, 原时间]
    # 15:00 之后，返回 [-5, 15:00]
    def tell_time_zone(self, t_stamp = 0):
        if t_stamp ==0 :
            return [0,0]

        if t_stamp < self.m_s_time:
            return [-1, self.m_s_time]
        elif t_stamp >= self.m_s_time and t_stamp <= self.m_e_time:
            return [2,t_stamp]
        elif t_stamp > self.m_e_time and t_stamp < self.n_s_time:
            return [-3, self.n_s_time]
        elif t_stamp >= self.n_s_time and t_stamp <= self.n_e_time:
            return [4, t_stamp]
        elif t_stamp > self.n_e_time:
            return [-5, self.n_e_time]
