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
    def __init__(self, date_str=''):
        if len(date_str) == 0 :
            date_str = (date.today()).strftime('%Y%m%d')

        self.m_s_time = int(time.mktime(time.strptime(date_str + " 09:25:00", '%Y%m%d %H:%M:%S')))
        self.m_e_time = int(time.mktime(time.strptime(date_str + " 11:30:00", '%Y%m%d %H:%M:%S')))
        self.n_s_time = int(time.mktime(time.strptime(date_str + " 13:00:00", '%Y%m%d %H:%M:%S')))
        self.n_e_time = int(time.mktime(time.strptime(date_str + " 15:00:00", '%Y%m%d %H:%M:%S')))

        # get_rt_data 函数中 ，记录上一次查询RT数据的截止时间戳，即下一次查询的开始时间戳，以半小时为界限取整
        # ['09:30', '10:05', '10:35', '11:05', '13:05', '13:35', '14:05', '14:35']  #
        # self.record_stamp_arr = [int(time.mktime(time.strptime(date_str + " 14:35:00", '%Y%m%d %H:%M:%S'))),
        #                          int(time.mktime(time.strptime(date_str + " 14:05:00", '%Y%m%d %H:%M:%S'))),
        #                          int(time.mktime(time.strptime(date_str + " 13:35:00", '%Y%m%d %H:%M:%S'))),
        #                          int(time.mktime(time.strptime(date_str + " 13:05:00", '%Y%m%d %H:%M:%S'))),
        #                          int(time.mktime(time.strptime(date_str + " 11:05:00", '%Y%m%d %H:%M:%S'))),
        #                          int(time.mktime(time.strptime(date_str + " 10:35:00", '%Y%m%d %H:%M:%S'))),
        #                          int(time.mktime(time.strptime(date_str + " 10:05:00", '%Y%m%d %H:%M:%S'))),
        #                          int(time.mktime(time.strptime(date_str + " 09:30:00", '%Y%m%d %H:%M:%S')))]

        self.t_frame_dict = {'09:25': ['09:40', '09:40'], '09:40': ['09:50', '09:50'], '09:50': ['10:00', '10:00'],
                             '10:00': ['10:30', '10:30'], '10:30': ['11:00', '11:00'],
                             '11:00': ['11:30', '13:00'], '13:00': ['13:30', '13:30'], '13:30': ['14:00', '14:00'],
                             '14:00': ['14:30', '14:30'], '14:30': ['14:40', '14:40'], '14:40': ['14:50', '14:50'],
                             '14:50': ['15:00', '']}
        self.record_stamp_arr = []
        for key in self.t_frame_dict.keys():
            self.record_stamp_arr.insert(0, int(time.mktime(time.strptime(date_str + key, '%Y%m%d%H:%M'))))

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
    # 9:30 - 11:30 , 返回 [2, 原时间戳，记入文件的时间戳]
    # 11:30  - 13:00 之间，返回 [-3, 13:00，记入文件的时间戳 13:00]
    # 13:00 - 15:00 之间，返回 [4, 原时间，记入文件的时间戳 ]
    # 15:00 之后，返回 [-5, 15:00， 记入文件的时间戳 15:00]
    def tell_time_zone(self, t_stamp = 0):
        if t_stamp ==0 :
            return [0,0]
        self.record_stamp_arr.sort(reverse=True)  #降序排列

        if t_stamp < self.m_s_time:  # 早上9:25之前
            return [-1, self.m_s_time, self.m_s_time]
        elif t_stamp >= self.m_s_time and t_stamp < self.m_e_time:  # 上午交易时间
            for i in self.record_stamp_arr:
                if i > t_stamp:
                    continue
                else:
                    record_stamp = i
                    break # 找到第一个小于 当前时间戳的 整数时间
            return [2,t_stamp, record_stamp]
        elif t_stamp >= self.m_e_time and t_stamp < self.n_s_time:
            return [-3, self.n_s_time, self.n_s_time]
        elif t_stamp >= self.n_s_time and t_stamp < self.n_e_time:  # 下午交易时间
            for i in self.record_stamp_arr:
                if i > t_stamp:
                    continue
                else:
                    record_stamp = i
                    break # 找到第一个小于 当前时间戳的 整数时间
            return [4, t_stamp, record_stamp]
        elif t_stamp >= self.n_e_time:  # 下午 15:00 以后
            date_str = (date.today()).strftime('%Y%m%d')
            return [-5, self.n_e_time, int(time.mktime(time.strptime(date_str + " 15:00:00", '%Y%m%d %H:%M:%S')))]
