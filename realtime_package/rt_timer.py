from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import re
from datetime import time, date, timedelta
import datetime
import pandas as pd
import time
from tushare_data import ts_data

class wx_timer():
    def __init__(self):
        pass

    # 判断 某个日期 是否交易日, date 默认是当日
    def is_trading_date(self, date_str=''):
        ts = ts_data()
        if date_str == '':
            date_str = (date.today()).strftime('%Y%m%d')

        if ts.is_open(date_str=date_str):
            return True
        else:
            return False

    def is_trading_time(self):
        date_str = (date.today()).strftime('%Y-%m-%d')
        now_time = datetime.datetime.now()
        m_s_time = datetime.datetime.strptime(date_str+" 09:30:00", '%Y-%m-%d %H:%M:%S')
        m_e_time = datetime.datetime.strptime(date_str+" 11:30:00", '%Y-%m-%d %H:%M:%S')
        n_s_time = datetime.datetime.strptime(date_str+" 13:00:00", '%Y-%m-%d %H:%M:%S')
        n_e_time = datetime.datetime.strptime(date_str+" 15:00:00", '%Y-%m-%d %H:%M:%S')
        if now_time <= m_e_time and now_time >= m_s_time :
            return True
        elif now_time <= n_e_time and now_time >= n_s_time:
            return True
        else:
            return False