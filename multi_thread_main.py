# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import threading
import time
import os
import schedule
import new_logger as lg
lg._init_()
wx = lg.get_handle()

from stock_package import ts_data
from functions import *
from conf import conf_handler, xl_handler

from realtime_package import rt_163, wx_timer, rt_ana

my_timer = wx_timer()

# 判断 今日期 是否交易日
if my_timer.is_trading_date(date_str='') :
    wx.info("今天是交易日，继续运行")
else:
    wx.info("Not Trading Day: Pause 24 Hours... ")

# 判断 是否是交易时间
if my_timer.is_trading_time():
    wx.info("现在是交易时间，继续运行")
else:
    wx.info("现在不是交易时间")

# 从配置文件 rt_analyer.conf 读取参数，初始化
h_conf = conf_handler(conf="rt_analyer.conf")
rt_delay = int(h_conf.rd_opt('general', 'rt_delay'))

# 读取 accounts.xlsx
xl_h = xl_handler(f_name="accounts.xlsx")
xl_h.rd_file()
id_arr = xl_h.get_stock_id_from_conf()


# 初始化 rt163 对象，用于爬取数据
rt163 = rt_163(id_arr=id_arr, date_str='')

# 获得过去N天的交易记录，建立基线
# rt163.get_std_PV()

# 初始化 analyzer 对象，用于数据分析
analyzer = rt_ana()


# 读取实时数据，进入 rt163 的内部变量
get_rt_data(rt=rt163, src='163')
analyzer.rt_analyzer(rt=rt163)

time.sleep(120)
get_rt_data(rt=rt163, src='163')
analyzer.rt_analyzer(rt=rt163)

# time.sleep(120)
# get_rt_data(rt=rt163, src='163')
# time.sleep(120)
# get_rt_data(rt=rt163, src='163')

# schedule.every(4).minutes.do(get_rt_data, rt=rt163, src='163')
# schedule.every(4).minutes.do(ana_rt_data, rt=rt163, ana=analyzer)

# schedule.every().hour.do(job)
# schedule.every().day.at("10:30").do(job)
# schedule.every(5).to(10).minutes.do(job)
# schedule.every().monday.do(job)
# schedule.every().wednesday.at("13:15").do(job)
# schedule.every().minute.at(":17").do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

"""
thrd_sh = threading.Thread(target=update_sh_basic_info_2,args=())
thrd_sz = threading.Thread(target=update_sz_basic_info,args=())

thrd_sh.start()
thrd_sz.start()
thrd_sh.join()
thrd_sz.join()

thrd_sw_industry_code = threading.Thread(target=update_sw_industry_into_basic_info,args=())
thrd_daily_date_from_eastmoney = threading.Thread(target = update_daily_data_from_eastmoney, args=(False,))
thrd_dgj_trading_data = threading.Thread(target = update_dgj_trading_data_from_eastmoney, args=(False,))
thrd_repo_data = threading.Thread(target = update_repo_data_from_eastmoney, args=(False,))

thrd_sw_industry_code.start()
thrd_daily_date_from_eastmoney.start()
thrd_dgj_trading_data.start()
thrd_repo_data.start()
"""