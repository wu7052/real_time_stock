# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import threading
import time
import os
import new_logger as lg
lg._init_()
wx = lg.get_handle()

from stock_package import ts_data
from functions import *
from conf import conf_handler, xl_handler

from realtime_package import rt_163

# 判断 某个日期 是否交易日
if is_trading_date(date_str='') :
    wx.info("Trading Day : Start Running...")
else:
    wx.info("Not Trading Day: Pause 24 Hours... ")


# 从配置文件 rt_analyer.conf 读取参数，初始化
h_conf = conf_handler(conf="rt_analyer.conf")
rt_delay = int(h_conf.rd_opt('general', 'rt_delay'))

# 读取 accounts.xlsx
xl_h = xl_handler(f_name="accounts.xlsx")
xl_h.rd_file()
id_arr = xl_h.get_stock_id_from_conf()

# print(xl_h.all_accounts)
# print(xl_h.active_accounts)
# print(xl_h.near_expiry_accounts)

rt163 = rt_163(id_arr=id_arr, date_str=None)

# 读取实时数据，进入 rt163 的内部变量
acq_rt_data(rt=rt163, src='163', t_frame=180)

print("done")

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