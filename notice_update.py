# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ts_data
from realtime_package import rt_163, rt_bl, rt_ana, rt_east, notice_watcher

from functions import *
from conf import conf_handler, xl_handler


# 从配置文件 rt_analyer.conf 读取参数，初始化
# h_conf = conf_handler(conf="rt_analyer.conf")
# rt_delay = int(h_conf.rd_opt('general', 'rt_delay'))

# 读取 accounts.xlsx
xl_acc = xl_handler(f_name="accounts.xlsx")
xl_acc.rd_accounts_file()
id_arr = xl_acc.get_stock_id_from_conf()

#读取 keywords.xlsx
xl_keywords = xl_handler(f_name="keywords.xlsx")
keywords_arr = xl_keywords.rd_keywords_file()


default_date_arr = [(datetime.today() + timedelta(days=-1)).strftime('%Y-%m-%d'),
                    (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')]

max_start_date_str = (datetime.today() + timedelta(days=-30)).strftime('%Y-%m-%d')

start_date_str = input("设置开始日期（如:"+max_start_date_str+"）,默认开始日期["+default_date_arr[0]+"]回车确认：")
if len(start_date_str) > 0 and len(start_date_str)<10:
    wx.info("开始日期设置错误，退出")
    quit(0)
elif len(start_date_str) > 0 and start_date_str < max_start_date_str:
    wx.info("开始日期距今天不能超过30天，即[{}]".format(max_start_date_str))
    quit(0)
elif len(start_date_str) == 10:
    default_date_arr[0] = start_date_str

notice_process(id_arr=id_arr, keywords_arr=keywords_arr, date_arr=default_date_arr)

