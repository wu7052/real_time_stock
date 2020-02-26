# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ts_data
from realtime_package import rt_163, rt_bl, rt_ana, rt_east, notice_watcher

# import pandas as pd
# from assess_package import back_trader
from functions import *
from conf import conf_handler, xl_handler


# 从配置文件 rt_analyer.conf 读取参数，初始化
h_conf = conf_handler(conf="rt_analyer.conf")
rt_delay = int(h_conf.rd_opt('general', 'rt_delay'))

# 读取 accounts.xlsx
xl_acc = xl_handler(f_name="accounts.xlsx")
xl_acc.rd_accounts_file()
id_arr = xl_acc.get_stock_id_from_conf()

#读取 keywords.xlsx
xl_keywords = xl_handler(f_name="keywords.xlsx")
keywords_arr = xl_keywords.rd_keywords_file()

notice_process(id_arr=id_arr, keywords_arr=keywords_arr, date_arr=None)

