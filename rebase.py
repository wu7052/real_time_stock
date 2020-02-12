# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ts_data
from realtime_package import rt_163, rt_bl, rt_ana, rt_east

# import pandas as pd
# from assess_package import back_trader
from functions import *
from conf import conf_handler, xl_handler


# 从配置文件 rt_analyer.conf 读取参数，初始化
h_conf = conf_handler(conf="rt_analyer.conf")
rt_delay = int(h_conf.rd_opt('general', 'rt_delay'))

# 读取 accounts.xlsx
xl_h = xl_handler(f_name="accounts.xlsx")
xl_h.rd_file()
id_arr = xl_h.get_stock_id_from_conf()

# 实时交易对象
# rt163 = rt_163(id_arr=id_arr, date_str='')

rteast = rt_east(id_arr=id_arr, date_str='', item_page = '144')

# rt实时对象，src 数据源
# 利用全局RT 对象完成 数据收集
# 创建BL 对象完成 基线设定、导入数据库
# 从 163 获得数据
# rebase_rt_data(rt=rt163, src='163', date_str = '')
# 从 eastmoney 获得数据
rebase_rt_data(rt=rteast, src='east', date_str = '')

# 查询上证、深证的公司公告
notice_process(id_arr=id_arr, key_file='', date_arr=None)
