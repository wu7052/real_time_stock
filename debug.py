# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
from stock_package import ts_data
from realtime_package import rt_163

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


rt163 = rt_163(id_arr=id_arr, date_str='')

# rt实时对象，src 数据源
# 利用全局RT 对象完成 数据收集
# 创建BL 对象完成 基线设定、导入数据库
traceback_rt_data(rt=rt163, src='163', date_str = '20191220')


# rt163.clr_rt_data(minutes=0)
print("done")

# url = rt_163.url_encode("14:00:00")
# json_str = rt_163.get_json_str(id='600026')
# rt_df = rt_163.json_parse(json_str = json_str)
# print(rt_df)
# str_type = chardet.detect("10%3A13%3A34")
# unicode = lg.str_decode("10%3A13%3A34", str_type['encoding'])
# print(unicode)


# 读取 accounts.xlsx
# xl_h = xl_handler(f_name="accounts.xlsx")
# df = xl_h.rd_file()
# print(df)