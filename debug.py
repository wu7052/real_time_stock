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
# from functions import *
from conf import conf_handler, xl_handler

# 判断 某个日期 是否交易日
# ts = ts_data()
# if ts.is_open(date_str = '20191001'):
#     print('open')
# else:
#     print("Close")

rt_163 = rt_163(id_arr=['600026'], date_str='')
url = rt_163.url_encode("14:00:00")
json_str = rt_163.get_json_str(id='600026')
rt_df = rt_163.json_parse(json_str = json_str)
print(rt_df)
# str_type = chardet.detect("10%3A13%3A34")
# unicode = lg.str_decode("10%3A13%3A34", str_type['encoding'])
# print(unicode)


# 读取 accounts.xlsx
# xl_h = xl_handler(f_name="accounts.xlsx")
# df = xl_h.rd_file()
# print(df)