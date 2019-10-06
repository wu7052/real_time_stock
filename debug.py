# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import new_logger as lg
lg._init_()
wx = lg.get_handle()
# from stock_package import ma_kits, psy_kits
# from report_package import ws_rp

# import pandas as pd
# from assess_package import back_trader
# from functions import *
from conf import conf_handler, xl_handler

xl_h = xl_handler(f_name="accounts.xlsx")
df = xl_h.rd_file()
print(df)




# wx.info("============================[update_sh_basic_info_2]上证主板基础信息更新==========================================")
# update_sh_basic_info_2()
# wx.info("============================[update_sh_basic_info_kc]科创板基础信息更新==========================================")
# update_sh_basic_info_kc()
# analysis_dgj()
# analysis_hot_industry(duration = 10)
# 按板块股票数量 比例出图
# bt = back_trader(f_date='20190130', f_b_days=-100,  f_name='filter_rules\\filter_001.conf')
# bt.clear_bt_data()
# bt.get_qfq_data()


# update_sh_basic_info_kc()
# update_sh_basic_info_2()
