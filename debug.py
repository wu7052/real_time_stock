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

print("done")
# 实时交易对象
# rt163 = rt_163(id_arr=id_arr, date_str='')

# rteast = rt_east(id_arr=id_arr, date_str='', item_page = '144')

# rt实时对象，src 数据源
# 利用全局RT 对象完成 数据收集
# 创建BL 对象完成 基线设定、导入数据库
# 从 163 获得数据
# rebase_rt_data(rt=rt163, src='163', date_str = '')
# 从 eastmoney 获得数据
# rebase_rt_data(rt=rteast, src='east', date_str = '')

# 基线数据对象
# bl = rt_bl()

# 从数据库中统计过去三天 量价基线
# pa_bl_df = bl.get_baseline_pa(days=3)

# 从数据库中统计 过去三天 大单交易、内外盘的基线值
# 涵盖所有监控的股票
# 废弃函数，暂不需要 大单对比，直接体现在 BI图形
# big_bl_df = bl.get_baseline_big_deal(days=1)

# 开始获得 实时交易数据，并保存到 RT 实时交易对象中
# 从记录文件中读取 开始时间，当前时间作为截止时间
# begin_time_stamp = get_rt_data(rt=rteast, src='east', date_str='')
# if begin_time_stamp != False:
#     ana_rt_data(rt=rteast, begin_time_stamp=begin_time_stamp, big_bl_df=None, pa_bl_df=None, date_str= '')
#     ana_rt_data(rt=rt163, big_bl_df=big_bl_df, pa_bl_df=pa_bl_df)
#

# rt163.clr_rt_data(minutes=0)
# print("done")

# url = rt_163.url_encode("14:00:00")
# json_str = rt_163.get_json_str(id='600026')
# rt_df = rt_163.json_parse(json_str = json_str)
# print(rt_df)
# str_type = chardet.detect("10%3A13%3A34")
# unicode = lg.str_decode("10%3A13%3A34", str_type['encoding'])
# print(unicode)


# 读取 accounts.xlsx
# xl_h = xl_handler(f_name="accounts.xlsx")
# df = xl_h.rd_accounts_file()
# print(df)