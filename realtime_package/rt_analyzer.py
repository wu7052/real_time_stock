from db_package import db_ops
import new_logger as lg
import re
from datetime import datetime, time, date, timedelta
import os
import sys
import pandas as pd
import time

class rt_ana:
    def __init__(self):
        pass

    def rt_analyzer(self, rt_dict_df = None):
        wx = lg.get_handle()
        if rt_dict_df is None:
            wx.info("[rt_ana][rt_analyzer] 实时数据字典 是空，退出分析")
            return None
        for key in rt_dict_df:
            wx.info("[rt_ana][rt_analyzer] {} : {} 条数据已输出".format(key, len(rt_dict_df[key])))
            self.output_table(dd_df=rt_dict_df[key], filename= key+'实时数据', sheet_name=key)

    def output_table(self, dd_df=None, filename='null', sheet_name=None, type='.xlsx', index=False):
        wx = lg.get_handle()
        work_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        output_path = work_path + '\\log\\'
        today = date.today().strftime('%Y%m%d')
        filename = output_path + today + "_" + filename + type
        if dd_df is None or dd_df.empty:
            wx.info("[output_file] Dataframe is empty")
        elif sheet_name is None:
            sheet_name = 'Noname'
        else:
            if type == '.xlsx':
                dd_df.to_excel(filename,index=index, sheet_name= sheet_name, float_format="%.2f", encoding='utf_8_sig')
            else:
                dd_df.to_csv(filename, index=index, encoding='utf_8_sig')