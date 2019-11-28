from datetime import datetime
import new_logger as lg
import os
import sys
import re
import xlrd
from datetime import date, timedelta
import datetime
import pandas as pd

class xl_handler():
    def __init__(self, f_name=None):
        wx = lg.get_handle()
        wx.info("xl_handler __init__ {}".format(f_name))
        self.f_name = f_name

    def __del__(self):
        wx = lg.get_handle()
        wx.info("xl_handler __del__")

    def rd_file(self):
        def _id_convert(id):
            if re.match(r'\d+',id):
                id = '0'*(6-len(id)) + id
            else:
                id = 'xxxxxx'
            return id

        data_type = {'wechat':str,'expiry':str, 'stock_1':str, 'stock_2':str, 'stock_3':str,
                     'stock_4':str, 'stock_5':str, 'stock_6':str, 'stock_7':str, 'stock_8':str,
                     'stock_9':str, 'stock_10':str}
        df = pd.DataFrame(pd.read_excel(self.f_name, dtype=data_type))
        df['expiry'] = df['expiry'].apply(lambda x:x[0:11])
        for x in range(1, 11):
            df['stock_'+ str(x)] = df['stock_'+ str(x)].apply(_id_convert)

        self.all_accounts = df
        today = (date.today()).strftime('%Y-%m-%d')
        next_3_days = (date.today()+ timedelta(days=3)).strftime('%Y-%m-%d')
        self.active_accounts = df.loc[df.expiry >= today,]
        self.near_expiry_accounts = df.loc[(df.expiry <= next_3_days) & (df.expiry >= today) ,]

        id_acc = pd.Series()
        for x in range(1,10):
            tmp = pd.Series(self.active_accounts.wechat.values, index=self.active_accounts['stock_'+str(x)])
            # tmp.add(',')
            if len(id_acc) == 0:
                id_acc = tmp
            else:
                id_acc = tmp.append(id_acc)
        self.df_id_acc = pd.DataFrame({'id':id_acc.index, 'wechat':id_acc.values})


    # 统计需要监控的 stock id, 返回list数组
    def get_stock_id_from_conf(self):
        id_arr = list((set(self.df_id_acc.id.tolist())))
        id_arr.remove('xxxxxx')
        return id_arr

    def get_wechat_by_id(self):
        for df_each_id in self.df_id_acc.groupby(by=['id']):
            print(df_each_id)
        # self.id_acc = pd.pivot_table(df_id_acc, index=['id'], columns=['wechat'])
        # print(self.id_acc)

        """
        wb = xlrd.open_workbook(filename=self.f_name)  # 打开文件
        print(wb.sheet_names())  # 获取所有表格名字

        sheet1 = wb.sheet_by_index(0)  # 通过索引获取表格
        sheet2 = wb.sheet_by_name('accounts')  # 通过名字获取表格
        print(sheet1, sheet2)
        print(sheet1.name, sheet1.nrows, sheet1.ncols)

        rows = sheet1.row_values(2)  # 获取行内容
        cols = sheet1.col_values(3)  # 获取列内容
        print(rows)
        print(cols)

        print(sheet1.cell(1, 0).value)  # 获取表格里的内容，三种方式
        print(sheet1.cell_value(1, 0))
        print(sheet1.row(1)[0].value)
        """