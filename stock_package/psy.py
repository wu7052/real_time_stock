from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import re
import numpy as np
import pandas as pd

class psy_kits(object):
    def __init__(self):
        # period is the length of days in which we look at how many days witness price increase
        self.h_conf = conf_handler(conf="stock_analyer.conf")
        self.period = int(self.h_conf.rd_opt('psy', 'period'))

        self.tname_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
        self.tname_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
        self.tname_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
        self.tname_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')

        host = self.h_conf.rd_opt('db', 'host')
        database = self.h_conf.rd_opt('db', 'database')
        user = self.h_conf.rd_opt('db', 'user')
        pwd = self.h_conf.rd_opt('db', 'pwd')
        self.db = db_ops(host=host, db=database, user=user, pwd=pwd)

    def get_cprice(self, stock_id):
        wx = lg.get_handle()
        tname_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
        tname_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
        tname_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
        tname_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')

        if re.match('002',stock_id) is not None:
            t_name = tname_002
        elif  re.match('00', stock_id) is not None :
            t_name = tname_00
        elif re.match('30', stock_id) is not None:
            t_name = tname_30
        elif  re.match('60', stock_id) is not None :
            t_name = tname_60
        else:
            wx.info ("[Class psy_kits: calc] failed to identify the Stock_id {}".format(stock_id))
            return None

        sql = "select id, date, close from " + t_name + " where id = " + stock_id + " order by date desc limit 60"
        df_cprice = self.db._exec_sql(sql)
        df_cprice.sort_values(by='date', ascending=True, inplace=True)
        np_cprice = np.array(df_cprice)
        return np_cprice

    def calc(self, np_cprice = None, fresh = False):
        wx = lg.get_handle()
        if np_cprice is None:
            wx.info("[Class PSY_kits Calc] np_cprice is Empty, Wrong & Return")
            return None
        #rolling calculation of the PSY, psychological line
        #np_cprice should be the Date + CLose Price , in np format
        #priceData should be the close price data, in np format
        priceData = np_cprice[...,2]
        difference = priceData[1:] - priceData[:-1]
        #to make the length of the difference same as the priceData, lag of one day
        #to avoid the warning, use 0 instead of np.nan, the result should be the same
        difference = np.append(0, difference)
        difference_dir = np.where(difference > 0, 1, 0)
        #get the direction of the price change, if increase, 1, else 0
        psy = np.zeros((len(priceData),))
        psy[:self.period] *= np.nan
        #there are two kind of lags here, the lag of the price change and the lag of the period
        for i in range(self.period, len(priceData)):
            psy[i] = int(100*(difference_dir[i-self.period+1:i+1].sum()) / self.period)
            #definition of the psy: the number of the price increases to the total number of days
        np_cprice[...,2] = psy
        df_psy = pd.DataFrame(np_cprice)
        df_psy.dropna(axis=0, how="any", inplace=True)
        if fresh == False:
            df_psy = df_psy.iloc[-1:]  # 选取DataFrame最后一行，返回的是DataFrame
        return df_psy


    def calc_arr(self, stock_arr = None, fresh = False):
        wx = lg.get_handle()
        if stock_arr is None:
            wx.info("[Class PSY_kits Calc_arr] stock_arr is Empty, Wrong & Return")
            return None

        if re.match('002', stock_arr[0][0]) is not None:
            t_name = self.tname_002
        elif re.match('00', stock_arr[0][0]) is not None:
            t_name = self.tname_00
        elif re.match('30', stock_arr[0][0]) is not None:
            t_name = self.tname_30
        elif re.match('60', stock_arr[0][0]) is not None:
            t_name = self.tname_60
        else:
            wx.info("[Class PSY_kits: Calc_arr] failed to identify the Stock_id {}".format(stock_arr[0][0]))
            return None