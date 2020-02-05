import pymysql
import new_logger as lg
import pandas as pd
from conf import conf_handler
from datetime import date
import time

class db_ops:
    wx = lg.get_handle()
    def __init__(self):
        wx = lg.get_handle()
        try:
            self.h_conf = conf_handler(conf="rt_analyer.conf")
            host = self.h_conf.rd_opt('db', 'host')
            database = self.h_conf.rd_opt('db', 'database')
            user = self.h_conf.rd_opt('db', 'user')
            pwd = self.h_conf.rd_opt('db', 'pwd')

            if pwd is None:
                wx.info("[Err DB_OP]===> {0}:{1}:{2} need password ".format(host, db, user))
                raise Exception("Password is Null")
            else:
                # self.pwd = pwd
                self.config = {
                    'host': host,
                    'user': user,
                    'password': pwd,
                    'database': database,
                    'charset': 'utf8',
                    'port': 3306  # 注意端口为int 而不是str
                }
                # self.handle = pymysql.connect(self.host, self.user, self.pwd, self.db_name)
                self.handle = pymysql.connect(**self.config)
                self.cursor = self.handle.cursor()
                # wx.info("[OBJ] db_ops : __init__ called")
        except Exception as e:
            wx.info("Err occured in DB_OP __init__{}".format(e))
            raise e


    def __del__(self):
        # wx = lg.get_handle()
        # wx.info("[OBJ] db_ops : __del__ called")
        pass

    def get_trade_date(self, back_days=1):
        self.rt_conf = conf_handler(conf="rt_analyer.conf")
        tname = self.rt_conf.rd_opt('db', 'daily_table_cq_60')
        sql = "select distinct date from "+tname+" order by date desc limit "+ str(back_days)
        df = self._exec_sql(sql = sql)
        df.sort_values(by='date',ascending=False, inplace=True)
        # 取得最后一天的日期
        trade_date = df.iloc[-1][0]
        return  trade_date

    def select_table(self, t_name=None, where="", order="", limit=100):
        wx = lg.get_handle()
        if t_name is not None:
            sql = "select * from "+t_name+" "+where+" "+order+" limit "+str(limit)
        wx.info("[select_table] {}".format(sql))
        df_ret = self._exec_sql(sql)
        return df_ret

    def _exec_sql(self, sql=None):
        wx = lg.get_handle()
        if sql is None:
            return None
        iCount = self.cursor.execute(sql)
        self.handle.commit()
        if iCount > 0:
            # wx.info("[calc days vol] acquire {} rows of result".format(iCount))
            arr_ret = self.cursor.fetchall()
            if len(arr_ret) == 0:
                wx.info("[_exec_sql] Empty Dataframe Returned : SQL {}".format(sql))
                return None
            columnDes = self.cursor.description  # 获取连接对象的描述信息
            columnNames = [columnDes[i][0] for i in range(len(columnDes))]
            df_ret = pd.DataFrame([list(i) for i in arr_ret], columns=columnNames)
            return df_ret
        else:
            wx.info("[_exec_sql] Empty Dataframe Returned : SQL {}".format(sql))
            return None

    def db_load_into_RT_BL_Big_Deal(self, df = None):
        wx = lg.get_handle()
        t_name = self.h_conf.rd_opt('db', 'rt_baseline_big')
        if df is None:
            wx.info("[db_load_into_RT_BL_Big_Deal]Err: dataframe is Empty,")
            return -1
        df_array = df.values.tolist()
        i = 0
        while i < len(df_array):
            df_array[i] = tuple(df_array[i])
            i += 1

        # cols = ['id', 'date', 't_frame', 'big_qty', 'big_abs_pct', 'big_io_pct', 'big_buy_pct', 'big_sell_pct',
        #         'amount', 'sell_qty', 'sell_amount', 'buy_qty', 'buy_amount', 'air_qty', 'air_amount',
        #         'cu_big_qty', 'cu_amount', 'cu_sell_qty', 'cu_sell_amount', 'cu_buy_qty', 'cu_buy_amount',
        #         'cu_air_qty', 'cu_air_amount']

        sql = "REPLACE INTO "+t_name+" SET id=%s, date=%s, t_frame=%s, big_qty=%s, big_abs_pct=%s, big_io_pct=%s, " \
              "big_buy_pct=%s, big_sell_pct=%s, amount=%s, sell_qty=%s, sell_amount=%s, buy_qty=%s, buy_amount=%s," \
              "air_qty=%s, air_amount=%s, cu_big_qty=%s, cu_amount=%s, cu_sell_qty=%s, cu_sell_amount=%s, " \
                                     "cu_buy_qty=%s, cu_buy_amount=%s, cu_air_qty=%s, cu_air_amount=%s"
        self.cursor.executemany(sql, df_array)
        self.handle.commit()


    # 实时交易数据 监测的超阀值记录 导入数据库
    def db_load_RT_MSG(self, df = None):
        wx = lg.get_handle()
        t_name = self.h_conf.rd_opt('db', 'rt_message')

        if df is None or df.empty:
            wx.info("[db_load_RT_MSG] 实时信息 DataFrame 为空，退出")
            return
        df_array = df.values.tolist()
        i = 0
        while i < len(df_array):
            df_array[i] = tuple(df_array[i])
            i += 1

        sql = "REPLACE INTO "+ t_name +" SET id=%s, date=%s, t_frame=%s, type=%s, msg=%s"
        self.cursor.executemany(sql, df_array)
        self.handle.commit()


    def db_load_into_RT_BL_PA(self, df = None):
        wx = lg.get_handle()
        t_name = self.h_conf.rd_opt('db', 'rt_baseline_PA')
        if df is None:
            wx.info("[db_load_into_RT_BL_PA]Err: dataframe is Empty,")
            return -1
        df_array = df.values.tolist()
        i = 0
        while i < len(df_array):
            df_array[i] = tuple(df_array[i])
            i += 1

        # cols = ['id', 'date', 't_frame', 'sample_time',
        #         'bl_pa', 'bl_pa_angle', 'bl_pct', 'bl_amount', 'bl_dir']

        sql = "REPLACE INTO "+t_name+" SET id=%s, date=%s, t_frame=%s, sample_time=%s, " \
                                    "bl_pa=%s, bl_pa_ang=%s, " \
                                    "bl_pct=%s, bl_amount=%s, bl_dir=%s"
        self.cursor.executemany(sql, df_array)
        self.handle.commit()

    def get_bl_pa(self, days=1):
        t_name = self.h_conf.rd_opt('db', 'rt_baseline_PA')
        sql = "select distinct date from "+t_name+"  order by date desc limit "+str(days)
        date_df = self._exec_sql(sql = sql)
        data_arr = date_df.date.values.tolist()
        date_str = ",".join(data_arr)
        sql = "select * from "+t_name+" where date in ("+date_str+")"
        ret_df = self._exec_sql(sql=sql)
        return ret_df

    def get_bl_big_deal(self, days=3):
        wx = lg.get_handle()
        t_name = self.h_conf.rd_opt('db', 'rt_baseline_big')
        sql = "select distinct date from "+t_name+"  order by date desc limit "+str(days)
        date_df = self._exec_sql(sql = sql)
        data_arr = date_df.date.values.tolist()
        date_str = ",".join(data_arr)

        sql = "select * from "+t_name+" where date in ("+date_str+")"
        ret_df = self._exec_sql(sql=sql)
        return ret_df

    def get_cu_big_deal_date(self, date_str='', t_frame=''):
        if date_str is None or len(date_str) == 0:
            date_str = (date.today()).strftime('%Y%m%d')
        t_name = self.h_conf.rd_opt('db', 'rt_baseline_big')
        sql = "select id, sum(big_qty) as cu_big_qty, sum(amount) as cu_amount, " \
              "sum(buy_qty) as cu_buy_qty, sum(buy_amount) as cu_buy_amount , " \
              "sum(sell_qty) as cu_sell_qty, sum(sell_amount) as cu_sell_amount " \
              "from "+ t_name+" where date= "+date_str+" and t_frame < '"+t_frame+"' group by id"
        ret_df = self._exec_sql(sql=sql)
        return ret_df
