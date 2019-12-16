import pymysql
import new_logger as lg
import pandas as pd
from conf import conf_handler

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
        # ['id', 'date', 't_frame', 'big_qty', 'big_abs_pct', 'big_io_pct', 'big_buy_pct', 'big_sell_pct']
        sql = "REPLACE INTO "+t_name+" SET id=%s, date=%s, t_frame=%s, big_qty=%s, big_abs_pct=%s, big_io_pct=%s, " \
              "big_buy_pct=%s, big_sell_pct=%s"
        self.cursor.executemany(sql, df_array)
        self.handle.commit()
