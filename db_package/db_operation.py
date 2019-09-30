import pymysql
import new_logger as lg
import pandas as pd

class db_ops:
    wx = lg.get_handle()
    def __init__(self, host='localhost', db='mysql', user='root', pwd=None):
        wx = lg.get_handle()
        try:
            if pwd is None:
                wx.info("[Err DB_OP]===> {0}:{1}:{2} need password ".format(host, db, user))
                raise Exception("Password is Null")
            else:
                # self.pwd = pwd
                self.config = {
                    'host': host,
                    'user': user,
                    'password': pwd,
                    'database': db,
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
