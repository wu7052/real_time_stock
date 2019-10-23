import tushare as ts
from datetime import datetime, date, timedelta
import time
import new_logger as lg
from conf import conf_handler
import re

class ts_data:
    __counter = 1
    __timer = 0
    def __init__(self):
        self.h_conf = conf_handler(conf="rt_analyer.conf")
        self.token = self.h_conf.rd_opt('tushare', 'token')
        self.api= ts.pro_api(self.token)

        # self.api= ts.pro_api('bbbbd0cec7a9a4c7f8b295c738c6d694877fab8db8f48efe9263385f')

    def basic_info(self):
        data = self.api.stock_basic(exchange='', list_status='L', fields='symbol,name,area,industry,list_date')
        # data = self.api.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        return data

    def trans_day(self):
        wx = lg.get_handle()
        today = date.today().strftime('%Y%m%d')
        yesterday = (date.today() + timedelta(days=-1)).strftime('%Y%m%d')
        wx.info("Yesterday:{} ---- Today date: {}".format(yesterday,today))
        return self.api.trade_cal(exchange='', start_date=yesterday, end_date=today)

    def acquire_factor(self, date):
        wx = lg.get_handle()
        try:
            df_factor = self.api.query('adj_factor', trade_date=date)
        except Exception as e:
            wx.info("tushare exception: {}... sleep 10 seconds, retry....".format(e))
            time.sleep(10)
            df_factor = self.api.query('adj_factor', trade_date=date)
        return df_factor

    """
    # 获取指定日期之后的交易日历
    """
    def acquire_trade_cal(self, start_date=''):
        wx = lg.get_handle()
        if start_date == '':
            wx.info("[acquire_trade_cal] 开始日期为空，错误退出")
            return None
        today = datetime.now().strftime('%Y%m%d')
        try:
            df_trade_cal = self.api.trade_cal(exchange='', start_date=start_date, end_date=today)
        except Exception as e:
            wx.info("tushare exception: {}... sleep 10 seconds, retry....".format(e))
            time.sleep(10)
            df_trade_cal = self.api.trade_cal(exchange='', start_date=start_date, end_date=today)
        return df_trade_cal

    """
    # 判断指定日期是否是交易日，返回 True/False
    """
    def is_open(self, date_str=''):
        wx = lg.get_handle()
        if date_str == '':
            wx.info("[verify_trade_date] 日期为空，错误退出")
            return None
        if len(date_str) >= 10:
            date_str = date_str[0:10]
            date_str = re.sub(r'-','',date_str)
        # today = datetime.now().strftime('%Y%m%d')
        try:
            df_trade_cal = self.api.trade_cal(exchange='', start_date=date_str, end_date=date_str)
        except Exception as e:
            wx.info("tushare exception: {}... sleep 10 seconds, retry....".format(e))
            time.sleep(10)
            df_trade_cal = self.api.trade_cal(exchange='', start_date=date_str, end_date=date_str)

        open_arr = df_trade_cal.is_open.tolist()
        if open_arr[0] == 1:
            return True
        else:
            return False




    """
    # 获取指定时间区间的 前复权数据
    """
    def acquire_qfq_period(self, id, start_date, end_date):
        wx = lg.get_handle()
        try:
            if len(id)<9:
                wx.info("[acquire_qfq_period] ID {} error ".format(id))
                return None
            qfq_df = ts.pro_bar(ts_code=id, adj='qfq', start_date=start_date, end_date=end_date)
        except Exception as e:
            wx.info("tushare exception: {}... sleep 10 seconds, retry....".format(e))
            time.sleep(10)
            qfq_df = ts.pro_bar(ts_code=id, adj='qfq', start_date=start_date, end_date=end_date)
        return qfq_df


    def acquire_daily_data(self, code, period, type ='cq'):
        wx = lg.get_handle()
        ts.set_token(self.token)
        wx.info("tushare called {} times，id: {}".format(ts_data.__counter, code))
        if (ts_data.__counter == 1):  # 第一次调用，会重置 计时器
            ts_data.__timer = time.time()
        if (ts_data.__counter >= 200): # 达到 200 次调用，需要判断与第一次调用的时间间隔
            ts_data.__counter = 0      # 重置计数器=0，下面立即调用一次 计数器+1
            wait_sec = 60 - (int)(time.time() - ts_data.__timer) # 计算时间差
            # ts_data.__timer = time.time() # 重置计时器
            if (wait_sec > 0): # 在一分钟内已经累计200次调用，需要停下等待了
                wx.info("REACH THE LIMIT, MUST WAIT ({}) SECONDS".format(wait_sec))
                time.sleep(wait_sec+2)
                # ts_data.__timer = time.time() # 不需要重置计时器，因为上面重置了 计数器，下一次调用时，会重置计时器
            else:
                # 累计到 200次 调用，用时已超过1分钟，新的一分钟 怎么计算 200 次调用呢
                ts_data.__counter = 8 * (abs(wait_sec)%60)
                # ts_data.__timer += 60 + abs(wait_sec)
                ts_data.__timer = time.time()-(abs(wait_sec)%60)
                wx.info("Called 200 times More than 60 + {} seconds. New timer start at {}".format(abs(wait_sec),ts_data.__timer))

        end_date = date.today().strftime('%Y%m%d')
        start_date = (date.today() + timedelta(days = period)).strftime('%Y%m%d')
        try:
            if type == 'cq':
                df = self.api.query('daily', ts_code=code, start_date=start_date, end_date=end_date)
            elif type == 'qfq':
                df = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_date, end_date=end_date)
        except Exception as e:
            wx.info("tushare exception: {}... sleep 60 seconds, retry....".format(e))
            time.sleep(60)
            if type == 'cq':
                df = self.api.query('daily', ts_code=code, start_date=start_date, end_date=end_date)
            elif type == 'qfq':
                df = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_date, end_date=end_date)
            # df = self.ts.query('daily', ts_code=code, start_date=start_date, end_date=end_date)
            ts_data.__timer = time.time()
            ts_data.__counter = 0
        ts_data.__counter += 1
        wx.info("tushare completed called {} times，id: {} ".format(ts_data.__counter, code))
        return df

    def acquire_daily_data_by_date(self, q_date=''):
        wx = lg.get_handle()
        dd_df = self.api.daily(trade_date=q_date)
        while dd_df is None:
            wx.info("[Tushare][acquire_daily_data_by_date] 从Tushare获取 {} 数据失败, 休眠10秒后重试 ...".format(q_date))
            time.sleep(10)
            dd_df = self.api.daily(trade_date=q_date)

        return dd_df
