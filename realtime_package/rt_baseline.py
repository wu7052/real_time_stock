from db_package import db_ops
from conf import conf_handler
import new_logger as lg
from datetime import datetime, timedelta
import pandas as pd
import time
import numpy as np
class rt_bl:
    def __init__(self):  # ,, items_page=200):  time_frame=180):
        wx = lg.get_handle()

        # 建立数据库对象
        self.db = db_ops()

        # 读取数据表名
        self.h_conf = conf_handler(conf="rt_analyer.conf")

        # 大单 金额下限
        self.rt_big_amount = float(self.h_conf.rd_opt('rt_analysis_rules', 'big_deal_amount'))

        # 量价向量 的取样resample的时间段
        self.rt_PA_resample_secs = self.h_conf.rd_opt('general', 'PA_resample_secs')

        # 去除极值的常量
        self.mid_constant = float(self.h_conf.rd_opt('general', 'mid_constant'))

        self.cq_tname_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
        self.cq_tname_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
        self.cq_tname_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
        self.cq_tname_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')
        self.cq_tname_68 = self.h_conf.rd_opt('db', 'daily_table_cq_68')

    # time_frame_arr 时间区间，建立一条 量价的数据基线
    # 量价基线内容：采样频率 20S 一次，计算量价向量长度，统计均值 和 标准差
    # time_frame_arr ['09:30','10:30'] 起止时间段

    def baseline_PA(self, rt=None, date_str=None, time_frame_arr=None):
        # def _pct_up_down_(x):
        #     if x.max == x.min:
        #         up_down = 0
        #     elif x.idxmax(axis=0) > x.idxmin(axis=0) : # 高价格 出现时间较晚，上涨
        #         up_down = 1
        #     elif x.idxmax(axis=0) < x.idxmin(axis=0): # 高价格 出现时间较早，下跌
        #         up_down = -1
        #     return pd.Series(up_down,index=['up_down'])

        wx = lg.get_handle()
        if date_str is None or len(date_str) == 0:
            date_str = datetime.now().strftime("%Y%m%d")
            wx.info("[RT_BL][baseline_PA] 量价基线设立日期：{}".format(date_str))

        else:
            wx.info("[RT_BL][baseline_PA] 量价基线设立日期：{}".format(date_str))

        begin_t_stamp = int(time.mktime(time.strptime(date_str + time_frame_arr[0], "%Y%m%d%H:%M")))
        end_t_stamp = int(time.mktime(time.strptime(date_str + time_frame_arr[1], "%Y%m%d%H:%M")))

        # 基线起止时间颠倒，互换
        if begin_t_stamp > end_t_stamp:
            begin_t_stamp, end_t_stamp = end_t_stamp, begin_t_stamp

        baseline_PA_df = pd.DataFrame()

        for id in rt.rt_dict_df.keys():
            id_baseline_PA_df = pd.DataFrame()
            # 增加一列 pandas 的日期格式，用来做 rolling
            rt.rt_dict_df[id]['pd_time'] = date_str +" "+ rt.rt_dict_df[id]['time_str']
            rt.rt_dict_df[id]['pd_time'] = pd.to_datetime(rt.rt_dict_df[id]['pd_time'], format="%Y%m%d %H:%M:%S")

            wx.info("[RT_BL][baseline_PA]开始更新[{}]的数据基线[{}-{}]".format(id, time_frame_arr[0], time_frame_arr[1]))

            # 从RT 数据中筛选出 基线时间段 内的交易记录
            rt_df = rt.rt_dict_df[id].loc[ (rt.rt_dict_df[id]['time_stamp'] >= begin_t_stamp) & ( rt.rt_dict_df[id]['time_stamp'] <= end_t_stamp)].copy()
            if rt_df is None or len(rt_df) == 0:
                wx.info("[RT_BL][baseline_PA][{}] 在[{}-{}] 空交易数据，开始处理下一支股票".format(id, time_frame_arr[0], time_frame_arr[1]))
                continue
            rt_df = rt_df.sort_values(by="time_stamp", ascending=True)
            rt_df['amount'] = rt_df['price']*rt_df['vol']
            rt_df.set_index('pd_time', inplace=True)
            rt_df['price'] = pd.to_numeric(rt_df['price'])

            id_baseline_PA_df['amount'] = rt_df['amount'].resample(self.rt_PA_resample_secs ).sum()
            id_baseline_PA_df['min_price'] = rt_df['price'].resample(self.rt_PA_resample_secs ).min()
            id_baseline_PA_df['max_price'] = rt_df['price'].resample(self.rt_PA_resample_secs ).max()
            id_baseline_PA_df['pct_chg'] = (id_baseline_PA_df['max_price'] / id_baseline_PA_df['min_price'] -1 )*100000000


            # Time(Low Price) - Time (High Price) < 0 上涨； >0 下跌
            id_baseline_PA_df['pct_up_down'] = rt_df['price'].resample(self.rt_PA_resample_secs ).apply(lambda x: x.idxmin()- x.idxmax()
                                                    if len(x) > 0
                                                    else (pd.to_datetime(0)-pd.to_datetime(0)))

            id_baseline_PA_df.fillna(0, inplace=True)

            # 過濾掉 成交量 == 0 & 價格變動 ==0 的 時間段記錄
            id_baseline_PA_df = id_baseline_PA_df.loc[(id_baseline_PA_df['amount'] > 0)&(id_baseline_PA_df['pct_chg'] >0),]

            id_baseline_PA_df['pct_up_down'] = pd.to_numeric(id_baseline_PA_df['pct_up_down'])


            id_baseline_PA_df['id'] = id
            id_baseline_PA_df['t_frame'] = "-".join(time_frame_arr)
            id_baseline_PA_df['sample_time'] = self.rt_PA_resample_secs
            # 量价向量长度
            id_baseline_PA_df['pa_vector'] = pow( pow(id_baseline_PA_df['amount'],2) + pow(id_baseline_PA_df['pct_chg'],2),0.5)
            # 量价向量方向
            id_baseline_PA_df['pct_dir'] = id_baseline_PA_df['pct_up_down'].apply(lambda x: x/abs(x) if x != 0 else 0)

            # 上涨向量
            id_up_baseline_PA_df = id_baseline_PA_df.loc[id_baseline_PA_df['pct_up_down']>0,]
            id_up_baseline_PA_df = self._pa_df_(id_up_baseline_PA_df)

            # 下跌向量
            id_down_baseline_PA_df = id_baseline_PA_df.loc[id_baseline_PA_df['pct_up_down']<0,]
            id_down_baseline_PA_df = self._pa_df_(id_down_baseline_PA_df)

            if baseline_PA_df.empty or baseline_PA_df is None:
                baseline_PA_df = id_up_baseline_PA_df
                baseline_PA_df = baseline_PA_df.append(id_down_baseline_PA_df)

            else:
                baseline_PA_df = baseline_PA_df.append(id_up_baseline_PA_df)
                baseline_PA_df = baseline_PA_df.append(id_down_baseline_PA_df)

            # 使用rolling 滑动窗口 取样，放弃这种方式
            # id_baseline_PA_df['amount'] = rt_df['amount'].rolling('20s').sum()
            # id_baseline_PA_df['max_price_index']  = pd.rolling_max(rt_df['price'], freq='20s')#.apply(self.__pct_up_down__, raw=False)
            # id_baseline_PA_df['max_price_index']  = rt_df['price'].rolling_max('20s')#.apply(self.__pct_up_down__, raw=False)
            # id_baseline_PA_df['min_price_index']  = rt_df['price'].rolling_min('20s')#.apply(_pct_up_down_, raw=False)
            # id_baseline_PA_df['pct_up_down'] = id_baseline_PA_df.apply(self.__pct_up_down__)


        return baseline_PA_df

    #  去除 量价向量的 极值，返回均值范围内的dataframe
    def _pa_df_(self, pa_df=None):
        vec_ave = pa_df['pa_vector'].mean()
        vec_std = pa_df['pa_vector'].std()
        vec_max = vec_ave + self.mid_constant * vec_std
        vec_min = vec_ave - self.mid_constant * vec_std
        pa_df = pa_df.loc[ (pa_df['pa_vector']<=vec_max )&(pa_df['pa_vector']>=vec_min),]
        return pa_df

    # time_frame_arr 时间区间，建立一条 大单交易的数据基线
    # 建立成交量、大单数量、大单金额占比 的基线数据，并导入数据库
    # time_frame_arr ['09:30','10:30'] 起止时间段
    def baseline_big_deal(self, rt=None , date_str=None, time_frame_arr=None ):
        wx = lg.get_handle()
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")
            wx.info("[RT_BL][baseline_big_deal] 大单基线设立日期：{}".format(date_str))

        else:
            wx.info("[RT_BL][baseline_big_deal] 大单基线设立日期：{}".format(date_str))

        begin_t_stamp = int(time.mktime(time.strptime(date_str+time_frame_arr[0], "%Y%m%d%H:%M")))
        end_t_stamp = int(time.mktime(time.strptime(date_str+time_frame_arr[1], "%Y%m%d%H:%M")))

        # 基线起止时间颠倒，互换
        if begin_t_stamp > end_t_stamp:
            begin_t_stamp, end_t_stamp = end_t_stamp, begin_t_stamp

        baseline_big_deal_df = pd.DataFrame()
        for id in rt.rt_dict_df.keys():
            # rt_end_time = self.rt_dict_df[id]['time_stamp'].max()
            # rt_begin_time = self.rt_dict_df[id]['time_stamp'].min()
            # rt_begin_timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rt_begin_time))
            # rt_end_timestr = time.strftime("%H:%M:%S", time.localtime(rt_end_time))

            # if begin_t_stamp < rt_begin_time or end_t_stamp > rt_end_time:
            #     wx.info("[RT_BL][baseline_big_deal] [{}] 设定的基线时间段 [{}-{}] 大于实时数据时间范围 [{}-{}],退出"
            #             .format(time_frame_arr[0], time_frame_arr[1], rt_begin_timestr, rt_end_timestr))
                # return None

            wx.info("[RT_BL][baseline_big_deal]开始更新[{}]的数据基线[{}-{}]".format(id, time_frame_arr[0], time_frame_arr[1]))

            # 从RT 数据中筛选出 基线时间段 内的交易记录
            rt_df = rt.rt_dict_df[id].loc[ (rt.rt_dict_df[id]['time_stamp'] >= begin_t_stamp) & ( rt.rt_dict_df[id]['time_stamp'] <= end_t_stamp)].copy()

            if rt_df is None or rt_df.empty:
                wx.info("[RT_BL][baseline_big_deal] [{}] 在[{}-{}]期间交易数据为空，开始处理下一支股票".format(id, time_frame_arr[0], time_frame_arr[1]))
                continue

            # ID 的所有成交量
            rt_df['amount'] = rt_df['vol'] * rt_df['price']
            rt_amount = rt_df['amount'].sum()
            rt_df['io_amount'] = rt_df['amount'] * rt_df['type']

            # 成交明细中的 大单列表
            rt_big_df = rt_df.loc[rt_df['amount'] >= self.rt_big_amount,]
            # 大单的数量
            rt_big_qty = len(rt_big_df)
            # 大单买入、卖出金额合计
            rt_big_amount_sum_abs = rt_big_df['amount'].sum()
            # 大单买入 卖出对冲后的金额
            rt_big_amount_sum_io = rt_big_df['io_amount'].sum()

            # 大单金额 占 总成交量的比例
            big_abs_amount_pct = rt_big_amount_sum_abs/rt_amount

            # 大单净买入 占 总成交量的比例
            big_io_amount_pct = rt_big_amount_sum_io / rt_amount

            # 平均每分钟的 大单买入、卖出金额
            # rt_ave_big_amount_per_min_abs = rt_big_amount_sum_abs/((rt_end_time-rt_begin_time)/60)

            # 卖盘的 大单金额
            rt_big_sell_df = rt_big_df.loc[(rt_big_df['type'] < 0),]
            rt_big_sell_amount = rt_big_sell_df['amount'].sum()
            rt_big_sell_amount_pct = rt_big_sell_amount/rt_amount

            # 买盘的 大单金额
            rt_big_buy_df = rt_big_df.loc[(rt_big_df['type'] > 0),]
            rt_big_buy_amount = rt_big_buy_df['amount'].sum()
            rt_big_buy_amount_pct = rt_big_buy_amount/rt_amount

            rt_baseline = {"id":id, "date":date_str,"t_frame":"-".join(time_frame_arr), "big_qty":rt_big_qty,
                           "big_abs_pct":big_abs_amount_pct, "big_io_pct":big_io_amount_pct,
                           "big_buy_pct":rt_big_buy_amount_pct, "big_sell_pct":rt_big_sell_amount_pct,
                           "amount":rt_amount}

            if baseline_big_deal_df is None or baseline_big_deal_df.empty:
                baseline_big_deal_df = pd.DataFrame([rt_baseline])
            else:
                baseline_big_deal_df = baseline_big_deal_df.append(pd.DataFrame([rt_baseline]))

        if baseline_big_deal_df is None or baseline_big_deal_df.empty:
            wx.info("[RT_BL][baseline_big_deal] [{}-{}] 基线交易数据为空，退出".format(time_frame_arr[0], time_frame_arr[1]))
            return None
        else:
            cols = ['id','date','t_frame','big_qty','big_abs_pct','big_io_pct','big_buy_pct','big_sell_pct','amount']
            baseline_big_deal_df = baseline_big_deal_df.loc[:,cols]
            baseline_big_deal_df.fillna(0,inplace=True)
            baseline_big_deal_df.reset_index(drop=True, inplace=True)
            wx.info("[RT_BL][baseline_big_deal] [{}-{}]数据基线更新完毕".format(time_frame_arr[0], time_frame_arr[1]))
            return baseline_big_deal_df


    def db_load_baseline_big_deal(self, df = None):
        wx = lg.get_handle()
        if df is None or df.empty:
            wx.info("[RT_BL][db_load_baseline_big_deal] 大单交易数据 DataFrame 为空，退出")
            return
        self.db.db_load_into_RT_BL_Big_Deal(df=df)
        wx.info("[RT_BL][db_load_baseline_big_deal] 大单交易数据{}条 导入数据库完成".format(len(df)))


    def db_load_baseline_PA(self, df = None):
        wx = lg.get_handle()
        if df is None or df.empty:
            wx.info("[RT_BL][db_load_baseline_PA] 量价数据 DataFrame 为空，退出")
            return
        self.db.db_load_into_RT_BL_PA(df=df)
        wx.info("[RT_BL][db_load_baseline_big_PA] 量价数据{}条 导入数据库完成".format(len(df)))



    # 获得监控股票的前 N 天的成交量数据
    def get_std_PV(self, id_arr=None):
        wx = lg.get_handle()
        all_t_name = [self.cq_tname_00, self.cq_tname_30, self.cq_tname_60, self.cq_tname_002, self.cq_tname_68]
        std_days = self.h_conf.rd_opt('general', 'std_days')

        std_date = self.db.get_trade_date(back_days= std_days)
        id_arr_str = (",".join(id_arr))
        std_df = pd.DataFrame()
        wx.info("[RT_BL][get_std_PV] 开始为 [{}支股票] 设立基线数据，以 [{}] 为基线数据统计的开始日期".
                format(len(id_arr),std_date))
        for t_name in all_t_name:
            sql = "SELECT id, date, vol, amount, pct_up_down FROM "+ t_name +" where id in ("+id_arr_str+\
                  ") and date >= "+ std_date +" order by date desc;"
            temp_df = self.db._exec_sql(sql = sql)
            if std_df.empty:
                std_df = temp_df
            else:
                std_df = std_df.append(temp_df)

        std_V_df = std_df['vol'].groupby(std_df['id']).sum()  # N天累计成交量，单位 ：100股
        std_A_df = std_df['amount'].groupby(std_df['id']).sum() # N天累计成交金额，单位：1000元
        std_days_df = std_df['date'].groupby(std_df['id']).count()  # 统计天数

        # 按天计算 0.01% 涨幅 对应的成交量，并计算N天的平均值
        std_df['A_per_pct'] = (std_df['amount']*1000) / (std_df['pct_up_down'] * 100)
        std_A_Pct_df = std_df['A_per_pct'].groupby(std_df['id']).mean()

        std_Pct_df = std_df['pct_up_down'].groupby(std_df['id']).sum() # N天价格的累计振幅

        std_PVA_df = pd.concat([std_V_df, std_A_df, std_Pct_df, std_A_Pct_df, std_days_df], axis=1)
        # std_PVA_df.reset_index(drop=False, inplace=True)

         # 平均每分钟的成交金额 单位元
        std_PVA_df['ave_A_per_Mint'] = (std_PVA_df['amount']*1000)/(std_PVA_df['date']*240)

        # 平均每分钟的成交量，单位 100股
        # 用不到这个数据，直接用成交金额就可以了
        std_PVA_df['ave_vol_per_Mint'] = std_PVA_df['vol']/(std_PVA_df['date']*240)

        #  过去N天 0.01% 振幅 的成交量 单位元
        std_PVA_df['ave_A_per_Pct'] = (std_PVA_df['amount'] * 1000) / (std_PVA_df['pct_up_down'] * 100)

        # 用过去N天的均值作为基线数据不准确，直接设为0，用当天的数据 迭代出基线数据
        # std_PVA_df['ave_A_per_Pct'] = 0

        # 保存基线数据到 内部对象，字典 和 DataFrame 两种类型
        self.std_PVA_df = std_PVA_df
        self.std_PVA_df.reset_index(drop=False, inplace=True)
        self.std_PVA_dict = std_PVA_df.to_dict(orient= 'index')
        wx.info("[RT_BL][get_std_PV] [{}支股票]的基线数据设立完毕".format(len(id_arr)))


