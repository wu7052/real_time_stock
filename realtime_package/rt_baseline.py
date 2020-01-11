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

        # 量价向量 取样聚合的 agg 的时间段
        self.rt_PA_resample_agg_secs = int(self.h_conf.rd_opt('general', 'PA_resample_agg_secs'))

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
        wx = lg.get_handle()
        if date_str is None or len(date_str) == 0:
            date_str = datetime.now().strftime("%Y%m%d")
            wx.info("[RT_BL][baseline_PA] 量价基线设立日期：{}".format(date_str))
        else:
            wx.info("[RT_BL][baseline_PA] 量价基线设立日期：{}".format(date_str))

        # 半个小时 时间段，来自 rebase_rt_data 设定
        t_frame_begin_stamp = int(time.mktime(time.strptime(date_str + time_frame_arr[0], "%Y%m%d%H:%M")))
        t_frame_end_stamp = int(time.mktime(time.strptime(date_str + time_frame_arr[1], "%Y%m%d%H:%M")))

        # 基线起止时间颠倒，互换
        if t_frame_begin_stamp > t_frame_end_stamp:
            t_frame_begin_stamp, t_frame_end_stamp = t_frame_end_stamp, t_frame_begin_stamp

        baseline_PA_df = pd.DataFrame()

        # 使用 rt_PA_resample_agg_secs 设定的时间段，聚合resample 的数据
        begin_t_stamp = t_frame_begin_stamp
        icount = (t_frame_end_stamp - t_frame_begin_stamp) // self.rt_PA_resample_agg_secs +1
        while icount > 0:
            icount = icount -1
            end_t_stamp = begin_t_stamp + self.rt_PA_resample_agg_secs
            if end_t_stamp > t_frame_end_stamp:
                end_t_stamp = t_frame_end_stamp

            agg_t_frame = [time.strftime("%H:%M", time.localtime(begin_t_stamp)),
                           time.strftime("%H:%M", time.localtime(end_t_stamp))]

            for id in rt.rt_dict_df.keys():
                id_baseline_PA_df = pd.DataFrame()
                # 增加一列 pandas 的日期格式，用来做 rolling　或 resample 的index列
                rt.rt_dict_df[id]['pd_time'] = date_str +" "+ rt.rt_dict_df[id]['time_str']
                rt.rt_dict_df[id]['pd_time'] = pd.to_datetime(rt.rt_dict_df[id]['pd_time'], format="%Y%m%d %H:%M:%S")

                wx.info("[RT_BL][baseline_PA]开始更新[{}]的数据基线[{}-{}]".format(id, time_frame_arr[0], time_frame_arr[1]))

                # 从RT 数据中筛选出 rt_PA_resample_agg_secs 时间长度 （配置文件 10分钟）的交易记录
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
                id_baseline_PA_df['pct_chg'] = id_baseline_PA_df['max_price'] / id_baseline_PA_df['min_price'] -1
                id_baseline_PA_df['pct_chg_enhanced'] = (id_baseline_PA_df['max_price'] / id_baseline_PA_df['min_price'] -1 )*500000000

                # Time(High Price) - Time (Low Price) > 0 上涨； >0 下跌, （高价时间 - 低价时间）
                id_baseline_PA_df['pct_up_down'] = rt_df['price'].resample(self.rt_PA_resample_secs ).apply(lambda x: x.idxmax()- x.idxmin()
                                                        if len(x) > 0
                                                        else (pd.to_datetime(0)-pd.to_datetime(0)))

                id_baseline_PA_df.fillna(0, inplace=True)

                # 過濾掉 成交量 == 0 & 價格變動 ==0 的 時間段記錄
                id_baseline_PA_df = id_baseline_PA_df.loc[(id_baseline_PA_df['amount'] > 0)&(id_baseline_PA_df['pct_chg_enhanced'] >0),]
                # 将 pd.datetime64 之差 转换成 float 类型，方便判断 时间切片内的涨跌
                id_baseline_PA_df['pct_up_down'] = pd.to_numeric(id_baseline_PA_df['pct_up_down'])

                # 量价向量长度
                id_baseline_PA_df['pa_vector'] = pow( pow(id_baseline_PA_df['amount'],2) + pow(id_baseline_PA_df['pct_chg_enhanced'],2),0.5)
                # 量价向量方向
                id_baseline_PA_df['pct_dir'] = id_baseline_PA_df['pct_up_down'].apply(lambda x: x/abs(x) if x != 0 else 0)
                # 量价向量角度, X轴 涨跌幅度 ， Y轴 成交金额
                # 数值越小：小金额，大幅度涨跌
                # 数值越大：大金额，小幅度涨跌
                id_baseline_PA_df['pa_angle'] = id_baseline_PA_df['amount'] / id_baseline_PA_df['pct_chg_enhanced']

                # 上涨向量，去极值后，求均值 标准差
                id_up_baseline_PA_df = id_baseline_PA_df.loc[id_baseline_PA_df['pct_up_down']>0,]
                if id_up_baseline_PA_df is not None and len(id_up_baseline_PA_df) >0:
                    id_up_baseline_PA_df = self._pa_df_(id_up_baseline_PA_df, col='pa_vector') # 去极值函数（1倍标准差范围内的向量），获得计算均值的Dataframe
                    id_up_baseline_PA_df = self._pa_df_(id_up_baseline_PA_df, col='pa_angle') # 去极值函数（1倍标准差范围内的向量），获得计算均值的Dataframe
                    id_up_baseline_PA_df = self._pa_df_(id_up_baseline_PA_df, col='amount') # 去极值函数（1倍标准差范围内的向量），获得计算均值的Dataframe
                    id_up_baseline_PA_df = self._pa_df_(id_up_baseline_PA_df, col='pct_chg') # 去极值函数（1倍标准差范围内的向量），获得计算均值的Dataframe
                    id_up_bl_PA_ave = id_up_baseline_PA_df['pa_vector'].mean() # 向量长度均值
                    # id_up_bl_PA_std = id_up_baseline_PA_df['pa_vector'].std()  # 向量长度标准差
                    id_up_bl_pct_ave = id_up_baseline_PA_df['pct_chg'].mean()  # 涨幅均值
                    id_up_bl_amount_ave = id_up_baseline_PA_df['amount'].mean()  # 涨幅对应的成交金额均值
                    id_up_bl_PA_angle_ave = id_up_baseline_PA_df['pa_angle'].mean() # 向量角度均值
                    # id_up_bl_PA_angle_std = id_up_baseline_PA_df['pa_angle'].std() # 向量角度标准差
                else:
                    id_up_bl_PA_ave = 0
                    # id_up_bl_PA_std = 0
                    id_up_bl_pct_ave = 0
                    id_up_bl_amount_ave = 0
                    id_up_bl_PA_angle_ave = 0
                    # id_up_bl_PA_angle_std = 0

                # 下跌向量，去极值后，求均值 标准差
                id_down_baseline_PA_df = id_baseline_PA_df.loc[id_baseline_PA_df['pct_up_down']<0,]
                if id_down_baseline_PA_df is not None and len(id_down_baseline_PA_df) >0:
                    id_down_baseline_PA_df = self._pa_df_(id_down_baseline_PA_df, col='pa_vector')
                    id_down_baseline_PA_df = self._pa_df_(id_down_baseline_PA_df, col='pa_angle')
                    id_down_baseline_PA_df = self._pa_df_(id_down_baseline_PA_df, col='amount')
                    id_down_baseline_PA_df = self._pa_df_(id_down_baseline_PA_df, col='pct_chg')
                    id_down_bl_PA_ave = id_down_baseline_PA_df['pa_vector'].mean()
                    # id_down_bl_PA_std = id_down_baseline_PA_df['pa_vector'].std()
                    id_down_bl_pct_ave = -1 * id_down_baseline_PA_df['pct_chg'].mean()  # 跌幅均值，转换成负数
                    id_down_bl_amount_ave = id_down_baseline_PA_df['amount'].mean()  # 跌幅对应的成交金额均值
                    id_down_bl_PA_angle_ave = -1 * id_down_baseline_PA_df['pa_angle'].mean() # 向量角度均值
                    # id_down_bl_PA_angle_std = -1 * id_down_baseline_PA_df['pa_angle'].std() # 向量角度标准差
                else:
                    id_down_bl_PA_ave = 0
                    # id_down_bl_PA_std = 0
                    id_down_bl_pct_ave = 0
                    id_down_bl_amount_ave = 0
                    id_down_bl_PA_angle_ave = 0
                    # id_down_bl_PA_angle_std = 0

                pa_baseline = {"id":id, "date":date_str,"t_frame":"-".join(agg_t_frame), "sample_time":self.rt_PA_resample_secs,
                               "up_bl_pa_ave":id_up_bl_PA_ave, #"up_bl_pa_std":id_up_bl_PA_std,
                               "up_bl_pct_ave":id_up_bl_pct_ave , "up_bl_amount_ave":id_up_bl_amount_ave ,
                               "down_bl_pa_ave":id_down_bl_PA_ave, #"down_bl_pa_std":id_down_bl_PA_std,
                               "down_bl_pct_ave": id_down_bl_pct_ave, "down_bl_amount_ave": id_down_bl_amount_ave,
                               "up_bl_pa_angle_ave":id_up_bl_PA_angle_ave,#"up_bl_pa_angle_std":id_up_bl_PA_angle_std,
                               "down_bl_pa_angle_ave":id_down_bl_PA_angle_ave}#,"down_bl_pa_angle_std":id_down_bl_PA_angle_std}

                if baseline_PA_df is None or baseline_PA_df.empty:
                    baseline_PA_df = pd.DataFrame([pa_baseline])
                else:
                    baseline_PA_df = baseline_PA_df.append(pd.DataFrame([pa_baseline]))

            # while 循环，设定下一次的开始时间戳
            begin_t_stamp = end_t_stamp
            if end_t_stamp >= t_frame_end_stamp:
                break
            # 使用rolling 滑动窗口 取样，放弃这种方式
            # id_baseline_PA_df['amount'] = rt_df['amount'].rolling('20s').sum()
            # id_baseline_PA_df['max_price_index']  = pd.rolling_max(rt_df['price'], freq='20s')#.apply(self.__pct_up_down__, raw=False)
            # id_baseline_PA_df['max_price_index']  = rt_df['price'].rolling_max('20s')#.apply(self.__pct_up_down__, raw=False)
            # id_baseline_PA_df['min_price_index']  = rt_df['price'].rolling_min('20s')#.apply(_pct_up_down_, raw=False)
            # id_baseline_PA_df['pct_up_down'] = id_baseline_PA_df.apply(self.__pct_up_down__)


        if baseline_PA_df is None or baseline_PA_df.empty:
            wx.info("[RT_BL][baseline_PA] [{}-{}] 量价基线交易数据为空，退出".format(time_frame_arr[0], time_frame_arr[1]))
            return None
        else:
            # cols = ['id', 'date', 't_frame', 'sample_time',
            #         'up_bl_pa_ave', 'up_bl_pa_std','up_bl_pa_angle_ave','up_bl_pa_angle_std','up_bl_pct_ave','up_bl_amount_ave',
            #         'down_bl_pa_ave', 'down_bl_pa_std','down_bl_pa_angle_ave','down_bl_pa_angle_std','down_bl_pct_ave','down_bl_amount_ave']

            cols = ['id', 'date', 't_frame', 'sample_time',
                    'up_bl_pa_ave', 'up_bl_pa_angle_ave', 'up_bl_pct_ave', 'up_bl_amount_ave',
                    'down_bl_pa_ave', 'down_bl_pa_angle_ave', 'down_bl_pct_ave', 'down_bl_amount_ave']

            baseline_PA_df = baseline_PA_df.loc[:, cols]
            baseline_PA_df.fillna(0, inplace=True)
            baseline_PA_df.reset_index(drop=True, inplace=True)

            #
            # wx.info("[RT_BL][baseline_PA] 去除极值前记录数[{}]".format(len(baseline_PA_df)))
            # baseline_PA_df =self._clr_extreme_data(pa_df=baseline_PA_df)
            # wx.info("[RT_BL][baseline_PA] 去除极值后记录数[{}]".format(len(baseline_PA_df)))

            wx.info("[RT_BL][baseline_PA] [{}-{}] 量价数据基线更新完毕".format(time_frame_arr[0], time_frame_arr[1]))
            return baseline_PA_df

    # 对一整天的PA数据去极值
    # 去除 量价Dataframe 的极值，返回均值范围内的DataFrame
    def _clr_extreme_data(self, pa_df=None):
        ret_df = pd.DataFrame()
        for df_each_id in pa_df.groupby(by=['id']):
            id_tmp_df = self._pa_df_(pa_df=df_each_id[1], col='up_bl_pa_ave')
            id_tmp_df = self._pa_df_(pa_df=id_tmp_df, col='down_bl_pa_ave')
            id_tmp_df = self._pa_df_(pa_df=id_tmp_df, col='up_bl_pa_angle_ave')
            id_tmp_df = self._pa_df_(pa_df=id_tmp_df, col='down_bl_pa_angle_ave')
            # id_tmp_df = self._pa_df_(pa_df=id_tmp_df, col='up_bl_pct_ave')
            # id_tmp_df = self._pa_df_(pa_df=id_tmp_df, col='down_bl_pct_ave')
            # id_tmp_df = self._pa_df_(pa_df=id_tmp_df, col='up_bl_amount_ave')
            # id_tmp_df = self._pa_df_(pa_df=id_tmp_df, col='down_bl_amount_ave')
            if ret_df is None or len(ret_df) == 0:
                ret_df = id_tmp_df
            else:
                ret_df = ret_df.append(id_tmp_df)
        return ret_df

    #  去除 量价向量的 极值，返回均值范围内的dataframe
    def _pa_df_(self, pa_df=None, col='pa_vector'):
        vec_ave = pa_df[col].mean()
        vec_std = pa_df[col].std()
        if np.isnan(vec_std):
            vec_std = 0
        vec_max = vec_ave + self.mid_constant * vec_std
        vec_min = vec_ave - self.mid_constant * vec_std
        pa_df = pa_df.loc[ (pa_df[col]<=vec_max )&(pa_df[col]>=vec_min),]
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

            # 内外盘、中性盘的数量统计、金额统计
            rt_sell_qty = rt_df.loc[rt_df["type"] == -1].shape[0]
            rt_buy_qty = rt_df.loc[rt_df["type"] == 1].shape[0]
            rt_air_qty = rt_df.loc[rt_df["type"] == 0].shape[0]
            rt_buy_amount = rt_df.loc[rt_df["type"] == 1].amount.sum()
            rt_sell_amount = rt_df.loc[rt_df["type"] == -1].amount.sum()
            rt_air_amount = rt_df.loc[rt_df["type"] == 0].amount.sum()

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
                           "amount":rt_amount, "sell_qty":rt_sell_qty, "sell_amount":rt_sell_amount,
                           "buy_qty":rt_buy_qty, "buy_amount":rt_buy_amount,
                           "air_qty":rt_air_qty, "air_amount":rt_air_amount}

            if baseline_big_deal_df is None or baseline_big_deal_df.empty:
                baseline_big_deal_df = pd.DataFrame([rt_baseline])
            else:
                baseline_big_deal_df = baseline_big_deal_df.append(pd.DataFrame([rt_baseline]))

        if baseline_big_deal_df is None or baseline_big_deal_df.empty:
            wx.info("[RT_BL][baseline_big_deal] [{}-{}] 大单基线交易数据为空，退出".format(time_frame_arr[0], time_frame_arr[1]))
            return None
        else:
            cols = ['id','date','t_frame','big_qty','big_abs_pct','big_io_pct','big_buy_pct','big_sell_pct',
                    'amount','sell_qty','sell_amount','buy_qty','buy_amount','air_qty','air_amount']
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

    def get_baseline_pa(self, days=1):
        bl_pa = self.db.get_bl_pa(days = days)
        baseline_pa = pd.DataFrame()

    def get_baseline_big_deal(self, days=3):
        wx = lg.get_handle()
        bl_df = self.db.get_bl_big_deal(days=days)
        baseline_bd_df = pd.DataFrame()
        for tmp_df in bl_df.groupby(bl_df['id']):
            # for each_id_tframe_df in each_id_df[1].groupby(by=['t_frame']):
            each_id_df = tmp_df[1].fillna(0, inplace=False)
            b_qty_ave = each_id_df['big_qty'].groupby(each_id_df['t_frame']).mean()
            b_qty_std = each_id_df['big_qty'].groupby(each_id_df['t_frame']).std()
            b_qty_max = b_qty_ave + b_qty_std
            b_qty_min = b_qty_ave - b_qty_std
            b_qty_max.name = 'b_qty_max'
            b_qty_min.name = 'b_qty_min'

            b_abs_pct_ave = each_id_df['big_abs_pct'].groupby(each_id_df['t_frame']).mean()
            b_abs_pct_std = each_id_df['big_abs_pct'].groupby(each_id_df['t_frame']).std()
            b_pct_max = b_abs_pct_ave + b_abs_pct_std
            b_pct_min = b_abs_pct_ave - b_abs_pct_std
            b_pct_max.name = 'b_pct_max'
            b_pct_min.name = 'b_pct_min'

            b_buy_pct_ave = each_id_df['big_buy_pct'].groupby(each_id_df['t_frame']).mean()
            b_buy_pct_std = each_id_df['big_buy_pct'].groupby(each_id_df['t_frame']).std()
            b_buy_pct_max = b_buy_pct_ave + b_buy_pct_std
            b_buy_pct_min = b_buy_pct_ave - b_buy_pct_std
            b_buy_pct_max.name = 'b_buy_pct_max'
            b_buy_pct_min.name = 'b_buy_pct_min'

            b_sell_pct_ave = each_id_df['big_sell_pct'].groupby(each_id_df['t_frame']).mean()
            b_sell_pct_std = each_id_df['big_sell_pct'].groupby(each_id_df['t_frame']).std()
            b_sell_pct_max = b_sell_pct_ave + b_sell_pct_std
            b_sell_pct_min = b_sell_pct_ave - b_sell_pct_std
            b_sell_pct_max.name = 'b_sell_pct_max'
            b_sell_pct_min.name = 'b_sell_pct_min'

            each_id_df = each_id_df[~(each_id_df['buy_qty'].isin([0]))]
            all_buy_qty_ave = each_id_df['buy_qty'].groupby(each_id_df['t_frame']).mean()
            all_buy_qty_std = each_id_df['buy_qty'].groupby(each_id_df['t_frame']).std()
            all_buy_qty_max = all_buy_qty_ave + all_buy_qty_std
            all_buy_qty_min = all_buy_qty_ave - all_buy_qty_std
            all_buy_qty_max.name = 'all_buy_qty_max'
            all_buy_qty_min.name = 'all_buy_qty_min'

            each_id_df = each_id_df[~(each_id_df['buy_amount'].isin([0]))]
            all_buy_amount_ave = each_id_df['buy_amount'].groupby(each_id_df['t_frame']).mean()
            all_buy_amount_std = each_id_df['buy_amount'].groupby(each_id_df['t_frame']).std()
            all_buy_amount_max = all_buy_amount_ave + all_buy_amount_std
            all_buy_amount_min = all_buy_amount_ave - all_buy_amount_std
            all_buy_amount_max.name = 'all_buy_amount_max'
            all_buy_amount_min.name = 'all_buy_amount_min'

            each_id_df = each_id_df[~(each_id_df['sell_qty'].isin([0]))]
            all_sell_qty_ave = each_id_df['sell_qty'].groupby(each_id_df['t_frame']).mean()
            all_sell_qty_std = each_id_df['sell_qty'].groupby(each_id_df['t_frame']).std()
            all_sell_qty_max = all_sell_qty_ave + all_sell_qty_std
            all_sell_qty_min = all_sell_qty_ave - all_sell_qty_std
            all_sell_qty_max.name = 'all_sell_qty_max'
            all_sell_qty_min.name = 'all_sell_qty_min'

            each_id_df = each_id_df[~(each_id_df['sell_amount'].isin([0]))]
            all_sell_amount_ave = each_id_df['sell_amount'].groupby(each_id_df['t_frame']).mean()
            all_sell_amount_std = each_id_df['sell_amount'].groupby(each_id_df['t_frame']).std()
            all_sell_amount_max = all_sell_amount_ave + all_sell_amount_std
            all_sell_amount_min = all_sell_amount_ave - all_sell_amount_std
            all_sell_amount_max.name = 'all_sell_amount_max'
            all_sell_amount_min.name = 'all_sell_amount_min'

            id_df = pd.concat([b_qty_max, b_qty_min, b_pct_max, b_pct_min, b_buy_pct_max, b_buy_pct_min,
                               b_sell_pct_max, b_sell_pct_min, all_buy_qty_max, all_buy_qty_min,
                               all_buy_amount_max, all_buy_amount_min, all_sell_qty_max, all_sell_qty_min,
                               all_sell_amount_max, all_sell_amount_min]
                                , axis=1)
            # cols = ['qty_max', 'qty_min' , 'pct_max', 'pct_min', 'buy_pct_max', 'buy_pct_min',
            #         'sell_pct_max', 'sell_pct_min', 'all_buy_qty_max', 'all_buy_qty_min',
            #         'all_buy_amount_max', 'all_buy_amount_min', 'all_sell_qty_max', 'all_sell_qty_min',
            #         'all_sell_amount_max', 'all_sell_amount_min']
            # id_df = id_df.loc[:,cols]
            id_df['id']=tmp_df[0]
            if baseline_bd_df is None or baseline_bd_df.empty:
                baseline_bd_df = id_df
            else:
                baseline_bd_df = baseline_bd_df.append(id_df)

        baseline_bd_df.reset_index(drop=False, inplace=True)
        return baseline_bd_df
