from db_package import db_ops
import new_logger as lg
from datetime import datetime, date, timedelta
import time
import os
import sys
from conf import conf_handler
import pandas as pd
from msg_package import f_msg


class rt_ana:
    def __init__(self):
        h_conf = conf_handler(conf="rt_analyer.conf")
        ana_time_slice = h_conf.rd_opt('rt_analysis_rules', 'ana_time_slice')
        self.ana_time_slice_arr = ana_time_slice.split(',')
        self.rt_big_amount = float(h_conf.rd_opt('rt_analysis_rules','big_deal_amount'))
        self.msg = f_msg()

        # 量价向量 的取样resample的时间段
        self.rt_PA_resample_secs = h_conf.rd_opt('general', 'PA_resample_secs')

        # 建立数据库对象
        self.db = db_ops()

        self.rt_data_keeper_1 = pd.DataFrame() # 保存 大单记录，作为基线数据

        # key:array  时间段开始时间：[时间段结束时间， 下一时间段开始时间]
        # self.t_frame_dict = {'09:30':['10:00','10:05'],'10:05':['10:30','10:35'],'10:35':['11:00','11:05'],
        #                      '11:05':['11:30','13:05'],'13:05':['13:30','13:35'],'13:35':['14:00','14:05'],
        #                      '14:05':['14:30','14:35'],'14:35':['15:00','']}
        self.t_frame_dict = {'09:25':['09:40','09:40'],'09:40':['09:50','09:50'],'09:50':['10:00','10:00'],
                             '10:00':['10:30','10:30'],'10:30':['11:00','11:00'],
                             '11:00':['11:30','13:00'],'13:00':['13:30','13:30'],'13:30':['14:00','14:00'],
                             '14:00':['14:30','14:30'],'14:30':['14:40','14:40'],'14:40':['14:50','14:50'],
                             '14:50':['15:00','']}

        date_str = (date.today()).strftime('%Y%m%d')

        # ['09:30', '10:05', '10:35', '11:05', '13:05', '13:35', '14:05', '14:35']  #
        # record_stamp_dict = {int(time.mktime(time.strptime(date_str + " 14:30:00", '%Y%m%d %H:%M:%S'))):"14:35",
        #                     int(time.mktime(time.strptime(date_str + " 14:00:00", '%Y%m%d %H:%M:%S'))):"14:05",
        #                     int(time.mktime(time.strptime(date_str + " 13:30:00", '%Y%m%d %H:%M:%S'))):"13:35",
        #                     int(time.mktime(time.strptime(date_str + " 13:00:00", '%Y%m%d %H:%M:%S'))):"13:05",
        #                     int(time.mktime(time.strptime(date_str + " 11:00:00", '%Y%m%d %H:%M:%S'))):"11:05",
        #                     int(time.mktime(time.strptime(date_str + " 10:30:00", '%Y%m%d %H:%M:%S'))):"10:35",
        #                     int(time.mktime(time.strptime(date_str + " 10:00:00", '%Y%m%d %H:%M:%S'))):"10:05",
        #                     int(time.mktime(time.strptime(date_str + " 09:25:00", '%Y%m%d %H:%M:%S'))):"09:30"}

        self.record_stamp_dict = {}
        # 用 self.t_frame_dict 的key （时间段的起始时间点）建立 record_stamp_dict 字典
        for time_str in self.t_frame_dict.keys():
            stamp = int(time.mktime(time.strptime(date_str + time_str, '%Y%m%d%H:%M')))
            self.record_stamp_dict[stamp] = time_str


    def rt_cmp_pa_baseline(self, rt=None, pa_bl_df=None):
        wx = lg.get_handle()
        rt_dict_df = rt.rt_dict_df
        date_str = (date.today()).strftime('%Y%m%d')
        if rt_dict_df is None:
            wx.info("[Rt_Ana][RT_CMP_PA_Baseline] 实时数据字典 是空，退出")
            return None
        if pa_bl_df is None:
            wx.info("[Rt_Ana][RT_CMP_PA_Baseline] 基线数据 是空，退出")
            return None
        cmp_pa_result = pd.DataFrame() # 保存最后的结果，并导入数据库
        for id in rt_dict_df.keys():
            if rt_dict_df[id] is None:
                wx.info("[Rt_Ana][RT_CMP_PA_Baseline] {} 未产生实时交易数据，进入下一支股票".format(id))
                continue

            # 起始时间边界对齐
            [frame_begin_stamp, frame_begin_time_str] = self.rt_df_find_start_stamp(rt_stamp= rt_dict_df[id]['time_stamp'].min())
            end_stamp = rt_dict_df[id].time_stamp.max()

            # 按时间段 切片rt数据，计算PA向量长度、角度、涨幅、成交金额，然后对比基线数据
            while frame_begin_stamp < end_stamp:
                # 每个时间段，初始化一次 rt_pa_df ，保存该时间段内的RT resample数据
                rt_pa_df = pd.DataFrame()

                # frame_begin_time_str = time.strftime("%H:%M", time.localtime(frame_begin_stamp))
                frame_end_time_str = self.t_frame_dict.get(frame_begin_time_str)[0]
                if frame_end_time_str is None:
                    wx.info("[Rt_Ana][RT_CMP_PA_Baseline] {} [{}] 起始时间不属于正常范围！！！！".format(id, frame_begin_time_str))
                    break

                t_frame = frame_begin_time_str + "-" + frame_end_time_str
                frame_end_stamp = int(time.mktime(time.strptime(date_str + frame_end_time_str, '%Y%m%d%H:%M')))

                #  PA 实时数据 与 基线的对比，不需要 整个时间段，随时可以对照 基线的 t_frame 数据
                # if frame_end_stamp > end_stamp:
                #     wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] {} {} 已超出本次获取的实时数据范围，进入下一支股票".format(id, t_frame))
                #     break

                # 从基线DataFrame 提取该id 对应 t_frame 的基线数据
                id_bl_df= pa_bl_df.loc[(pa_bl_df['id']==id)&(pa_bl_df['t_frame']==t_frame),]

                # 从RT DataFrame 提取该 id 对应
                if frame_end_time_str == '15:00':  # 15:00 收市后，有最后一笔交易记录 产生在 15:00 之后若干秒
                    rt_df = rt.rt_dict_df[id].loc[(rt.rt_dict_df[id]['time_stamp'] >= frame_begin_stamp)].copy()
                else:
                    rt_df = rt.rt_dict_df[id].loc[(rt.rt_dict_df[id]['time_stamp'] >= frame_begin_stamp) &
                                                  (rt.rt_dict_df[id]['time_stamp'] < frame_end_stamp)].copy()
                if rt_df is None or rt_df.empty:
                    wx.info("[Rt_Ana][RT_CMP_PA_Baseline] [{}] 在[{}]期间交易数据为空，开始处理下一支股票".format(id, t_frame))
                    break

                #
                # 开始resample RT数据，并计算 PA 向量、角度
                #
                rt_df = rt_df.sort_values(by="time_stamp", ascending=True)
                rt_df['amount'] = rt_df['price']*rt_df['vol']
                rt_df['pd_time'] = date_str +" "+ rt_df['time_str']
                rt_df['pd_time'] = pd.to_datetime(rt_df['pd_time'], format="%Y%m%d %H:%M:%S")
                rt_df.set_index('pd_time', inplace=True)
                rt_df['price'] = pd.to_numeric(rt_df['price'])

                rt_pa_df['amount'] = rt_df['amount'].resample(self.rt_PA_resample_secs ).sum()
                rt_pa_df['min_price'] = rt_df['price'].resample(self.rt_PA_resample_secs ).min()
                rt_pa_df['max_price'] = rt_df['price'].resample(self.rt_PA_resample_secs ).max()
                rt_pa_df['pct_chg'] = rt_pa_df['max_price'] / rt_pa_df['min_price'] -1
                rt_pa_df['pct_chg_enhanced'] = (rt_pa_df['max_price'] / rt_pa_df['min_price'] -1 )*500000000

                # Time(High Price) - Time (Low Price) > 0 上涨； >0 下跌, （高价时间 - 低价时间）
                rt_pa_df['pct_up_down'] = rt_df['price'].resample(self.rt_PA_resample_secs ).apply(lambda x: x.idxmax()- x.idxmin()
                                                        if len(x) > 0
                                                        else (pd.to_datetime(0)-pd.to_datetime(0)))

                rt_pa_df.fillna(0, inplace=True)

                # 過濾掉 成交量 == 0 & 價格變動 ==0 的 時間段記錄
                rt_pa_df = rt_pa_df.loc[(rt_pa_df['amount'] > 0)&(rt_pa_df['pct_chg_enhanced'] >0),]
                # 将 pd.datetime64 之差 转换成 float 类型，方便判断 时间切片内的涨跌
                rt_pa_df['pct_up_down'] = pd.to_numeric(rt_pa_df['pct_up_down'])

                # 量价向量长度
                rt_pa_df['pa_vector'] = pow( pow(rt_pa_df['amount'],2) + pow(rt_pa_df['pct_chg_enhanced'],2),0.5)
                # 量价向量方向
                rt_pa_df['pct_dir'] = rt_pa_df['pct_up_down'].apply(lambda x: x/abs(x) if x != 0 else 0)
                # 量价向量角度, X轴 涨跌幅度 ， Y轴 成交金额
                # 数值越小：小金额，大幅度涨跌
                # 数值越大：大金额，小幅度涨跌
                rt_pa_df['pa_angle'] = rt_pa_df['amount'] / rt_pa_df['pct_chg_enhanced'] * rt_pa_df['pct_dir']
                rt_pa_df['id'] = id
                rt_pa_df.reset_index(drop=False, inplace=True)
                rt_pa_df['pd_time'] = rt_pa_df['pd_time'].dt.strftime('%Y%m%d %H:%M:%S')
                rt_pa_df['date'] = rt_pa_df['pd_time'].apply(lambda x:x[0:8])
                rt_pa_df['time'] = rt_pa_df['pd_time'].apply(lambda x:x[9:])
                pa_cmp_df = pd.merge(rt_pa_df, id_bl_df, on=['id'], how='left')

                pa_cmp_df['pct_chg'] *= 100
                pa_cmp_df['b_up_pct_max'] *= 100
                pa_cmp_df['b_up_pct_min'] *= 100
                pa_cmp_df['b_down_pct_max'] *= 100
                pa_cmp_df['b_down_pct_min'] *= 100

                # 按照上涨、下跌分类
                # 开始与基线数据比对
                cmp_up_dict = {'pa_vector': ['b_up_pa_max', 'b_up_pa_min', 'PA_UP长度-[超高]-', 'PA_UP长度-[超低]-', '[PA_UP向量超长]', '[PA_UP向量超短'], # PA向量长度
                            'pa_angle': ['b_up_ang_max', 'b_up_ang_min', 'PA_UP角度-[量大幅小]-', 'PA_UP角度-[量小幅大]-', '[PA_UP量大幅小]', '[PA_UP量小幅大]'],  # PA向量角度
                            # 'amount': ['b_up_amount_max', 'b_up_amount_min', '大买单金额占比-[超高]-', '大买单金额占比-[超低]-','[B多]大买单净比高', '[B空]大买单净比低'],  # 大单买入金额占比
                            # 'pct_chg': ['b_up_pct_max', 'b_up_pct_min', '大卖单金额占比-[超高]-', '大卖单金额占比-[超低]-', '[B空]大卖单净比高', '[B多]大卖单净比低'],  # 大单卖出金额占比
                            }
                cmp_up_result_df = pd.DataFrame()
                # 筛选出上涨的Dataframe
                pa_up_cmp_df = pa_cmp_df.loc[pa_cmp_df['pct_dir']>0,]
                if pa_up_cmp_df is not None and len(pa_up_cmp_df) >0:
                    for key in cmp_up_dict.keys():
                        high_df = pa_up_cmp_df.loc[pa_up_cmp_df[key] > pa_up_cmp_df[cmp_up_dict[key][0]]]
                        high_df = self._cmp_data_process_2_(df=high_df, key=key, val=cmp_up_dict[key][0], msg=cmp_up_dict[key][2],
                                                          type=cmp_up_dict[key][4])
                        if key == 'pa_vector': # PA 向量长度短，不做异常记录
                            low_df = None
                        else:
                            low_df = pa_up_cmp_df.loc[pa_up_cmp_df[key] < pa_up_cmp_df[cmp_up_dict[key][1]]]
                            low_df = self._cmp_data_process_2_(df=low_df, key=key, val=cmp_up_dict[key][1], msg=cmp_up_dict[key][3],
                                                             type=cmp_up_dict[key][5])
                        if cmp_up_result_df is None or len(cmp_up_result_df) == 0:
                            if high_df is not None:
                                cmp_up_result_df = high_df
                                if low_df is not None:
                                    cmp_up_result_df = cmp_up_result_df.append(low_df)
                            else:
                                cmp_up_result_df = low_df
                        else:
                            if high_df is not None:
                                cmp_up_result_df = cmp_up_result_df.append(high_df)
                            if low_df is not None:
                                cmp_up_result_df = cmp_up_result_df.append(low_df)

                # 按照上涨、下跌分类
                # 开始与基线数据比对
                cmp_down_dict = { 'pa_vector': ['b_down_pa_max', 'b_down_pa_min', 'PA_DOWN长度-[超高]-', 'PA_DOWN长度-[超低]-', '[PA_DOWN向量超长]', '[PA_DOWN向量超短'], # PA向量长度
                                'pa_angle': ['b_down_ang_max', 'b_down_ang_min', 'PA_DOWN角度-[量小幅大]-', 'PA_DOWN角度-[量大幅小]-', '[PA_DOWN量小幅大]', '[PA_DOWN量大幅小'],  # PA向量角度
                    # 'amount': ['b_up_amount_max', 'b_up_amount_min', '大买单金额占比-[超高]-', '大买单金额占比-[超低]-','[B多]大买单净比高', '[B空]大买单净比低'],  # 大单买入金额占比
                    # 'pct_chg': ['b_up_pct_max', 'b_up_pct_min', '大卖单金额占比-[超高]-', '大卖单金额占比-[超低]-', '[B空]大卖单净比高', '[B多]大卖单净比低'],  # 大单卖出金额占比
                    }
                cmp_down_result_df = pd.DataFrame()
                # 筛选出下跌的Dataframe
                pa_down_cmp_df = pa_cmp_df.loc[pa_cmp_df['pct_dir'] < 0,]
                if pa_down_cmp_df is not None and len(pa_down_cmp_df) > 0:
                    for key in cmp_down_dict.keys():
                        high_df = pa_down_cmp_df.loc[pa_down_cmp_df[key] > pa_down_cmp_df[cmp_down_dict[key][0]]]
                        high_df = self._cmp_data_process_2_(df=high_df, key=key, val=cmp_down_dict[key][0],
                                                          msg=cmp_down_dict[key][2],
                                                          type=cmp_down_dict[key][4])
                        if key == 'pa_vector':
                            low_df = None # PA 向量长度短，不做异常记录
                        else:
                            low_df = pa_down_cmp_df.loc[pa_down_cmp_df[key] < pa_down_cmp_df[cmp_down_dict[key][1]]]
                            low_df = self._cmp_data_process_2_(df=low_df, key=key, val=cmp_down_dict[key][1],
                                                             msg=cmp_down_dict[key][3],
                                                             type=cmp_down_dict[key][5])
                        if cmp_down_result_df is None or len(cmp_down_result_df) == 0:
                            if high_df is not None:
                                cmp_down_result_df = high_df
                                if low_df is not None:
                                    cmp_down_result_df = cmp_down_result_df.append(low_df)
                            else:
                                cmp_down_result_df = low_df
                        else:
                            if high_df is not None:
                                cmp_down_result_df = cmp_down_result_df.append(high_df)
                            if low_df is not None:
                                cmp_down_result_df = cmp_down_result_df.append(low_df)

                # 本ID 的异常PA检测完成，收集结果到 cmp_pa_result
                if cmp_pa_result is None or len(cmp_pa_result) == 0:
                    if cmp_up_result_df is not None:
                        cmp_pa_result = cmp_up_result_df
                        if cmp_down_result_df is not None:
                            cmp_pa_result = cmp_pa_result.append(cmp_down_result_df)
                    else:
                        cmp_pa_result = cmp_down_result_df
                else:
                    if cmp_up_result_df is not None:
                        cmp_pa_result = cmp_pa_result.append(cmp_up_result_df)
                    if cmp_down_result_df is not None:
                        cmp_pa_result = cmp_pa_result.append(cmp_down_result_df)

                # 准备进入下一个 while frame_begin_stamp < end_stamp 循环
                frame_begin_time_str = self.t_frame_dict.get(frame_begin_time_str)[1]
                if len(frame_begin_time_str) == 0:
                    wx.info("[Rt_Ana][RT_CMP_PA_Baseline] {} {} 已处理完毕，进入下一支股票".format(id, t_frame))
                    break
                frame_begin_stamp = int(time.mktime(time.strptime(date_str + frame_begin_time_str, '%Y%m%d%H:%M')))

        cols = ['id', 'date', 'time', 'type', 'msg']
        cmp_pa_result = cmp_pa_result.loc[:, cols]

        return cmp_pa_result


    def rt_cmp_big_baseline(self, date_str=None, rt=None, big_bl_df=None):
        wx = lg.get_handle()
        rt_dict_df = rt.rt_dict_df
        if date_str is None:
            date_str = (date.today()).strftime('%Y%m%d')
        if rt_dict_df is None:
            wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] 实时数据字典 是空，退出")
            return None

        # 暂不需要 big_bl_df 大单基线数据，不需要做对比，直接体现在BI图形上
        # if big_bl_df is None:
        #     wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] 基线数据 是空，退出")
        #     return None

        rt_big_deal_df = pd.DataFrame()
        for id in rt_dict_df.keys():
            if rt_dict_df[id] is None:
                wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] {} 未产生实时交易数据，进入下一支股票".format(id))
                continue
            if date_str is None:  # 处理当天数据
                # 起始时间边界对齐
                [frame_begin_stamp, frame_begin_time_str] = self.rt_df_find_start_stamp(
                    rt_stamp=rt_dict_df[id]['time_stamp'].min())
            else:  # 处理历史数据 , 只用来做调试
                # 起始时间边界对齐
                # 从记录文件读取上一次查询实时交易的截止时间，本次查询的开始时间
                # 获得的时间戳有两种情况 1）取整（半小时）的时间戳 2）空文件，自动处理成 09:25
                # 记录文件名：日期_数据来源
                frame_begin_time_str = '09:25'
                frame_begin_stamp = int(time.mktime(time.strptime(date_str + frame_begin_time_str, '%Y%m%d%H:%M')))

            end_stamp = rt_dict_df[id].time_stamp.max()
            # 按时间段 切片rt数据，计算每个片段的 大单数据，并与基线做比对，再导入基线数据库
            while frame_begin_stamp < end_stamp:
                # frame_begin_time_str = time.strftime("%H:%M", time.localtime(frame_begin_stamp))
                frame_end_time_str = self.t_frame_dict.get(frame_begin_time_str)[0]
                if frame_end_time_str is None:
                    wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] {} [{}] 起始时间不属于正常范围！！！！".format(id, frame_begin_time_str))
                    break
                else:
                    t_frame = frame_begin_time_str + "-" + frame_end_time_str
                    frame_end_stamp = int(time.mktime(time.strptime(date_str + frame_end_time_str, '%Y%m%d%H:%M')))

                if frame_end_stamp > end_stamp:
                    wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] {} {} 已超出本次获取的实时数据范围，进入下一支股票".format(id, t_frame))
                    break

                if frame_end_time_str == '15:00':  # 15:00 收市后，有最后一笔交易记录 产生在 15:00 之后若干秒
                    rt_df = rt.rt_dict_df[id].loc[(rt.rt_dict_df[id]['time_stamp'] >= frame_begin_stamp)].copy()
                else:
                    rt_df = rt.rt_dict_df[id].loc[(rt.rt_dict_df[id]['time_stamp'] >= frame_begin_stamp) &
                                                  (rt.rt_dict_df[id]['time_stamp'] < frame_end_stamp)].copy()

                if rt_df is None or rt_df.empty:
                    wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] [{}] 在[{}]期间交易数据为空，开始处理下一支股票".format(id, t_frame))
                    break

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

                rt_data = {"id":id, "date":date_str,"t_frame":t_frame, "big_qty":rt_big_qty,
                               "big_abs_pct":big_abs_amount_pct, "big_io_pct":big_io_amount_pct,
                               "big_buy_pct":rt_big_buy_amount_pct, "big_sell_pct":rt_big_sell_amount_pct,
                               "amount":rt_amount, "sell_qty":rt_sell_qty, "sell_amount":rt_sell_amount,
                               "buy_qty":rt_buy_qty, "buy_amount":rt_buy_amount,
                               "air_qty":rt_air_qty, "air_amount":rt_air_amount}

                if rt_big_deal_df is None or rt_big_deal_df.empty:
                    rt_big_deal_df = pd.DataFrame([rt_data])
                else:
                    rt_big_deal_df = rt_big_deal_df.append(pd.DataFrame([rt_data]))

                # 准备进入下一个循环
                frame_begin_time_str = self.t_frame_dict.get(frame_begin_time_str)[1]
                if len(frame_begin_time_str) == 0:
                    wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] {} {} 已处理完毕，进入下一支股票".format(id, t_frame))
                    break
                frame_begin_stamp = int(time.mktime(time.strptime(date_str + frame_begin_time_str, '%Y%m%d%H:%M')))

        cu_big_df = self.db.get_cu_big_deal_date(date_str = date_str)
        if cu_big_df is None:
            id_arr = list(rt_dict_df.keys())
            cu_big_df = pd.DataFrame({'id': list(id_arr), 'cu_big_qty': [float(0)] * len(id_arr), 'cu_amount': [float(0)] * len(id_arr),
                               'cu_sell_qty': [float(0)] * len(id_arr), 'cu_sell_amount': [float(0)] * len(id_arr),
                               'cu_buy_qty': [float(0)] * len(id_arr), 'cu_buy_amount': [float(0)] * len(id_arr)})

        final_cu_big_deal_df = pd.DataFrame()
        for each_id in rt_big_deal_df.groupby(rt_big_deal_df['id']):
            df_each_id = each_id[1].sort_values(by="t_frame", ascending=True)
            df_each_id['cu_big_qty'] = df_each_id['big_qty'].cumsum() + \
                                       cu_big_df.loc[cu_big_df['id']==each_id[0]]['cu_big_qty'].values[0]
            df_each_id['cu_amount'] = df_each_id['amount'].cumsum()+ \
                                      cu_big_df.loc[cu_big_df['id']==each_id[0]]['cu_amount'].values[0]
            df_each_id['cu_sell_qty'] = df_each_id['sell_qty'].cumsum()+\
                                        cu_big_df.loc[cu_big_df['id']==each_id[0]]['cu_sell_qty'].values[0]
            df_each_id['cu_sell_amount'] = df_each_id['sell_amount'].cumsum()+ \
                                           cu_big_df.loc[cu_big_df['id']==each_id[0]]['cu_sell_amount'].values[0]
            df_each_id['cu_buy_qty'] = df_each_id['buy_qty'].cumsum()+ \
                                       cu_big_df.loc[cu_big_df['id']==each_id[0]]['cu_buy_qty'].values[0]
            df_each_id['cu_buy_amount'] = df_each_id['buy_amount'].cumsum()+ \
                                          cu_big_df.loc[cu_big_df['id']==each_id[0]]['cu_buy_amount'].values[0]
            df_each_id['cu_air_qty'] = df_each_id['air_qty'].cumsum()
            df_each_id['cu_air_amount'] = df_each_id['air_amount'].cumsum()
            if final_cu_big_deal_df is None or len(final_cu_big_deal_df) == 0:
                final_cu_big_deal_df = df_each_id
            else:
                final_cu_big_deal_df = final_cu_big_deal_df.append(df_each_id)

        cols = ['id', 'date', 't_frame', 'big_qty', 'big_abs_pct', 'big_io_pct', 'big_buy_pct', 'big_sell_pct',
                'amount', 'sell_qty', 'sell_amount', 'buy_qty', 'buy_amount', 'air_qty', 'air_amount',
                'cu_big_qty', 'cu_amount', 'cu_sell_qty', 'cu_sell_amount', 'cu_buy_qty', 'cu_buy_amount',
                'cu_air_qty', 'cu_air_amount']

        final_cu_big_deal_df = final_cu_big_deal_df.loc[:, cols]
        final_cu_big_deal_df.fillna(0, inplace=True)
        final_cu_big_deal_df.reset_index(drop=True, inplace=True)

        return final_cu_big_deal_df

        """
        if rt_big_deal_df is None or rt_big_deal_df.empty:
            wx.info("[Rt_Ana][Rt_Cmp_Big_Baseline] 大单数据为空，退出")
            return None
        else:
            cols = ['id','date','t_frame','big_qty','big_abs_pct','big_io_pct','big_buy_pct','big_sell_pct',
                    'amount','sell_qty','sell_amount','buy_qty','buy_amount','air_qty','air_amount']
            rt_big_deal_df = rt_big_deal_df.loc[:,cols]
            rt_big_deal_df.fillna(0,inplace=True)
            rt_big_deal_df.reset_index(drop=True, inplace=True)

        return rt_big_deal_df
        """
        # 导入基线数据库
        # self.db.db_load_into_RT_BL_Big_Deal(df=rt_big_deal_df)

        """ 不做统计对比， 直接导入 基线数据库，BI工具图形化展示
        big_cmp_df = pd.merge(rt_big_deal_df, big_bl_df, on=['id','t_frame'], how='left')

        cmp_dict = {'big_qty':['b_qty_max', 'b_qty_min','大单数量-[超高]-','大单数量-[超低]-','[B多]大单数量高','[B空]大单数量低'],  # 大单数量
                    'big_abs_pct':['b_pct_max', 'b_pct_min','大单金额占比-[超高]-','大单金额占比-[超低]-','[B多]大单净比高','[B空]大单净比低'], # 大单金额占比
                    'big_buy_pct':['b_buy_pct_max', 'b_buy_pct_min','大买单金额占比-[超高]-','大买单金额占比-[超低]-','[B多]大买单净比高','[B空]大买单净比低'],  # 大单买入金额占比
                    'big_sell_pct':['b_sell_pct_max', 'b_sell_pct_min','大卖单金额占比-[超高]-','大卖单金额占比-[超低]-','[B空]大卖单净比高','[B多]大卖单净比低'], # 大单卖出金额占比
                    'buy_qty':['all_buy_qty_max', 'all_buy_qty_min','外盘数量-[超高]-','外盘数量-[超低]-','[A多]外盘数量高','[A空]外盘数量低'], # 买盘（外盘）单数
                    'buy_amount':['all_buy_amount_max', 'all_buy_amount_min','外盘金额-[超高]-','外盘金额-[超低]-','[A多]外盘净比高','[A空]外盘净比低'], # 买盘（外盘） 金额
                    'sell_qty': ['all_sell_qty_max', 'all_sell_qty_min','内盘数量-[超高]-','内盘数量-[超低]-', '[A空]内盘数量高', '[A多]内盘数量低'], # 卖盘（内盘）单数
                    'sell_amount':['all_sell_amount_max', 'all_sell_amount_min','内盘金额-[超高]-','内盘金额-[超低]-', '[A空]内盘净比高', '[A多]内盘净比低']} # 卖盘（内盘）金额
        cmp_result_df = pd.DataFrame()
        for key in cmp_dict.keys():
            high_df = big_cmp_df.loc[big_cmp_df[key] > big_cmp_df[cmp_dict[key][0]]]
            high_df = self._cmp_data_process_(df = high_df, key = key, val = cmp_dict[key][0], msg = cmp_dict[key][2], type = cmp_dict[key][4])
            low_df = big_cmp_df.loc[big_cmp_df[key] < big_cmp_df[cmp_dict[key][1]]]
            low_df = self._cmp_data_process_(df = low_df, key = key, val = cmp_dict[key][1], msg = cmp_dict[key][3], type = cmp_dict[key][5])
            if cmp_result_df is None or len(cmp_result_df) == 0:
                if high_df is not None:
                    cmp_result_df = high_df
                    if low_df is not None:
                        cmp_result_df = cmp_result_df.append(low_df)
                else:
                    cmp_result_df = low_df
            else:
                if high_df is not None:
                    cmp_result_df = cmp_result_df.append(high_df)
                if low_df is not None:
                    cmp_result_df = cmp_result_df.append(low_df)

        cols = ['id', 'date', 't_frame', 'type', 'msg']
        cmp_result_df = cmp_result_df.loc[:, cols]

        return cmp_result_df
        """
    def _cmp_data_process_(self, df=None, type='', key='', val='', msg=''):
        if df is None:
            return None
        ret_df = pd.DataFrame(df, columns=['id','date','t_frame',key, val ])
        # ret_df[[key, val]] = ret_df[[key, val]].astype(int)
        # ret_df[key] = round(ret_df[key],2)
        # ret_df[val] = round(ret_df[val],2)
        ret_df = ret_df.round({key: 2, val: 2})
        ret_df[[key, val]] = ret_df[[key, val]].astype(str)
        ret_df['type'] = type
        ret_df['msg'] = msg +"[当前]:" +ret_df[key]+" -- [最值]:"+ ret_df[val]
        ret_df.drop(columns=[key, val], inplace=True)
        return ret_df

    def _cmp_data_process_2_(self, df=None, type='', key='', val='', msg=''):
        if df is None or len(df)==0:
            return None
        ret_df = pd.DataFrame(df, columns=['id', 'date','time',  key, val])
        # ret_df[[key, val]] = ret_df[[key, val]].astype(int)
        # ret_df[key] = round(ret_df[key],2)
        # ret_df[val] = round(ret_df[val],2)
        ret_df = ret_df.round({key: 2, val: 2})
        ret_df[[key, val]] = ret_df[[key, val]].astype(str)
        ret_df['type'] = type
        ret_df['msg'] = msg + "[当前]:" + ret_df[key] + " -- [最值]:" + ret_df[val]
        ret_df.drop(columns=[key, val], inplace=True)
        return ret_df

    def rt_df_find_start_stamp(self, rt_stamp=None):
        wx = lg.get_handle()
        if rt_stamp is None:
            wx.info("[Rt_Ana][Rt_DF_Segment] 初始时间戳为空，退出")
            return None
        record_stamp_arr = list(self.record_stamp_dict.keys())
        record_stamp_arr.sort(reverse=True)
        for stamp in record_stamp_arr:  # stamp 的每个时间段的起始时间点
            if stamp > rt_stamp:
                continue
            else:
                return [stamp, self.record_stamp_dict[stamp]]
        return None

    def db_load_into_rt_msg(self, cmp_df = None):
        wx = lg.get_handle()
        if cmp_df is None:
            wx.info("[Rt_Ana][DB_load_into_rt_msg] 初始时间戳为空，退出")
            return None
        self.db.db_load_RT_MSG(df = cmp_df)
        wx.info("[Rt_Ana][DB_load_into_rt_msg] 已导入 {} 条超阀值的交易记录".format(len(cmp_df)))

    """
    # 废弃函数
    def rt_analyzer(self, rt = None):
        wx = lg.get_handle()
        rt_dict_df = rt.rt_dict_df
        if rt_dict_df is None:
            wx.info("[rt_ana][rt_analyzer] 实时数据字典 是空，退出分析")
            return None

        # for key in rt_dict_df:
        #     self.output_table(dd_df=rt_dict_df[key], filename= key+'实时数据', sheet_name=key)
        #     wx.info("[rt_ana][rt_analyzer] {} : {} 条数据已输出".format(key, len(rt_dict_df[key])))

        sliced_ana_ret_df_1 = pd.DataFrame()  # 保存切片的 大单成交记录统计结果
        sliced_ana_ret_df_2 = pd.DataFrame()  # 保存切片的 每分钟成交量、每0.01% 振幅的成交量 统计结果

        # 所有被监控的股票 一起按时间段切片，然后再调用 分析函数
        for t_slice in self.ana_time_slice_arr:
            sliced_rt_df = pd.DataFrame() # 开始切片前，清空初始化
            for key in rt_dict_df:
                sliced_tmp = self.rt_df_slice(rt_df=rt_dict_df[key], t_slice= int(t_slice))
                sliced_tmp['id'] = key
                if sliced_rt_df.empty:
                    sliced_rt_df = sliced_tmp
                else:
                    sliced_rt_df = sliced_rt_df.append(sliced_tmp)
            wx.info("[rt_ana][rt_analyzer][{}秒]切片，[{}--{}]交易数据一共{}条，开始分析...".
                    format(t_slice, sliced_rt_df.time_str.min(), sliced_rt_df.time_str.max(), len(sliced_rt_df)))

            rt_summery_1 = self.rt_rule_1(rt = rt, rt_df = sliced_rt_df.copy(), t_slice = int(t_slice))
            if sliced_ana_ret_df_1 is None or sliced_ana_ret_df_1.empty:
                sliced_ana_ret_df_1 = rt_summery_1
            else:
                sliced_ana_ret_df_1 = sliced_ana_ret_df_1.append(rt_summery_1)

            # rt_summery_2 = self.rt_rule_2(rt=rt, rt_df=sliced_rt_df.copy(), t_slice= int(t_slice))

        # 调用分析对比函数，寻找大单异常
        if self.rt_data_keeper_1 is None or self.rt_data_keeper_1.empty:
            wx.info("[rt_ana][rt_analyzer] 大单分析，导入第一批数据建立基线")
            self.rt_data_keeper_1 = sliced_ana_ret_df_1
        else:
            self.rt_data_keeper_1 = self.rt_cmp_1(his_df=self.rt_data_keeper_1, new_df = sliced_ana_ret_df_1)

        # self.msg.output("*****[{}秒]切片，[{}--{}] 大单交易数据{}条，已发送成功...".
        #         format(t_slice, sliced_rt_df.time_str.min(), sliced_rt_df.time_str.max(), len(sliced_ana_ret_df)))



    # 废弃函数
    # 【大单分析】时间切片 筛选出 big_amount 的大单，再统计金额、笔数、买入卖出
    # vol单位：1股
    # 与rt_data_keeper 保存的数据做比对
    def rt_rule_1(self,rt=None, rt_df=None, t_slice = 0 ):
        wx= lg.get_handle()
        if rt_df is None or rt_df.empty:
            wx.info("[rt_ana][rt_analyzer][rt_rule_1] 实时数据切片为空，退出")
            return
        rt_df['amount']=rt_df['vol']*rt_df['price']
        rt_df['io_amount']=rt_df['amount']*rt_df['type']

        # ID 的所有成交量
        rt_id_amount = rt_df['amount'].groupby(rt_df['id']).sum()
        rt_id_amount_df = pd.DataFrame(rt_id_amount)
        rt_id_amount_df.reset_index(drop=False, inplace=True)
        rt_id_amount_df.columns = ['id', 'sliced_total_amount']

        # 成交明细中的 大单列表
        rt_rule_result = rt_df.loc[rt_df['amount']>=self.rt_big_amount,]
        # 按ID 计算大单的数量
        rt_counter = rt_rule_result.groupby(by='id', as_index=False).size()
        # 大单买入、卖出金额合计
        rt_big_amount_sum_abs = rt_rule_result['amount'].groupby(rt_rule_result['id']).sum()
        # 平均每分钟的 大单买入、卖出金额
        rt_ave_big_amount_per_min_abs= rt_rule_result['amount'].groupby(rt_rule_result['id']).sum() / (t_slice/60)
        # 大单买入 卖出对冲后的金额
        rt_big_amount_sum_io = rt_rule_result['io_amount'].groupby(rt_rule_result['id']).sum()
        rt_rule_result_summery = pd.concat([rt_counter, rt_ave_big_amount_per_min_abs, rt_big_amount_sum_abs, rt_big_amount_sum_io], axis=1)
        rt_rule_result_summery.reset_index(drop=False, inplace=True)
        rt_rule_result_summery.columns = ['id', 'big_counter', 'ave_big_A_per_mint', 'big_A_sum_abs', 'big_A_sum_io']

        # 卖盘的 大单金额
        rt_sell_result = rt_df.loc[(rt_df['amount'] >= self.rt_big_amount) & (rt_df['type'] < 0),]
        rt_big_sell_amount = rt_sell_result['amount'].groupby(rt_sell_result['id']).sum()
        rt_big_sell_amount_df = pd.DataFrame(rt_big_sell_amount)
        rt_big_sell_amount_df.reset_index(drop=False, inplace=True)
        rt_big_sell_amount_df.columns = ['id', 'big_sell_amount_sum']

        # 买盘的 大单金额
        rt_buy_result = rt_df.loc[(rt_df['amount'] >= self.rt_big_amount) & (rt_df['type'] > 0),]
        rt_big_buy_amount = rt_buy_result['amount'].groupby(rt_buy_result['id']).sum()
        rt_big_buy_amount_df = pd.DataFrame(rt_big_buy_amount)
        rt_big_buy_amount_df.reset_index(drop=False, inplace=True)
        rt_big_buy_amount_df.columns = ['id', 'big_buy_amount_sum']

        # DataFrame 组合，大单数量、大单总成交额、大单对冲成交额 slice 总成交量、大单买入金额、大单卖出金额
        rt_rule_result_summery = pd.merge(rt_rule_result_summery, rt_id_amount_df, how='left', on=['id'])
        rt_rule_result_summery = pd.merge(rt_rule_result_summery, rt_big_sell_amount_df, how='left', on=['id'])
        rt_rule_result_summery = pd.merge(rt_rule_result_summery, rt_big_buy_amount_df, how='left', on=['id'])
        rt_rule_result_summery['slice'] = t_slice
        rt_rule_result_summery.fillna(0, inplace=True)
        # self.msg.output("发现{}只股票".format(rt_counter))
        return rt_rule_result_summery


    # 废弃函数
    # 时间切片 计算平均每分钟的成交量 ，与基线数据对比
    # vol单位：1股
    def rt_rule_2(self, rt=None, rt_df=None, t_slice = 0):
        wx = lg.get_handle()
        if rt_df is None or rt_df.empty:
            wx.info("[rt_ana][rt_analyzer][rt_rule_2] 实时数据切片为空，退出")
            return
        if t_slice == 0:
            wx.info("[rt_ana][rt_analyzer][rt_rule_2] 切片时间长度为零，退出")

        # 计算切片时间段内，平均每分钟成交量 VS 基线平均值
        rt_df['amount']=rt_df['vol']*rt_df['price']
        amount_per_mint_df = rt_df['amount'].groupby(rt_df['id']).sum()/(int(t_slice)/60)
        # amount_per_mint_df.reset_index(drop=False, inplace=True)

        # 计算切片时间内，平均0.01% 振幅对应的成交量
        # pct_up_down_df = 10000 * (rt_df['price'].groupby(rt_df['id']).max()/rt_df['price'].groupby(rt_df['id']).min()-1)
        amount_per_pct_df = rt_df['amount'].groupby(rt_df['id']).sum()/\
                      (10000*(rt_df['price'].groupby(rt_df['id']).max()/rt_df['price'].groupby(rt_df['id']).min()-1))

        rt_rule_result_summery = pd.concat([amount_per_mint_df, amount_per_pct_df], axis=1)
        rt_rule_result_summery.columns=['sliced_A_per_mint', 'sliced_A_per_pct']
        rt_rule_result_summery.reset_index(drop=False, inplace=True)

        # DataFrame 导入 基线平均值
        rt_rule_result_summery = pd.merge(rt_rule_result_summery,rt.std_PVA_df, how='inner', on=['id'])

        return rt_rule_result_summery

    # 废弃函数
    # 对实时交易Dataframe 按照 conf 文件中的 ana_time_slice 切片
    def rt_df_slice(self, rt_df = None, t_slice=0):
        wx = lg.get_handle()
        if rt_df is None:
            wx.info("[rt_ana][rt_df_slice] 按时间切片的源DataFrame是空，退出分析")
            return None
        rt_df=rt_df.sort_values(by="time_str", ascending= False)
        # 最后一条交易记录的时间戳
        lastest_t_stamp = rt_df.head(1)["time_stamp"][0]
        # 减去时间片长度，得到开始的时间戳
        start_t_stamp = lastest_t_stamp - t_slice
        sliced_rt_df = rt_df.loc[rt_df['time_stamp'] >= start_t_stamp]
        return sliced_rt_df

    # 废弃函数
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
    """
