from db_package import db_ops
import new_logger as lg
from datetime import datetime, time, date, timedelta
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

        self.rt_data_keeper_1 = pd.DataFrame() # 保存 大单记录，作为基线数据

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

    # 大单数据对比分析
    def rt_cmp_1(self, his_df=None, new_df = None):
        wx = lg.get_handle()
        wx.info("[rt_ana][rt_cmp_1]")

        pass

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

