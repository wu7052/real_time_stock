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
        rt_ana_time_slice = h_conf.rd_opt('general', 'rt_ana_time_slice')
        self.rt_ana_time_slice_arr = rt_ana_time_slice.split(',')
        self.rt_big_amount = float(h_conf.rd_opt('rt_analysis_rules','big_deal_amount'))
        self.msg = f_msg()

    def rt_analyzer(self, rt = None):
        wx = lg.get_handle()
        rt_dict_df = rt.rt_dict_df
        if rt_dict_df is None:
            wx.info("[rt_ana][rt_analyzer] 实时数据字典 是空，退出分析")
            return None

        # for key in rt_dict_df:
        #     self.output_table(dd_df=rt_dict_df[key], filename= key+'实时数据', sheet_name=key)
        #     wx.info("[rt_ana][rt_analyzer] {} : {} 条数据已输出".format(key, len(rt_dict_df[key])))

        # 所有被监控的股票 一起按时间段切片，然后再调用 分析函数
        for t_slice in self.rt_ana_time_slice_arr:
            sliced_rt_df = pd.DataFrame() # 开始切片前，清空初始化
            for key in rt_dict_df:
                sliced_tmp = self.rt_df_slice(rt_df=rt_dict_df[key], t_slice= int(t_slice))
                sliced_tmp['id'] = key
                if sliced_rt_df.empty:
                    sliced_rt_df = sliced_tmp
                else:
                    sliced_rt_df = sliced_rt_df.append(sliced_tmp)
            wx.info("[rt_ana][rt_analyzer][{}]切片完成，实时交易数据一共{}条，开始分析...".format(t_slice,len(sliced_rt_df)))
            self.rt_rule_1(rt = rt, rt_df = sliced_rt_df)

    # 时间切片内
    # 寻找超过 big_amount 的大单
    # vol单位：1股
    def rt_rule_1(self,rt=None, rt_df=None ):
        wx= lg.get_handle()
        if rt_df is None or rt_df.empty:
            wx.info("[rt_ana][rt_analyzer][rt_rule_1] 实时数据切片为空，退出")
            return
        rt_df['amount']=rt_df['vol']*rt_df['price']
        rt_df['io_amount']=rt_df['amount']*rt_df['type']
        rt_rule_result = rt_df.loc[rt_df['amount']>=self.rt_big_amount,]
        rt_counter = rt_rule_result.groupby(by='id', as_index=False).size()
        # 买入、卖出金额合计
        rt_big_amount_sum_abs = rt_rule_result['amount'].groupby(rt_rule_result['id']).sum()
        # 买入 卖出对冲后的金额
        rt_big_amount_sum = rt_rule_result['io_amount'].groupby(rt_rule_result['id']).sum()
        rt_rule_result_summery = pd.concat([rt_counter, rt_big_amount_sum_abs, rt_big_amount_sum], axis=1)
        rt_rule_result_summery.reset_index(drop=False, inplace=True)
        # self.msg.output("发现{}只股票".format(rt_counter))

    # 时间切片内
    # 计算振幅 对应的成交量，与基线数据对比
    # vol单位：1股
    def rt_rule_2(self, rt=None, rt_df=None):

        wx = lg.get_handle()

        pass

    # 对实时交易Dataframe 按照 conf 文件中的 rt_ana_time_slice 切片
    def rt_df_slice(self, rt_df = None, t_slice=0):
        wx = lg.get_handle()
        if rt_df is None:
            wx.info("[rt_ana][rt_df_slice] 按时间切片的源DataFrame是空，退出分析")
            return None
        rt_df=rt_df.sort_values(by="time_str", ascending= False)
        # 最后一条交易记录的时间戳
        lastest_t_stamp = rt_df.head(1)["time_stamp_sec"][0]
        # 减去时间片长度，得到开始的时间戳
        start_t_stamp = lastest_t_stamp - t_slice
        sliced_rt_df = rt_df.loc[rt_df['time_stamp_sec'] >= start_t_stamp]
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