from realtime_package import rt_163, rt_east, rt_sina, rt_ana, rt_bl
import pandas as pd
from datetime import datetime, timedelta
import time
import new_logger as lg
from conf import conf_handler, xl_handler
from db_package import db_ops

# 计时器 装饰器
def wx_timer(func):
    def wrapper(*args, **kwargs):
        wx = lg.get_handle()
        start_time = time.time()
        func(*args, **kwargs)
        time_used = time.time() - start_time
        # print("{} used {} seconds".format(func.__name__, time_used))
        wx.info("{} used {:.2f} seconds".format(func.__name__, time_used))

    return wrapper  # 这个语句 不属于 wrapper(), 而是 wx_timer 的返回值. 对应 func 后面这个()调用


# 计时器 装饰器
def wx_timer_ret(func):
    def wrapper(*args, **kwargs):
        wx = lg.get_handle()
        start_time = time.time()
        ret = func(*args, **kwargs)
        time_used = time.time() - start_time
        # print("{} used {} seconds".format(func.__name__, time_used))
        wx.info("{} used {:.2f} seconds".format(func.__name__, time_used))
        return ret
    return wrapper  # 这个语句 不属于 wrapper(), 而是 wx_timer 的返回值. 对应 func 后面这个()调用




# 获取实时数据
# src 数据源 ， t_frame 读取间隔时间， id_arr 股票ID代码数组
def get_rt_data(rt=None, src=''):
    wx = lg.get_handle()

    # 股票代码数组，由 rt 对象内部变量 带入
    if rt.id_arr is None:
        wx.info("[Get_RT_Data] 股票列表为空，退出")
        return None

    # 获得当前时间，作为查询实时交易数据的时间节点
    time_str = time.strftime("%H:%M:%S", time.localtime())
    # time_str = (datetime.datetime.now()).strftime("%H:%M:%S")
    # 时间偏移到交易时间，测试用途
    # time_str = (datetime.now()+timedelta(hours=-6)).strftime("%H:%M:%S")

    # rt 对象在主函数生成，传入此函数，添加
    if src == '163':
        wx.info("[Get_RT_Data] 从[{}] 查询 [{}]支股票的 交易数据 [{}] ".format(src, len(rt.id_arr), time_str ))

    for icount, id in enumerate(rt.id_arr):
        # wx.info("[Get_RT_Data][{}:{}] {} 获取逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
        json_str = rt.get_json_str(id=id, time_str=time_str)
        # wx.info("[Get_RT_Data][{}:{}] {} 解析逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
        time_range = rt.json_parse(id=id, json_str=json_str)
        wx.info("[Get_RT_Data][{}/{}] {} [{}--{}]逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id,time_range[0],time_range[1], time_str))


def ana_rt_data(ana=None, rt=None):
    ana.rt_analyzer(rt = rt)

# 重新设定 实时数据的基线
def rebase_rt_data(id_arr = None, rt=None):
    wx = lg.get_handle()
    db = db_ops()
    h_conf = conf_handler(conf="rt_analyer.conf")
    rt_big_amount = float(h_conf.rd_opt('rt_analysis_rules', 'big_deal_amount'))

    if rt is None or rt.empty:
        wx.info("[rebase_rt_data]基线设定：实时数据DataFrame为空，退出")
        return

    for id in rt.rt_dict_df.keys():
        # 如果指定了 股票id，
        if id_arr is not None and id not in id_arr:
            wx.info("[rebase_rt_data]已指定需更新基线的股票代码，[{}]不是指定的股票代码".format(id))
            continue
        else:
            rt_df = rt.rt_dict_df[id]
            rt_end_time = rt_df['time_stamp'].max()
            rt_begin_time = rt_df['time_stamp'].min()
            rt_begin_timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rt_begin_time))
            rt_end_timestr = time.strftime("%H:%M:%S", time.localtime(rt_end_time))
            wx.info("[rebase_rt_data]开始更新[{}]的数据基线[{}--{}]".format(id, rt_begin_timestr, rt_end_timestr))
            rt_date_str = time.strftime("%Y%m%d", time.localtime(rt_begin_time))

            # ID 的所有成交量
            rt_df['amount'] = rt_df['vol'] * rt_df['price']
            rt_df['io_amount'] = rt_df['amount'] * rt_df['type']
            rt_amount = rt_df['amount'].sum()

            # 成交明细中的 大单列表
            rt_big_df = rt_df.loc[rt_df['amount'] >= rt_big_amount,]
            # 大单的数量
            rt_big_qty = len(rt_big_df)
            # 大单买入、卖出金额合计
            rt_big_amount_sum_abs = rt_big_df['amount'].sum()
            # 大单买入 卖出对冲后的金额
            rt_big_amount_sum_io = rt_big_df['io_amount'].sum()

            # 大单金额 占 总成交量的比例
            big_amount_pct = rt_big_amount_sum_abs/rt_amount

            # 平均每分钟的 大单买入、卖出金额
            rt_ave_big_amount_per_min_abs = rt_big_amount_sum_abs/((rt_end_time-rt_begin_time)/60)

            # 卖盘的 大单金额
            rt_big_sell_df = rt_big_df.loc[(rt_big_df['type'] < 0),]
            rt_big_sell_amount = rt_big_sell_df['amount'].sum()
            rt_big_sell_amount_pct = rt_big_sell_amount/rt_amount

            # 买盘的 大单金额
            rt_big_buy_df = rt_big_df.loc[(rt_big_df['type'] > 0),]
            rt_big_buy_amount = rt_big_buy_df['amount'].sum()
            rt_big_buy_amount_pct = rt_big_buy_amount/rt_amount

            rt_baseline = {"date":rt_date_str,"id":id, "big_qty":rt_big_qty, "big_pct":big_amount_pct,
                           "big_bug_pct":rt_big_buy_amount_pct, "big_sell_pct":rt_big_sell_amount_pct}
            # DataFrame 组合，大单数量、大单总成交额、大单对冲成交额 slice 总成交量、大单买入金额、大单卖出金额
            # rt_rule_result_summery = pd.merge(rt_rule_result_summery, rt_id_amount_df, how='left', on=['id'])
            # rt_rule_result_summery = pd.merge(rt_rule_result_summery, rt_big_sell_amount_df, how='left', on=['id'])
            # rt_rule_result_summery = pd.merge(rt_rule_result_summery, rt_big_buy_amount_df, how='left', on=['id'])
            # rt_rule_result_summery.columns = ['id', 'big_counter', 'ave_big_A_per_mint', 'big_A_sum_abs', 'big_A_sum_io']
            # rt_rule_result_summery.fillna(0, inplace=True)


# rt实时对象，src 数据源
# 利用全局RT 对象完成 数据收集
# 创建BL 对象完成 基线设定、导入数据库
def traceback_rt_data(rt=None, src='', date_str = None):
    wx = lg.get_handle()

    # 股票代码数组，由 rt 对象内部变量 带入
    if rt.id_arr is None:
        wx.info("[Traceback_RT_Data] 股票列表为空，退出")
        return None

    # 股票代码数组，由 rt 对象内部变量 带入
    if date_str is None or len(date_str) == 0:
        date_str = (datetime.today()).strftime('%Y%m%d')
        wx.info("[Traceback_RT_Data] 未指定回溯的日期，默认使用 {}".format(date_str))

    # 起始时间，作为查询实时交易数据的时间节点
    # date_str = '20191216'
    begin_time_arr = ['09:30','10:30','13:00','14:00']#
    # end_time_arr = ['09:35','11:30','14:00','15:00']
    end_time_arr = ['10:30','11:30','14:00','15:00']#

    baseline_big_deal_df = pd.DataFrame()
    bl = rt_bl()
    for index in range(len(begin_time_arr)):
        time_inc = 5
        begin_time_stamp = int(time.mktime(time.strptime(date_str+begin_time_arr[index], "%Y%m%d%H:%M")))
        end_time_stamp = int(time.mktime(time.strptime(date_str+end_time_arr[index], "%Y%m%d%H:%M")))
        while begin_time_stamp  <= end_time_stamp:
            time_str = time.strftime("%H:%M:%S", time.localtime(begin_time_stamp))
            # wx.info("{}".format(time_str))
            begin_time_stamp += time_inc*60

            # rt 对象在主函数生成，传入此函数，添加
            if src == '163':
                wx.info("[Traceback_RT_Data] 从[{}] 获得[{}] 支股票的交易数据 [{}]-[{}] ".format(src, len(rt.id_arr), date_str, time_str ))

            for icount, id in enumerate(rt.id_arr):
                # wx.info("[Get_RT_Data][{}:{}] {} 获取逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
                json_str = rt.get_json_str(id=id, time_str=time_str)
                # wx.info("[Get_RT_Data][{}:{}] {} 解析逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
                time_range = rt.json_parse(id=id, json_str=json_str)
                if time_range is None:
                    wx.info("[Traceback_RT_Data][{}/{}] [{}] [{}-{}] 交易数据不存在".format(icount+1,len(rt.id_arr),id, date_str, time_str))
                else:
                    wx.info("[Traceback_RT_Data][{}/{}] {} [{}--{}]逐笔交易数据[{}-{}]".format(icount+1,len(rt.id_arr),id,time_range[0],time_range[1], date_str, time_str))
                time.sleep(0.5)

        # 量价 基线数据，每个小时产生一次
        baseline_PA_df = bl.baseline_PA(rt=rt, date_str=date_str, time_frame_arr=[begin_time_arr[index], end_time_arr[index]])
        # 导入数据库
        bl.db_load_baseline_PA(df=baseline_PA_df)

        # 大单交易的 基线数据，每个小时产生一次
        # baseline_big_deal_df = bl.baseline_big_deal(rt=rt, date_str=date_str, time_frame_arr=[begin_time_arr[index], end_time_arr[index]])
        # 导入数据库
        # bl.db_load_baseline_big_deal(df = baseline_big_deal_df)

        # 释放 RT 对象的内部变量，只保留 最后30分钟的交易数据
        rt.clr_rt_data(minutes=30)