from realtime_package import rt_163, rt_east, rt_sina, rt_ana, rt_bl, wx_timer
import pandas as pd
from datetime import datetime, timedelta
import time
import new_logger as lg

"""
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
"""

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
def get_rt_data(rt=None, src='', date_str=''):
    wx = lg.get_handle()

    # 股票代码数组，由 rt 对象内部变量 带入
    if rt.id_arr is None:
        wx.info("[Get_RT_Data]: 股票列表为空，退出")
        return None

    if date_str is None or len(date_str) == 0:
        date_str = (datetime.today()).strftime('%Y%m%d')
        wx.info("[Get_RT_Data] 未指定交易日期，默认使用 {}".format(date_str))

    my_timer = wx_timer()

    # 判断 今日期 是否交易日
    if my_timer.is_trading_date(date_str=date_str):
        wx.info("今天是交易日，继续运行")
    else:
        wx.info("[Get_RT_Data]:{}不是交易日，退出实时交易数据获取 ".format(date_str))
        return None

    time_inc = 5  # 5分钟增量

    # 从记录文件读取上一次查询实时交易的截止时间，本次查询的开始时间
    # 获得的时间戳有两种情况 1）取整（半小时）的时间戳 2）空文件，自动处理成 09:25
    # 记录文件名：日期_数据来源
    begin_time_str = rt._get_last_record_()
    if begin_time_str is None :
        begin_time_str = '09:25'
    elif begin_time_str == '15:00':
        wx.info("[Get_RT_Data] 今日文件记录已查询过所有实时交易，退出")
        return None

    # 开始 获取实时交易的时间起点，并判断时间是否在交易时间
    begin_time_stamp = int(time.mktime(time.strptime(date_str+begin_time_str[:5], "%Y%m%d%H:%M")))
    ret_zone = my_timer.tell_time_zone(t_stamp = begin_time_stamp)
    if ret_zone[0] < 0:
        begin_time_stamp = ret_zone[1]

    # 当前时间，如果超出交易时间，则拉回到 交易时间
    end_time_stamp = int(time.time())
    ret_zone = my_timer.tell_time_zone(t_stamp = end_time_stamp)
    if ret_zone[0] < 0:
        end_time_stamp = ret_zone[1]

    # if begin_time_stamp > end_time_stamp:
    #     wx.info("[Get_RT_Data] 查询间隔不足5分钟，需等待{}秒".format(begin_time_stamp-end_time_stamp))
    #     time.sleep(begin_time_stamp-end_time_stamp)

    while begin_time_stamp <= end_time_stamp:
        time_str = time.strftime("%H:%M:%S", time.localtime(begin_time_stamp))
        # rt 对象在主函数生成，传入此函数，添加
        if src == '163':
            wx.info("[Get_RT_Data] 从[{}] 查询 [{}]支股票的 交易数据 [{}] ".format(src, len(rt.id_arr), time_str))

        for icount, id in enumerate(rt.id_arr):
            # wx.info("[Get_RT_Data][{}:{}] {} 获取逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
            json_str = rt.get_json_str(id=id, time_str=time_str)
            # wx.info("[Get_RT_Data][{}:{}] {} 解析逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
            time_range = rt.json_parse(id=id, json_str=json_str)
            if time_range is None:
                wx.info("[Get_RT_Data][{}/{}] {} [{}]逐笔交易数据 为空".format(icount + 1, len(rt.id_arr), id, time_str))
            else:
                wx.info("[Get_RT_Data][{}/{}] {} [{}--{}]逐笔交易数据[{}]".format(icount + 1, len(rt.id_arr), id, time_range[0],
                                                                            time_range[1], time_str))

        # 计算下一个循环的 起始时间， 和 文件记录时间，并调整
        begin_time_stamp += time_inc*60
        ret_zone = my_timer.tell_time_zone(t_stamp=begin_time_stamp)
        if ret_zone[0] == -3:
            begin_time_stamp = ret_zone[1]
        elif ret_zone[0] > 0: # 在交易时间内，文件记录时间取整
            record_stamp = ret_zone[2]
        elif ret_zone[0] == -5:
            record_stamp = ret_zone[2]

        # begin_time_stamp == end_time_stamp 再进行一次循环
        # begin_time_stamp > end_time_stamp 且差值 在 time_inc * 60 秒内，设置 begin == end
        if begin_time_stamp - end_time_stamp > 0 and begin_time_stamp-end_time_stamp < time_inc*60:
            begin_time_stamp = end_time_stamp

    # 文件记录最近一次的实时交易数据时间
    time_str = time.strftime("%H:%M", time.localtime(record_stamp))
    rt.f_record.write('\n'+time_str)
    rt.f_record.flush()
    return True
    # 获得当前时间，作为查询实时交易数据的时间节点
    # time_str = time.strftime("%H:%M:%S", time.localtime())
    # time_str = (datetime.datetime.now()).strftime("%H:%M:%S")
    # 时间偏移到交易时间，测试用途
    # time_str = (datetime.now()+timedelta(hours=-11)).strftime("%H:%M:%S")


def ana_rt_data(rt=None, big_bl_df = None, pa_bl_df =None):
    analyzer = rt_ana()
    big_cmp_result = analyzer.rt_cmp_big_baseline(rt = rt, big_bl_df=big_bl_df)
    analyzer.db_load_into_rt_msg(cmp_df = big_cmp_result)

# rt实时对象，src 数据源
# 利用全局RT 对象完成 数据收集
# 创建BL 对象完成 基线设定、导入数据库
def rebase_rt_data(rt=None, src='', date_str = None):
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

    begin_time_arr= ['09:30','10:05','10:35','11:05','13:05','13:35','14:05','14:35']#
    # end_time_arr  = ['09:40','10:30','11:00','11:30','13:30','14:00','14:30','15:00']#
    end_time_arr  = ['10:00','10:30','11:00','11:30','13:30','14:00','14:30','15:00']#

    # 保持全部的 baseline 数据，去极值后，一次性导入数据库
    final_bl_big_deal_df = pd.DataFrame()
    # 保持全部的 baseline 数据，一次性导入数据库
    final_bl_pa_df = pd.DataFrame()

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

        # 大单交易的 基线数据，每半小时产生一次
        baseline_big_deal_df = bl.baseline_big_deal(rt=rt, date_str=date_str, time_frame_arr=[begin_time_arr[index], end_time_arr[index]])
        if final_bl_big_deal_df is None or len(final_bl_big_deal_df) == 0:
            final_bl_big_deal_df = baseline_big_deal_df
        else:
            final_bl_big_deal_df = final_bl_big_deal_df.append(baseline_big_deal_df)

        # 量价 基线数据，每半小时产生一次
        baseline_PA_df = bl.baseline_PA(rt=rt, date_str=date_str, time_frame_arr=[begin_time_arr[index], end_time_arr[index]])
        if final_bl_pa_df is None or len(final_bl_pa_df) == 0:
            final_bl_pa_df = baseline_PA_df
        else:
            final_bl_pa_df = final_bl_pa_df.append(baseline_PA_df)

        # 释放 RT 对象的内部变量，只保留 最后15分钟的交易数据
        rt.clr_rt_data(minutes=15)
    # for循环结束，进入下一个时间段（半小时）

    # 全天交易时间结束，将基线数据导入数据库
    # 导入数据库
    bl.db_load_baseline_big_deal(df=final_bl_big_deal_df)

    # 再次对全天的 PA 数据去极值
    final_bl_pa_df = bl._clr_extreme_data(pa_df=final_bl_pa_df)
    bl.db_load_baseline_PA(df=final_bl_pa_df)





