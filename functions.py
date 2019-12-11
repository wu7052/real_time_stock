from realtime_package import rt_163, rt_east, rt_sina, rt_ana
import pandas as pd
from datetime import datetime, date, timedelta
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
    # time_str = time.strftime("%H:%M:%S", time.localtime())
    # time_str = (datetime.datetime.now()).strftime("%H:%M:%S")
    # 时间偏移到交易时间，测试用途
    time_str = (datetime.now()+timedelta(hours=-6)).strftime("%H:%M:%S")

    # rt 对象在主函数生成，传入此函数，添加
    if src == '163':
        wx.info("[Get_RT_Data] [{}]从 [{}] 交易数据共 [{}] 支股票".format(src, time_str, len(rt.id_arr) ))

    for icount, id in enumerate(rt.id_arr):
        # wx.info("[Get_RT_Data][{}:{}] {} 获取逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
        json_str = rt.get_json_str(id=id, time_str=time_str)
        # wx.info("[Get_RT_Data][{}:{}] {} 解析逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id, time_str))
        time_range = rt.json_parse(id=id, json_str=json_str)
        wx.info("[Get_RT_Data][{}/{}] {} [{}--{}]逐笔交易数据[{}]".format(icount+1,len(rt.id_arr),id,time_range[0],time_range[1], time_str))


def ana_rt_data(ana=None, rt=None):
    ana.rt_analyzer(rt = rt)

def rebase_rt_data(id_arr = None, rt=None):
    wx = lg.get_handle()
    db = db_ops()