from realtime_package import rt_163, rt_east, rt_sina
from stock_package import ts_data, sz_web_data, sh_web_data, ex_web_data, ma_kits
import pandas as pd
from datetime import datetime, date, timedelta
import time
import new_logger as lg
import re
from conf import conf_handler, xl_handler
# from db_package import db_ops
from realtime_package import rt_163

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


# 判断 某个日期 是否交易日, date 默认是当日
def is_trading_date(date_str=''):
    ts = ts_data()
    if date_str == '':
        date_str = (date.today()).strftime('%Y%m%d')

    if ts.is_open(date_str = date_str):
        return True
    else:
        return False

# 获取实时数据
# src 数据源 ， t_frame 读取间隔时间， id_arr 股票ID代码数组
def acq_rt_data(rt=None, src='', t_frame=180):
    wx = lg.get_handle()

    # 股票代码数组，由 rt 对象内部变量 带入
    # if id_arr is None:
    #     wx.info("[Acq_RT_Data] 股票列表为空，退出")
    #     return None

    # rt 对象在主函数生成，传入此函数，添加
    # if src == '163':
    #     wx.info("[Acq_RT_Data] 开始从 [{}] 读取实时交易数据共 [{}] 支股票".format(src, len(id_arr) ))
    #     rt = rt_163(date_str='')

    for id in rt.id_arr:
    # url = rt_163.url_encode("14:00:00")
        json_str = rt.get_json_str(id=id)
        rt_df = rt.json_parse(id=id, json_str=json_str)




@wx_timer
def update_daily_data_from_sina(date=None):  # date 把数据更新到指定日期，默认是当天
    wx = lg.get_handle()
    # sz_data = sz_web_data()
    # sh_data = sh_web_data()
    web_data = ex_web_data()
    page_src = (('zxqy', '002%', '中小板'), ('cyb', '30%', '创业板'),
                ('sz_a', '00%', '深证 主板'), ('sh_a', '60%', '上证 主板'))

    try:
        for src in page_src:
            # 上证 主板 、深证 主板 、中小板、 创业板
            page_counter = 1
            loop_flag = True
            while loop_flag:
                wx.info("===" * 20)
                wx.info("[update_daily_data_from_sina] downloading {} Page {} ".format(src[2], page_counter))
                sina_daily_url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/" \
                                 "Market_Center.getHQNodeData?page=" + str(page_counter) + "&num=80&sort=symbol&" \
                                                                                           "asc=1&node=" + src[
                                     0] + "&symbol=&_s_r_a=page"

                daily_str = web_data.get_json_str(url=sina_daily_url, web_flag='sh_basic')
                # daily_str = daily_str[1:-1]
                found = re.match(r'.*symbol', daily_str)
                if found is None:
                    wx.info("[update_daily_data_from_sina] didn't acquire the data from page {}".format(page_counter))
                    break
                else:
                    page_counter += 1

                # 深证A股 的字符串包含 主板、中小板、创业板
                # 'sz_a' 标志截断 中、创
                if src[0] == 'sz_a':
                    trunc_pos = daily_str.find(',{symbol:"sz002')
                    if trunc_pos >= 0:
                        daily_str = daily_str[:trunc_pos] + ']'
                        loop_flag = False  # 完成本次后，退出循环

                # key字段 加引号，整理字符串
                jstr = re.sub(r'([a-z|A-Z]+)(?=:)', r'"\1"', daily_str)

                # 按股票拆分 成list， 这里把整个页面的股票数据统一处理成 dataframe，不做拆分了
                # d_arr = re.findall('{\S+?}',jstr)
                if date is None:
                    today = datetime.now().strftime('%Y%m%d')
                else:
                    today = date

                # 深证 A 股页面 包含了 主板、创业、中小， 所以处理 深证主板的时候，要把 创业、中小 的股票信息去掉
                daily_data_frame = web_data.sina_daily_data_json_parse(json_str=jstr, date=today)
                web_data.db_load_into_daily_data(dd_df=daily_data_frame, pre_id=src[1], mode='basic', type='cq')
                web_data.db_load_into_daily_data(dd_df=daily_data_frame, pre_id=src[1], mode='basic', type='qfq')

    except Exception as e:
        wx.info("Err [update_daily_data_from_sina]: {}".format(e))

"""
# 按日期，一次性获得当日所有股票数据，从Tushare
"""
def update_dd_by_date_from_ts(q_date=''):
    wx = lg.get_handle()
    ts = ts_data()
    web_data = ex_web_data()
    name_arr = (('^002', '002%', '中小板'), ('^60', '60%', '上证 主板'), ('^00[0,1,3-9]', '00%', '深证 主板'),
                ('^30', '30%', '创业板'), ('^68', '68%', '科创板'))
    if q_date == '':
        wx.info("[update_dd_by_date_from_ts] 日期为空，退出" )
        return
    try:
        dd_df = ts.acquire_daily_data_by_date(q_date=q_date)
        while dd_df is None:
            wx.info("[update_dd_by_date_from_ts]从Tushare获取 {} 数据失败, 休眠10秒后重试 ...".format(q_date))
            time.sleep(10)
            dd_df = ts.acquire_daily_data_by_date(q_date=q_date)
    except Exception as e:
        wx.info("Err:[update_dd_by_date_from_ts]---{}".format(e))
    dd_df['ts_code'] = dd_df['ts_code'].apply(lambda x: x[0:6])

    """暂时屏蔽
    # 除权数据 导入数据表
    for name in name_arr:
        # 根据板块拆分 dataframe ，导入数据表
        df_tmp = dd_df[dd_df['ts_code'].str.contains(name[0])]
        # df_00 = dd_df[dd_df['ts_code'].str.contains("^00[0,1,3-9]")]
        # df_002 = dd_df[dd_df['ts_code'].str.contains("^002")]
        # df_60 = dd_df[dd_df['ts_code'].str.contains("^60")]
        # df_68 = dd_df[dd_df['ts_code'].str.contains("^68")]
        web_data.db_load_into_daily_data(dd_df=df_tmp, pre_id=name[1], mode='basic', type='cq')
    wx.info("[update_dd_by_date_from_ts] 除权数据已导入数据表，开始处理 前复权 数据")
    """

    end_datetime_str = (date.today()).strftime('%Y%m%d')
    end_datetime = datetime.strptime(end_datetime_str, '%Y%m%d')
    cur_datetime_str = q_date
    end_factor_df = ts.acquire_factor(date=end_datetime_str)
    while end_factor_df is None:
        wx.info("[update_dd_by_date_from_ts]获取最近日期的 复权因子失败，等待10秒，再次尝试...")
        time.sleep(10)
        end_factor_df = ts.acquire_factor(date=end_datetime_str)

    while end_factor_df.empty:
        end_datetime += timedelta(days=-1)
        end_datetime_str = end_datetime.strftime('%Y%m%d')
        wx.info("[update_dd_by_date_from_ts]获取 最近日期的 复权因子为空，向前一日获取{}".format(end_datetime_str))
        end_factor_df = ts.acquire_factor(date=end_datetime_str)

    cur_factor_df = ts.acquire_factor(date=cur_datetime_str)
    while cur_factor_df is None:
        wx.info("[update_dd_by_date_from_ts]获取{}的 复权因子失败，等待10秒，再次尝试...".format(cur_datetime_str))
        time.sleep(10)
        cur_factor_df = ts.acquire_factor(date=cur_datetime_str)

    # 左链接，合并 cur_factor_df / end_factor_df 两张表
    factor_tmp = pd.merge(cur_factor_df, end_factor_df, on='ts_code', how='left')
    factor_tmp.rename(
        columns={'ts_code': 'id', 'trade_date_x': 'date', 'adj_factor_x': 'cur_factor',
                 'trade_date_y': 'end_date', 'adj_factor_y': 'end_factor'}, inplace=True)

    # cur_factor_df 所有复权因子保留，期末的复权因子为空，则设置0
    factor_tmp.fillna(0, inplace=True)
    # 期末复权因子为空的股票，计入 异常清单
    factor_abnormal_df = pd.DataFrame()
    factor_abnormal_df = factor_abnormal_df.append(factor_tmp[(factor_tmp['end_factor'] == 0)])

    # 删除期末复权因子为空的记录
    factor_tmp = factor_tmp[~(factor_tmp['end_factor'].isin([0]))]

    # 复权因子相除
    factor_tmp['d_factor'] = factor_tmp['cur_factor'] / factor_tmp['end_factor']
    factor_tmp['id'] = factor_tmp['id'].apply(lambda x: x[0:6])

    dd_df.rename(
        columns={'ts_code': 'id', 'trade_date': 'date'}, inplace=True)

    dd_df = pd.merge(dd_df, factor_tmp, on=['id', 'date'], how='left')
    dd_df['open'] *= dd_df['d_factor']
    dd_df['high'] *= dd_df['d_factor']
    dd_df['low'] *= dd_df['d_factor']
    dd_df['close'] *= dd_df['d_factor']
    dd_df.drop(['cur_factor', 'end_date', 'end_factor', 'd_factor'], axis=1, inplace=True)
    dd_df.fillna(0, inplace=True)

    # 前复权数据 导入数据表
    for name in name_arr:
        # 根据板块拆分 dataframe ，导入数据表
        df_tmp = dd_df[dd_df['id'].str.contains(name[0])]
        # df_00 = dd_df[dd_df['ts_code'].str.contains("^00[0,1,3-9]")]
        # df_002 = dd_df[dd_df['ts_code'].str.contains("^002")]
        # df_60 = dd_df[dd_df['ts_code'].str.contains("^60")]
        # df_68 = dd_df[dd_df['ts_code'].str.contains("^68")]
        web_data.db_load_into_daily_data(dd_df=df_tmp, pre_id=name[1], mode='basic', type='qfq')
    wx.info("[update_dd_by_date_from_ts] 前复权数据已导入数据表，开始处理 异常 数据")

@wx_timer
def update_daily_data_from_eastmoney(date=None, supplement=False):
    wx = lg.get_handle()
    web_data = ex_web_data()
    page_src = (('C.23', '68%', '科创板'), ('C.13', '002%', '中小板'),
                ('C.2', '60%', '上证 主板'), ('C._SZAME', '00%', '深证 主板'),
                ('C.80', '30%', '创业板'))
    try:
        for src in page_src:
            page_count = 1
            items_page = 500
            loop_page = True
            while loop_page:
                east_daily_url = "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?cb=" \
                                 "jQuery1124048605539191859704_1549549980465&type=CT&" \
                                 "token=4f1862fc3b5e77c150a2b985b12db0fd&sty=FCOIATC&" \
                                 "js=(%7Bdata%3A%5B(x)%5D%2CrecordsFiltered%3A(tot)%7D)&" \
                                 "cmd=" + src[0] + "&st=(Code)&sr=1&p=" + str(page_count) + "&ps=" + str(items_page) + \
                                 "&_=1549549980528"

                east_daily_str = web_data.get_json_str(url=east_daily_url, web_flag='eastmoney')

                # 把字符串 拆分成 交易数据",,,,,",",,,,,",",,,,",",,,,,"  和 记录数量 两个部分
                east_daily_str = re.search(r'(?:data\:\[)(.*)(?:\]\D+)(\d+)(?:.*)', east_daily_str)
                daily_data = east_daily_str.group(1)  # 获得交易数据
                total_item = int(east_daily_str.group(2))  # 获得股票总数量，用来计算 页数
                total_page = int((total_item + items_page - 1) / items_page)  # 总页数，向上取整
                wx.info("[update daily data from eastmoney] {}-- page {}/ {}".format(src[2], page_count, total_page))
                page_count += 1
                if page_count > total_page:
                    loop_page = False

                # 把交易数据 进一步拆分成 每支股票交易数据一条字符串 的数组
                east_daily_data = re.findall(r'(?:\")(.*?)(?:\")', daily_data)
                page_arr = list()
                for daily_str in east_daily_data:
                    daily_arr = daily_str.split(',')
                    daily_arr.pop(0)  # 去掉 无意义的 第一个字段
                    if src[0] == 'C._SZAME' and re.match(r'^002', daily_arr[0]) is not None:  # 深圳主板发现 中小板
                        loop_page = False
                        break
                    else:
                        page_arr.append(daily_arr)
                        # wx.info("{}".format(daily_arr[0]))
                page_full_df = pd.DataFrame(page_arr, columns=['id', 'name', 'close', 'chg', 'pct_chg', 'vol', 'amount',
                                                               'pct_up_down',
                                                               'high', 'low', 'open', 'pre_close', 'unknown1', 'qrr',
                                                               'tor', 'pe', 'pb',
                                                               'total_amount', 'total_flow_amount', 'unknown4',
                                                               'unknown5',
                                                               'unknown6', "unknown7", "date", "unknown8"])
                if date is None:
                    page_full_df['date'] = datetime.now().strftime('%Y%m%d')
                else:
                    page_full_df['date'] = date

                if supplement:  # 只采集增补信息
                    page_db_df = page_full_df.loc[:, ['id', 'date', 'qrr', 'tor', 'pct_up_down', 'pe', 'pb']]
                else:  # 采集全部数据
                    page_db_df = page_full_df.loc[:, ['id', 'date', 'open', 'high', 'low', 'close', 'pre_close', 'chg',
                                                      'pct_chg', 'vol', 'amount', 'qrr', 'tor', 'pct_up_down', 'pe',
                                                      'pb']]
                    page_db_df['open'] = page_db_df['open'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['high'] = page_db_df['high'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['low'] = page_db_df['low'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['close'] = page_db_df['close'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pre_close'] = page_db_df['pre_close'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['chg'] = page_db_df['chg'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pct_chg'] = page_db_df['pct_chg'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['vol'] = page_db_df['vol'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['amount'] = page_db_df['amount'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['qrr'] = page_db_df['qrr'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['tor'] = page_db_df['tor'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pct_up_down'] = page_db_df['pct_up_down'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pe'] = page_db_df['pe'].apply(lambda x: '0' if str(x) == '-' else x)
                    page_db_df['pb'] = page_db_df['pb'].apply(lambda x: '0' if str(x) == '-' else x)

                    page_db_df['amount'] = pd.to_numeric(page_db_df['amount'])
                    page_db_df['amount'] = page_db_df['amount'] / 1000

                    page_db_df_qfq = page_db_df.loc[:, ['id', 'date', 'open', 'high', 'low', 'close', 'pre_close',
                                                        'chg', 'pct_chg', 'vol', 'amount']]
                    web_data.db_load_into_daily_data(dd_df=page_db_df, pre_id=src[1], mode='full', type='cq')
                    web_data.db_load_into_daily_data(dd_df=page_db_df_qfq, pre_id=src[1], mode='basic', type='qfq')

    except Exception as e:
        wx.info("Err [update_daily_data_from_eastmoney]: {}".format(e))


@wx_timer
def update_ind_ma_df(fresh=False, data_src='cq', bt_start_date='', bt_end_date=''):
    wx = lg.get_handle()
    web_data = ex_web_data()
    pre_id = ['68%', '002%', '30%', '00%', '60%']

    if data_src=='bt_qfq':
        if bt_end_date == '' or  bt_start_date == '':
            wx.info("[update_ind_ma_df] bt_qfq mode:  bt_start_date and bt_end_date Can't Empty, Err ")
            return
    ma = ma_kits()
    for pre in pre_id:
        # id_arr = web_data.db_fetch_stock_id(pre_id=pre)
        # if len(id_arr)>0:
        ma_ret = ma.calc_arr(pre_id=pre, fresh=fresh, data_src=data_src, bt_start_date=bt_start_date, bt_end_date=bt_end_date)
        if ma_ret is None or ma_ret.empty:
            wx.info("[update_ind_ma_df]=========== {} {} MA =========== Data Empty ".format(pre, data_src))
        else:
            wx.info("[update_ind_ma_df]=========== {} {} MA =========== Loading Data".format(pre, data_src))
            web_data.db_load_into_ind_xxx(ind_type='ma', ind_df=ma_ret, stock_type=pre, data_src=data_src)
            wx.info("[update_ind_ma_df]=========== {} {} MA =========== Data Loaded ALL ".format(pre, data_src))
        # else:
        #     wx.info("[update_ind_ma_df]=========== {} {} Stock =========== Empty [{}]".format(pre, data_src, len(id_arr)))

