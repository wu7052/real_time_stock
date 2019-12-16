from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import re
from datetime import datetime, timedelta
import pandas as pd
import time
import json
from jsonpath import jsonpath
import urllib3
import chardet
from urllib import parse
from tushare_data import ts_data

class rt_163:
    def __init__(self, id_arr=None, date_str = None): #,, items_page=200):  time_frame=180):
        wx = lg.get_handle()
        if id_arr is None:
            wx.info("[rt_163] id_arr is None , __init__ EXIT !")
            return None
        else:
            self.id_arr = id_arr
        #     self.items_page = items_page
        #     self.t_frame = time_frame

        if date_str is None:
            self.date_str = (datetime.today()).strftime('%Y-%m-%d')
        else:
            self.date_str = date_str

        # DataFrame 字典， key = id, value = Dataframe
        # 用来保存 通过rt_163获取的各股票的实时数据DataFrame
        self.rt_dict_df = {}

        # 建立数据库对象
        self.db = db_ops()

        # 读取数据表名
        self.h_conf = conf_handler(conf="rt_analyer.conf")
        self.cq_tname_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
        self.cq_tname_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
        self.cq_tname_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
        self.cq_tname_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')
        self.cq_tname_68 = self.h_conf.rd_opt('db', 'daily_table_cq_68')

        # 大单 金额下限
        self.rt_big_amount = float(self.h_conf.rd_opt('rt_analysis_rules', 'big_deal_amount'))

    # 获得监控股票的前 N 天的成交量数据
    def get_std_PV(self):
        wx = lg.get_handle()
        all_t_name = [self.cq_tname_00, self.cq_tname_30, self.cq_tname_60, self.cq_tname_002, self.cq_tname_68]
        std_days = self.h_conf.rd_opt('general', 'std_days')

        std_date = self.db.get_trade_date(back_days= std_days)
        id_arr_str = (",".join(self.id_arr))
        std_df = pd.DataFrame()
        wx.info("[rt_163][get_std_PV] 开始为 [{}支股票] 设立基线数据，以 [{}] 为基线数据统计的开始日期".
                format(len(self.id_arr),std_date))
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
        wx.info("[rt_163][get_std_PV] [{}支股票]的基线数据设立完毕".format(len(self.id_arr)))


    # 添加监控的股票
    def add_id(self, id = None):
        wx = lg.get_handle()
        if id is None or len(id)==0 :
            wx.info("[rt_163] 增加股票ID为空")
            return False
        self.id_arr.append(id)
        return True

    # 清除 RT 对象内部的 rt_dict_df 数据
    # minutes = 0 清除所有数据
    # minutes = 其他数值： 当前最后一条记录的 minutes 分钟之前的数据全部清除
    def clr_rt_data(self, minutes=0):
        wx = lg.get_handle()
        if minutes == 0 :
            wx.info("[rt_163][clr_rt_data] 清理RT对象 全部的详细交易数据")
            self.rt_dict_df.clear()
        else:
            wx.info("[rt_163][clr_rt_data] RT对象清理 [{}]分钟前的 详细交易数据".format(minutes))
            # clr_timestamp = int(time.time())-minutes*60
            for key in self.rt_dict_df.keys():
                clr_timestamp = self.rt_dict_df[key]['time_stamp_sec'].max()-minutes*60
                self.rt_dict_df[key].reset_index(drop=True, inplace=True)
                self.rt_dict_df[key].drop(index=self.rt_dict_df[key][self.rt_dict_df[key].ix[:, 'time_stamp_sec'] <=clr_timestamp].index, inplace=True)
                # data1.drop(index=data1[data1.ix[:, 'sorce'] == 61].index, inplace=True)

    # 建立成交量、大单数量、大单金额占比 的基线数据，并导入数据库
    # 每天4个小时建立四条基线，方便同比\环比
    # time_frame_arr ['09:30','10:30'] 起止时间段
    def baseline_big_deal(self, date_str=None, time_frame_arr=None ):
        wx = lg.get_handle()
        if date_str is None:
            wx.info("[rt_163][baseline_big_deal] 大单基线设立日期：今天")
            date_str = datetime.now().strftime("%Y%m%d")
        else:
            wx.info("[rt_163][baseline_big_deal] 大单基线设立日期：{}".format(date_str))

        begin_t_stamp = int(time.mktime(time.strptime(date_str+time_frame_arr[0], "%Y%m%d%H:%M")))
        end_t_stamp = int(time.mktime(time.strptime(date_str+time_frame_arr[1], "%Y%m%d%H:%M")))

        # 基线起止时间颠倒，互换
        if begin_t_stamp > end_t_stamp:
            begin_t_stamp, end_t_stamp = end_t_stamp, begin_t_stamp

        baseline_big_deal_df = pd.DataFrame()
        for id in self.rt_dict_df.keys():
            # rt_end_time = self.rt_dict_df[id]['time_stamp_sec'].max()
            # rt_begin_time = self.rt_dict_df[id]['time_stamp_sec'].min()
            # rt_begin_timestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rt_begin_time))
            # rt_end_timestr = time.strftime("%H:%M:%S", time.localtime(rt_end_time))

            # if begin_t_stamp < rt_begin_time or end_t_stamp > rt_end_time:
            #     wx.info("[rt_163][baseline_big_deal] [{}] 设定的基线时间段 [{}-{}] 大于实时数据时间范围 [{}-{}],退出"
            #             .format(time_frame_arr[0], time_frame_arr[1], rt_begin_timestr, rt_end_timestr))
                # return None

            wx.info("[rt_163][baseline_big_deal]开始更新[{}]的数据基线[{}-{}]".format(id, time_frame_arr[0], time_frame_arr[1]))

            # 从RT 数据中筛选出 基线时间段 内的交易记录
            rt_df = self.rt_dict_df[id].loc[ (self.rt_dict_df[id]['time_stamp_sec'] >= begin_t_stamp) & ( self.rt_dict_df[id]['time_stamp_sec'] <= end_t_stamp)].copy()

            if rt_df is None or rt_df.empty:
                wx.info("[rt_163][baseline_big_deal] [{}] 在[{}-{}]期间交易数据为空，开始处理下一支股票".format(id, time_frame_arr[0], time_frame_arr[1]))
                continue

            # ID 的所有成交量
            rt_df['amount'] = rt_df['vol'] * rt_df['price']
            rt_df['io_amount'] = rt_df['amount'] * rt_df['type']
            rt_amount = rt_df['amount'].sum()

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
                           "big_buy_pct":rt_big_buy_amount_pct, "big_sell_pct":rt_big_sell_amount_pct}

            if baseline_big_deal_df is None or baseline_big_deal_df.empty:
                baseline_big_deal_df = pd.DataFrame([rt_baseline])
            else:
                baseline_big_deal_df = baseline_big_deal_df.append(pd.DataFrame([rt_baseline]))

        if baseline_big_deal_df is None or baseline_big_deal_df.empty:
            wx.info("[rt_163][baseline_big_deal] [{}-{}] 基线交易数据为空，退出".format(time_frame_arr[0], time_frame_arr[1]))
            return None
        else:
            cols = ['id','date','t_frame','big_qty','big_abs_pct','big_io_pct','big_buy_pct','big_sell_pct']
            baseline_big_deal_df = baseline_big_deal_df.loc[:,cols]
            baseline_big_deal_df.fillna(0,inplace=True)
            baseline_big_deal_df.reset_index(drop=True, inplace=True)
            wx.info("[rt_163][baseline_big_deal] [{}-{}]数据基线更新完毕".format(time_frame_arr[0], time_frame_arr[1]))
            return baseline_big_deal_df

    def db_load_baseline_big_deal(self, df = None):
        wx = lg.get_handle()
        if df is None or df.empty:
            wx.info("[rt_163][db_load_baseline_big_deal] 导入数据DataFrame 为空，退出")
            return
        self.db.db_load_into_RT_BL_Big_Deal(df=df)
        wx.info("[rt_163][db_load_baseline_big_deal] 大单交易数据{}条 导入数据库完成".format(len(df)))

    def url_encode(self, str):
        return parse.quote(str)

    def get_json_str(self, id, time_str=''):
        wx = lg.get_handle()
        if time_str == '':
            wx.info("[rt_163][get_json_str] 查询时间为空，退出!")
            return None
        url = "http://quotes.money.163.com/service/zhubi_ajax.html?symbol="+id+"&end="+time_str  #10%3A00%3A00"

        header = {
            'Cookie': 'UM_distinctid=16bf36d52242f3-0693469a5596d3-e323069-1fa400-16bf36d5225362; _ntes_nnid=16b2182ff532e10833492eedde0996df,1563157161323; _ntes_nuid=16b2182ff532e10833492eedde0996df; vjuids=e0fb8aa0.16d4ee83324.0.e074eccb150e; P_INFO=ghzhy1212@163.com|1570190476|0|mail163|00&99|hen&1570190062&mail163#CN&null#10#0#0|&0|mail163|ghzhy1212@163.com; nts_mail_user=ghzhy1212@163.com:-1:1; mail_psc_fingerprint=8da65e9cc5769a658a69962d94f7c46f; _ntes_usstock_recent_=NTES%7C; _ntes_usstock_recent_=NTES%7C; vjlast=1568986903.1571018378.11; s_n_f_l_n3=e119c348b08890ac1571018378289; NNSSPID=0e35f22546f44023b00d65e2a3ca1f26; ne_analysis_trace_id=1571018721010; _ntes_stock_recent_=1002699%7C0600000%7C1000573; _ntes_stock_recent_=1002699%7C0600000%7C1000573; _ntes_stock_recent_=1002699%7C0600000%7C1000573; pgr_n_f_l_n3=e119c348b08890ac1571018815386610; vinfo_n_f_l_n3=e119c348b08890ac.1.5.1563157161368.1570632456351.1571018833379',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
            'Referer': 'http://quotes.money.163.com/trade/cjmx_'+id+'.html'
        }

        # requests.packages.urllib3.disable_warnings()
        http = urllib3.PoolManager()
        try:
            raw_data = http.request('GET', url, headers=header)
        except Exception as e:
            return None
        finally:
            if raw_data.status >= 300:
                wx.info("Web response failed : {}".format(url))
                return None

        # 获得html源码,utf-8解码
        str_type = chardet.detect(raw_data.data)
        # unicode = raw_data.data.decode(str_type['encoding'])
        unicode = lg.str_decode(raw_data.data, str_type['encoding'])
        return unicode

    def json_parse(self, id=None, json_str=None):
        wx = lg.get_handle()
        if id is None:
            wx.info("[RT_163][json_parse] 传入参数 股票ID 为空，退出")
            return None
        if json_str is not None:
            json_obj = json.loads(json_str)
        begin_time_str = json_obj['begin']
        end_time_str = json_obj['end']
        if len(json_obj['zhubi_list']) == 0:
            return None
        seq = jsonpath(json_obj, '$.._id..$id')
        # date = [re.sub(r'-', '', tmp[0:10]) for tmp in dt]
        type = jsonpath(json_obj, '$..TRADE_TYPE')
        price = jsonpath(json_obj, '$..PRICE')
        vol = jsonpath(json_obj, '$..VOLUME_INC')
        time_stamp_sec = jsonpath(json_obj, '$..DATE..sec')
        # time_stamp_usec = jsonpath(json_obj, '$..DATE..usec')
        time_str = jsonpath(json_obj, '$..DATE_STR')

        rt_163_data = [seq, type, price, vol, time_stamp_sec, time_str]
        # rt_163_data = [seq, type, price, vol, time_stamp_sec, time_stamp_usec, time_str]
        df = pd.DataFrame(rt_163_data)
        df1 = df.T
        df1.rename(columns={0: 'seq', 1: 'type', 2: 'price', 3: 'vol',
                            4: 'time_stamp_sec', 5: 'time_str'}, inplace=True)
        ret_time_arr = [df1.time_str.min(), df1.time_str.max()]
        if id in self.rt_dict_df.keys():
            self.rt_dict_df[id] = self.rt_dict_df[id].append(df1).drop_duplicates()
            self.rt_dict_df[id] = self.rt_dict_df[id].sort_values(by="time_str", ascending=False)
        else:
            self.rt_dict_df[id] = df1
        return ret_time_arr

        # df1 = self.rt_dict_df[id].drop_duplicates()
        # return df1

        # df = df.drop_duplicates( subset = ['YJML', 'EJML', 'SJML', 'WZLB', 'GGXHPZ', 'CGMS'],# 去重列，按这些列进行去重
        # 　　keep = 'first'  # 保存第一条重复数据
        # )