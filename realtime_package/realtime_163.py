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
        # self.db = db_ops()

        # 读取数据表名
        # self.h_conf = conf_handler(conf="rt_analyer.conf")

        # 大单 金额下限
        # self.rt_big_amount = float(self.h_conf.rd_opt('rt_analysis_rules', 'big_deal_amount'))



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
                clr_timestamp = self.rt_dict_df[key]['time_stamp'].max()-minutes*60
                self.rt_dict_df[key].reset_index(drop=True, inplace=True)
                self.rt_dict_df[key].drop(index=self.rt_dict_df[key][self.rt_dict_df[key].ix[:, 'time_stamp'] <=clr_timestamp].index, inplace=True)
                # data1.drop(index=data1[data1.ix[:, 'sorce'] == 61].index, inplace=True)

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
        time_stamp = jsonpath(json_obj, '$..DATE..sec')
        # time_stamp_usec = jsonpath(json_obj, '$..DATE..usec')
        time_str = jsonpath(json_obj, '$..DATE_STR')

        rt_163_data = [seq, type, price, vol, time_stamp, time_str]
        # rt_163_data = [seq, type, price, vol, time_stamp, time_stamp_usec, time_str]
        df = pd.DataFrame(rt_163_data)
        df1 = df.T
        df1.rename(columns={0: 'seq', 1: 'type', 2: 'price', 3: 'vol',
                            4: 'time_stamp', 5: 'time_str'}, inplace=True)
        ret_time_arr = [df1.time_str.min(), df1.time_str.max()]
        if id in self.rt_dict_df.keys():
            self.rt_dict_df[id] = self.rt_dict_df[id].append(df1, sort=False).drop_duplicates()
            self.rt_dict_df[id] = self.rt_dict_df[id].sort_values(by="time_str", ascending=False)
        else:
            self.rt_dict_df[id] = df1
        return ret_time_arr

        # df1 = self.rt_dict_df[id].drop_duplicates()
        # return df1

        # df = df.drop_duplicates( subset = ['YJML', 'EJML', 'SJML', 'WZLB', 'GGXHPZ', 'CGMS'],# 去重列，按这些列进行去重
        # 　　keep = 'first'  # 保存第一条重复数据
        # )