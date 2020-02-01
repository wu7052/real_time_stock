import new_logger as lg
from datetime import datetime, timedelta
import pandas as pd
import re
import time
import json
from jsonpath import jsonpath
import urllib3
import chardet
from urllib import parse
import os

# http://push2ex.eastmoney.com/getStockFenShi?pagesize=144&ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wzfscj&
# cb=jQuery11230395420136369387_1580300330739&pageindex=0&id=3000592&sort=1&ft=1&code=300059&market=0&_=1580300330740

class rt_east:
    def __init__(self, id_arr=None, date_str=None, item_page = '144'):
        wx = lg.get_handle()
        if id_arr is None:
            wx.info("[RT_East] id_arr is None , __init__ EXIT !")
            return None
        else:
            self.id_arr = id_arr
        #     self.items_page = items_page
        #     self.t_frame = time_frame

        if date_str is None or len(date_str) == 0:
            self.date_str = (datetime.today()).strftime('%Y%m%d')
        else:
            self.date_str = date_str

        self.item_page = item_page

        self.f_record_name = self.date_str + "_east"
        self.f_record = open(self.f_record_name, "a+")

        # DataFrame 字典， key = id, value = Dataframe
        # 用来保存 通过rt_east获取的各股票的实时数据DataFrame
        self.rt_dict_df = {}

        # 用来保存 通过rt_east 查询的页数
        self.rt_page_df = {}

        # 建立数据库对象
        # self.db = db_ops()

        # 读取数据表名
        # self.h_conf = conf_handler(conf="rt_analyer.conf")

        # 大单 金额下限
        # self.rt_big_amount = float(self.h_conf.rd_opt('rt_analysis_rules', 'big_deal_amount'))


    def __del__(self):
        self.f_record.close()

    def _get_last_record_(self, l_num=-1):
        """
        get last line of a file
        :param filename: file name
        :return: last line or None for empty file
        """
        wx = lg.get_handle()
        try:
            filesize = os.path.getsize(self.f_record_name)
            if filesize == 0:
                return None
            else:
                with open(self.f_record_name, 'rb') as fp:  # to use seek from end, must use mode 'rb'
                    offset = -10  # initialize offset
                    while -offset < filesize:  # offset cannot exceed file size
                        fp.seek(offset, 2)  # read # offset chars from eof(represent by number '2')
                        lines = fp.readlines()  # read from fp to eof
                        if len(lines) >= 2:  # if contains at least 2 lines
                            return lines[l_num].decode()  # then last line is totally included
                        else:
                            offset *= 2  # enlarge offset
                    fp.seek(0)
                    lines = fp.readlines()
                    return lines[-1].decode()
        except FileNotFoundError:
            wx.info(self.f_record_name + ' not found!')
            return None

    # 添加监控的股票
    def add_id(self, id = None):
        wx = lg.get_handle()
        if id is None or len(id)==0 :
            wx.info("[RT_East] 增加股票ID为空")
            return False
        self.id_arr.append(id)
        return True

    # 清除 RT 对象内部的 rt_dict_df 数据
    # minutes = 0 清除所有数据
    # minutes = 其他数值： 当前最后一条记录的 minutes 分钟之前的数据全部清除
    def clr_rt_data(self, minutes=0):
        wx = lg.get_handle()
        if minutes == 0 :
            wx.info("[RT_East][clr_rt_data] 清理RT对象 全部的详细交易数据")
            self.rt_dict_df.clear()
        else:
            wx.info("[RT_East][clr_rt_data] RT对象清理 [{}]分钟前的 详细交易数据".format(minutes))
            for key in self.rt_dict_df.keys():
                clr_timestamp = self.rt_dict_df[key]['time_stamp'].max()-minutes*60
                self.rt_dict_df[key].reset_index(drop=True, inplace=True)
                self.rt_dict_df[key].drop(index=self.rt_dict_df[key][self.rt_dict_df[key].ix[:, 'time_stamp'] <=clr_timestamp].index, inplace=True)

    def get_json_str(self, id, time_str = None):
        wx = lg.get_handle()

        if time_str is None or len(time_str)<11:
            wx.info("[RT_East][get_json_str] 时间段 不正确，退出")
            return None
        else:
            [begin_time_str, end_time_str] = time_str.split("-")
            begin_time_stamp = int(time.mktime(time.strptime(self.date_str + begin_time_str, '%Y%m%d%H:%M')))
            end_time_stamp = int(time.mktime(time.strptime(self.date_str + end_time_str, '%Y%m%d%H:%M')))

        # 如果ID没有查询过，设置 rt_page_df[id] = 0,下面每次循环完成后，rt_page_df[id]累加1
        if id not in self.rt_page_df.keys():
            self.rt_page_df[id] = 0

        market_code_dict = {'60':['1','1'],'00':['2','0'],'30':['2','0'],'68':['1','0']}

        while True:
            url = "http://push2ex.eastmoney.com/getStockFenShi?pagesize="+self.item_page+\
                  "&ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wzfscj&" \
                  "cb=jQuery1123021130998143685753_1580471904475&pageindex="+str(self.rt_page_df[id])+\
                  "&id="+id+ market_code_dict[id[0:2]][0]+"&" \
                  "sort=1&ft=1&code="+id+"&market="+market_code_dict[id[0:2]][1]+"&_=1580471904476"

            # sort =1 升序 ； 2 降序；

            header = {
                'Cookie': 'UM_distinctid=16bf36d52242f3-0693469a5596d3-e323069-1fa400-16bf36d5225362; _ntes_nnid=16b2182ff532e10833492eedde0996df,1563157161323; _ntes_nuid=16b2182ff532e10833492eedde0996df; vjuids=e0fb8aa0.16d4ee83324.0.e074eccb150e; P_INFO=ghzhy1212@163.com|1570190476|0|mail163|00&99|hen&1570190062&mail163#CN&null#10#0#0|&0|mail163|ghzhy1212@163.com; nts_mail_user=ghzhy1212@163.com:-1:1; mail_psc_fingerprint=8da65e9cc5769a658a69962d94f7c46f; _ntes_usstock_recent_=NTES%7C; _ntes_usstock_recent_=NTES%7C; vjlast=1568986903.1571018378.11; s_n_f_l_n3=e119c348b08890ac1571018378289; NNSSPID=0e35f22546f44023b00d65e2a3ca1f26; ne_analysis_trace_id=1571018721010; _ntes_stock_recent_=1002699%7C0600000%7C1000573; _ntes_stock_recent_=1002699%7C0600000%7C1000573; _ntes_stock_recent_=1002699%7C0600000%7C1000573; pgr_n_f_l_n3=e119c348b08890ac1571018815386610; vinfo_n_f_l_n3=e119c348b08890ac.1.5.1563157161368.1570632456351.1571018833379',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
                'Referer': 'http://quote.eastmoney.com/f1.html?code=' +id + '&market=2'
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

            [page_start_time_str, page_end_time_str]=self.json_parse(id=id, json_str= unicode)
            page_end_time_stamp = int(time.mktime(time.strptime(self.date_str + page_end_time_str, '%Y%m%d%H:%M:%S')))

            # 页数累加，下次从此页号开始查询
            self.rt_page_df[id] += 1

            if page_end_time_stamp >= end_time_stamp:
                wx.info("[RT_East][{}] 第{}页 [{}--{}]逐笔交易数据，已获得目标时间段[{}--{}-{}]".
                                format(id, self.rt_page_df[id], page_start_time_str, page_end_time_str,
                                       self.date_str, begin_time_str, end_time_str))
                break
            else:
                wx.info("[RT_East][{}] 第{}页 [{}--{}]逐笔交易数据，目标时间段[{}--{}-{}]，继续获取下一页".
                        format(id, self.rt_page_df[id], page_start_time_str, page_end_time_str,
                               self.date_str, begin_time_str, end_time_str))

        return unicode


    def json_parse(self, id=None, json_str=None):
        wx = lg.get_handle()
        if id is None:
            wx.info("[RT_East][json_parse] 传入参数 股票ID 为空，退出")
            return None
        json_str = re.sub(r'jQuery\w+\(', r'', json_str)[:-2]
        if json_str is not None:
            json_obj = json.loads(json_str)
        if json_obj is None:
            wx.info("[RT_East][json_parse] JSON 对象为空，退出")
            return None
        # begin_time_str = json_obj['begin']
        # end_time_str = json_obj['end']
        # if len(json_obj['zhubi_list']) == 0:
        #     return None
        time_str = jsonpath(json_obj, '$..data..t')
        price_str = jsonpath(json_obj, '$..data..p')
        vol_str = jsonpath(json_obj, '$..data..v')
        dir_str = jsonpath(json_obj, '$..data..bs')
        rt_data = [dir_str, price_str,vol_str,time_str ]
        df = pd.DataFrame(rt_data)
        df1 = df.T
        df1.rename(columns={0: 'type', 1: 'price', 2: 'vol', 3: 'time_str'}, inplace=True)
        # 删除集合竞价未成交的记录
        df1 = df1[~(df1['type'].isin([4]))]
        # -1 卖盘内盘 ； 1 卖票外盘， 与163 保持统计
        df1['type'] = df1['type'].map({1: int(-1), 2: int(1)})
        df1['time_str'] = df1['time_str'].astype('str')
        df1['time_str'] = df1['time_str'].apply(lambda x: ':'.join([x[:-4], x[-4:]]))
        df1['time_str'] = df1['time_str'].apply(lambda x: ':'.join([x[:-2], x[-2:]]))
        df1['time_stamp'] = df1['time_str'].apply(lambda x: int(time.mktime(time.strptime(self.date_str + x, '%Y%m%d%H:%M:%S'))))
        df1['price'] = df1['price'] / 1000
        df1['vol'] = df1['vol'] * 100
        # date = [re.sub(r'-', '', tmp[0:10]) for tmp in dt]
        # type = jsonpath(json_obj, '$..TRADE_TYPE')
        # price = jsonpath(json_obj, '$..PRICE')
        # vol = jsonpath(json_obj, '$..VOLUME_INC')
        # time_stamp = jsonpath(json_obj, '$..DATE..sec')
        # time_stamp_usec = jsonpath(json_obj, '$..DATE..usec')
        # time_str = jsonpath(json_obj, '$..DATE_STR')

        # rt_163_data = [seq, type, price, vol, time_stamp, time_str]
        # rt_163_data = [seq, type, price, vol, time_stamp, time_stamp_usec, time_str]
        # df = pd.DataFrame(rt_163_data)

        ret_time_arr = [df1.time_str.min(), df1.time_str.max()]
        if id in self.rt_dict_df.keys():
            self.rt_dict_df[id] = self.rt_dict_df[id].append(df1, sort=False).drop_duplicates()
            self.rt_dict_df[id] = self.rt_dict_df[id].sort_values(by="time_str", ascending=False)
        else:
            self.rt_dict_df[id] = df1

        return ret_time_arr