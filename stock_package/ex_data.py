from db_package import db_ops

import urllib3
import requests
import chardet
# import logging
import os
from urllib import parse
import new_logger as lg
from datetime import datetime, time, date, timedelta
import time
import pandas as pd
import numpy as np
import json
from jsonpath import jsonpath
import re
from conf import conf_handler


class ex_web_data(object):

    def __init__(self):
        wx = lg.get_handle()
        # log_dir = os.path.abspath('.')
        # self.logger = myLogger(log_dir)
        try:
            self.h_conf = conf_handler(conf="rt_analyer.conf")
            host = self.h_conf.rd_opt('db', 'host')
            database = self.h_conf.rd_opt('db', 'database')
            user = self.h_conf.rd_opt('db', 'user')
            pwd = self.h_conf.rd_opt('db', 'pwd')
            self.db = db_ops(host=host, db=database, user=user, pwd=pwd)
            # self.db = db_ops(host='127.0.0.1', db='stock', user='wx', pwd='5171013')

            self.dd_cq_00 = self.h_conf.rd_opt('db', 'daily_table_cq_00')
            self.dd_cq_30 = self.h_conf.rd_opt('db', 'daily_table_cq_30')
            self.dd_cq_60 = self.h_conf.rd_opt('db', 'daily_table_cq_60')
            self.dd_cq_002 = self.h_conf.rd_opt('db', 'daily_table_cq_002')
            self.dd_cq_68 = self.h_conf.rd_opt('db', 'daily_table_cq_68')

            self.dd_qfq_00 = self.h_conf.rd_opt('db', 'daily_table_qfq_00')
            self.dd_qfq_30 = self.h_conf.rd_opt('db', 'daily_table_qfq_30')
            self.dd_qfq_60 = self.h_conf.rd_opt('db', 'daily_table_qfq_60')
            self.dd_qfq_002 = self.h_conf.rd_opt('db', 'daily_table_qfq_002')
            self.dd_qfq_68 = self.h_conf.rd_opt('db', 'daily_table_qfq_68')

            self.bt_dd_qfq_00 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_00')
            self.bt_dd_qfq_30 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_30')
            self.bt_dd_qfq_60 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_60')
            self.bt_dd_qfq_002 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_002')
            self.bt_dd_qfq_68 = self.h_conf.rd_opt('db', 'bt_daily_table_qfq_68')

            self.dd_hot_industry = self.h_conf.rd_opt('db', 'dd_hot_industry')
            # wx.info("[OBJ] ex_web_data : __init__ called")
        except Exception as e:
            raise e

    def __del__(self):
        # wx = lg.get_handle()
        self.db.cursor.close()
        self.db.handle.close()
        # wx.info("[OBJ] ex_web_data : __del__ called")

    def url_encode(self, str):
        return parse.quote(str)

    def daily_data_table_name(self):
        table_name = date.today().strftime('%Y%m')
        return table_name

    def db_fetch_stock_id(self, pre_id):
        # if pre_id == '00%':
        #     sql = "select id from list_a where id like '" + pre_id + "' and id not like '002%'"
        # else:
        sql = "select id from list_a where id like '" + pre_id + "'"
        id = self.db.cursor.execute(sql)
        id_array = self.db.cursor.fetchmany(id)
        return id_array

    def db_load_into_sw_industry(self, df_sw_industry=None):
        wx = lg.get_handle()
        if df_sw_industry is None:
            wx.info("[db_load_into_sw_industry] Err: SW DataFrame is Empty,")
            return -1
        sw_industry_arr = df_sw_industry.values.tolist()
        i = 0
        while i < len(sw_industry_arr):
            sw_industry_arr[i] = tuple(sw_industry_arr[i])
            i += 1
        sql = "REPLACE INTO sw_industry_code SET industry_code=%s, industry_name=%s, indicator_code=%s, level=%s"
        self.db.cursor.executemany(sql, sw_industry_arr)
        self.db.handle.commit()

    def db_get_sw_industry_code(self, level=2):
        wx = lg.get_handle()
        # 取第二级行业代码
        sql = "select industry_code from sw_industry_code where level = " + str(level)
        iCount = self.db.cursor.execute(sql)
        if iCount > 0:
            industry_code_arr = self.db.cursor.fetchall()
            return industry_code_arr
        else:
            return -1

    """
    #  插入 指标表 
    #  ind_type = 'ma' 移动均线 ； 'psy' 心理线
    """

    def db_load_into_ind_xxx(self, ind_type='ma', ind_df=None, stock_type=None, data_src='cq'):
        wx = lg.get_handle()
        if stock_type is None:
            wx.info("[db_load_into_ind_xxx] Err: {} Stock Type is Empty,".format(ind_type))
            return -1

        if ind_df is None or ind_df.empty:
            wx.info("[db_load_into_ind_xxx] Err: {} {} Data Frame is None or Empty".format(ind_type, stock_type))
            return -1
        ind_arry = ind_df.values.tolist()
        i = 0
        while i < len(ind_arry):
            ind_arry[i] = tuple(ind_arry[i])
            i += 1

        tname_00 = self.h_conf.rd_opt('db', ind_type+'_'+data_src+'_table_00')
        tname_30 = self.h_conf.rd_opt('db', ind_type+'_'+data_src+'_table_30')
        tname_60 = self.h_conf.rd_opt('db', ind_type+'_'+data_src+'_table_60')
        tname_002 = self.h_conf.rd_opt('db', ind_type+'_'+data_src+'_table_002')
        tname_68 = self.h_conf.rd_opt('db', ind_type+'_'+data_src+'_table_68')

        if re.match('002',stock_type) is not None:
            t_name = tname_002
        elif  re.match('00', stock_type) is not None :
            t_name = tname_00
        elif re.match('30', stock_type) is not None:
            t_name = tname_30
        elif  re.match('60', stock_type) is not None :
            t_name = tname_60
        elif re.match('68', stock_type) is not None:
            t_name = tname_68
        else:
            wx.info("[db_load_into_ind_xxx] stock_type does NOT match ('002','00','30','60')")

        if (ind_type == 'ma'):
            sql = "REPLACE INTO " + t_name + " SET id=%s, date=%s, ma_5=%s, ma_10=%s, ma_20=%s, ma_60=%s, " \
                                             "ma_13=%s, ma_34=%s, ma_55=%s, ema_12=%s, ema_26=%s, DIF=%s, DEA=%s, " \
                                             "bolling_mid=%s, bolling_top=%s, bolling_bottom=%s"
        elif (ind_type == 'psy'):
            sql = "REPLACE INTO " + t_name + " SET id=%s, date=%s, psy=%s"
        else:
            return None

        i_scale = 1000
        for i in range(0, len(ind_arry), i_scale):
            tmp_arry = ind_arry[i : i+i_scale]
            wx.info("[db_load_into_ind_xxx][{}] Loaded {} ~ {} , total {} " .format(t_name, i, i + i_scale, len(ind_arry)))
            self.db.cursor.executemany(sql, tmp_arry)
            self.db.handle.commit()


    def db_update_sw_industry_into_basic_info(self, code=None, id_arr=None):
        wx = lg.get_handle()
        if code is None or id_arr is None:
            wx.info(" Code {} or id_arr {} is None".format(code, id_arr))
            return -1
        sw_level_1 = code[0:2] + '0000'
        sw_level_2 = code

        for s_id in id_arr:
            sql = "update list_a set sw_level_1=%s , sw_level_2=%s where id = %s" % (sw_level_1, sw_level_2, s_id)
            self.db.cursor.execute(sql)
            self.db.handle.commit()
        # wx.info("SW Industry Code {} updated {} stocks ".format(code, len(id_arr)))


    def db_load_into_list_a_2(self, basic_info_df):
        wx = lg.get_handle()
        if (basic_info_df is None):
            wx.info("Err: basic info dataframe is Empty,")
            return -1
        basic_info_array = basic_info_df.values.tolist()
        i = 0
        while i < len(basic_info_array):
            basic_info_array[i] = tuple(basic_info_array[i])
            i += 1
        # wx.info(basic_info_array)
        sql = "REPLACE INTO stock.list_a SET id=%s, name=%s, total_shares=%s, flow_shares=%s, list_date=%s, " \
              "full_name=%s, industry=%s, industry_code=%s"
        self.db.cursor.executemany(sql, basic_info_array)
        self.db.handle.commit()

    """
    db_load_into_list_a() 已经废弃，目前使用新函数 db_load_into_list_a_2() 代替
    """

    def db_load_into_list_a(self, basic_info_df):
        wx = lg.get_handle()
        for basic_info in basic_info_df.get_values():
            sql = "select * from list_a where id ='" + basic_info[0] + "'"
            # sql =  'select count(*) from list_a where id = \'%s\''%basic_info[0]
            iCount = self.db.cursor.execute(sql)  # 返回值，受影响的行数， 不需要 fetchall 来读取了
            if iCount == 0:
                sql = "insert into list_a (id, name, total_shares, flow_shares, list_date, full_name, industry, industry_code) " \
                      "values (%s, %s, %s ,%s, %s, %s, %s, %s)"
                # self.logger.wt.info( "Insert id={0}, name={1}, t_shares={2}, f_shares={3}, date={4}, f_name={5},
                # industry={6}, industry_code={7}". format(basic_info[0], basic_info[1], basic_info[2], basic_info[
                # 3], basic_info[4], basic_info[5], basic_info[6], basic_info[7]))
                wx.info("Insert id={0}, name={1}".format(basic_info[0], basic_info[1]))
                self.db.cursor.execute(sql, (
                    basic_info[0], basic_info[1], float(basic_info[2]), float(basic_info[3]), basic_info[4],
                    basic_info[5], basic_info[6], basic_info[7]))
                self.db.handle.commit()
            elif iCount == 1:
                wx.info("Existed\t[{0}==>{1}]".format(basic_info[0], basic_info[1]))
            else:
                wx.info("iCount == %d , what happended ???" % iCount)

    def get_json_str(self, url, web_flag=None):
        wx = lg.get_handle()
        if web_flag == 'sz_basic':
            header = {
                'User-Agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
                'Connection': 'keep-alive'
            }
        elif web_flag == 'sh_basic':
            header = {
                'Cookie': 'yfx_c_g_u_id_10000042=_ck18112210334212135454572121490; yfx_mr_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10000042=; VISITED_COMPANY_CODE=%5B%22603017%22%2C%22600354%22%2C%22601975%22%2C%22600000%22%5D; VISITED_STOCK_CODE=%5B%22603017%22%2C%22600354%22%2C%22601975%22%2C%22600000%22%5D; seecookie=%5B601975%5D%3AST%u957F%u6CB9%2C%5B600000%5D%3A%u6D66%u53D1%u94F6%u884C; JSESSIONID=CA764F4C8465140437D5F6B868137460; yfx_f_l_v_t_10000042=f_t_1542854022203__r_t_1553650507322__v_t_1553651393256__r_c_23; VISITED_MENU=%5B%229055%22%2C%228536%22%2C%228451%22%2C%228453%22%2C%228454%22%2C%229057%22%2C%229062%22%2C%229056%22%2C%228466%22%2C%228523%22%2C%228528%22%5D',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
                'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/'
            }
        elif web_flag == 'eastmoney':
            header = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Cookie': 'st_pvi=71738581877645; st_sp=2018-11-22%2011%3A40%3A40; qgqp_b_id=8db9365e6c143170016c773cee144103; em_hq_fls=js; HAList=a-sz-000333-%u7F8E%u7684%u96C6%u56E2%2Ca-sz-300059-%u4E1C%u65B9%u8D22%u5BCC; st_si=74062085443937; st_asi=delete; st_sn=27; st_psi=20190113183705692-113300301007-4079839165',
                # 'Host': 'dcfm.eastmoney.com',
                'Upgrade-Insecure-Requests': 1,
                'User-Agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
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

    def sina_daily_data_json_parse(self, json_str=None, date='20100101'):
        # self.logger.wt.info("start to parse BASIC INFO ...\n")
        if json_str is not None:
            json_obj = json.loads(json_str)

            company_code = jsonpath(json_obj, '$..code')  # 公司/A股代码
            open = jsonpath(json_obj, '$..open')
            high = jsonpath(json_obj, '$..high')
            low = jsonpath(json_obj, '$..low')
            close = jsonpath(json_obj, '$..trade')
            pre_close = jsonpath(json_obj, '$..settlement')
            chg = jsonpath(json_obj, '$..pricechange')
            pct_chg = jsonpath(json_obj, '$..changepercent')
            v = jsonpath(json_obj, '$..volume')  # 成交量，单位“股”，需要换算成“手”
            vol = [float(tmp) / 100 for tmp in v]  # 换算成 “手” 成交量
            am = jsonpath(json_obj, '$..amount')  # 成交金额， 单位“元”， 需要换算成 “千”
            amount = [float(tmp) / 1000 for tmp in am]  # 换算成 “千” 成交金额
            cur_date = list(date for _ in range(0, len(company_code)))
            daily_data = [company_code, cur_date, open, high, low, close, pre_close, chg, pct_chg, vol, amount]
            df = pd.DataFrame(daily_data)
            df1 = df.T
            df1.rename(
                columns={0: 'ID', 1: 'Date', 2: 'Open', 3: 'High', 4: 'Low', 5: 'Close', 6: 'Pre_close', 7: 'Chg',
                         8: 'Pct_chg', 9: 'Vol', 10: 'Amount'}, inplace=True)
            # col_name = df1.columns.tolist()
            # col_name.insert(1, 'Date')
            # df1.reindex(columns=col_name)
            # df1['Date'] = date
            return df1
        else:
            # self.logger.wt.info("json string is Null , exit ...\n")
            return None

    def sina_industry_json_parse(self, json_str=None):
        if json_str is None:
            return -1
        json_obj = json.loads(json_str)
        stock_id_arr = jsonpath(json_obj, '$..code')
        return stock_id_arr

    def db_load_into_dgj_trade(self, dd_df=None, t_name='dgj_201901'):
        wx = lg.get_handle()
        if dd_df is None:
            wx.info("[db_load_into_dgj_trade] Err: Daily Data Frame Empty,")
            return -1
        dd_array = dd_df.values.tolist()
        i = 0
        while i < len(dd_array):
            dd_array[i] = tuple(dd_array[i])
            i += 1

        sql = "REPLACE INTO " + t_name + " SET date=%s, id=%s, dgj_name=%s, dgj_pos=%s, trader_name=%s, " \
                                         "relation=%s, vol=%s, price=%s, amount=%s, pct_chg=%s, " \
                                         "trading_type=%s, in_hand=%s"
        self.db.cursor.executemany(sql, dd_array)
        self.db.handle.commit()



    def db_load_into_daily_data(self, dd_df=None, pre_id = '',  mode='basic', type='cq'):
        wx = lg.get_handle()

        if (type == 'cq'):
            if pre_id == '00%':
                t_name = self.dd_cq_00
            elif pre_id == '30%':
                t_name = self.dd_cq_30
            elif pre_id == '60%':
                t_name = self.dd_cq_60
            elif pre_id == '002%':
                t_name = self.dd_cq_002
            elif pre_id == '68%':
                t_name = self.dd_cq_68
            else:
                wx.info("[db_load_into_daily_data]: TYPE = cq, pre_id ( {} )is NOT Match".format(pre_id))
                return  None
        elif(type == 'qfq'):
            if pre_id == '00%':
                t_name = self.dd_qfq_00
            elif pre_id == '30%':
                t_name = self.dd_qfq_30
            elif pre_id == '60%':
                t_name = self.dd_qfq_60
            elif pre_id == '002%':
                t_name = self.dd_qfq_002
            elif pre_id == '68%':
                t_name = self.dd_qfq_68
            else:
                wx.info("[db_load_into_daily_data]: TYPE = qfq, pre_id ( {} )is NOT Match".format(pre_id))
                return  None
        elif(type == 'bt_qfq'):
            if re.match('002',pre_id) is not None:
                t_name = self.bt_dd_qfq_002
            elif re.match('30',pre_id) is not None:
                t_name = self.bt_dd_qfq_30
            elif re.match('60',pre_id) is not None:
                t_name = self.bt_dd_qfq_60
            elif re.match('00',pre_id) is not None:
                t_name = self.bt_dd_qfq_00
            elif re.match('68',pre_id) is not None:
                t_name = self.bt_dd_qfq_68
            else:
                wx.info("[db_load_into_daily_data]: TYPE = bt_qfq, pre_id ( {} )is NOT Match".format(pre_id))
                return  None
        else:
            wx.info("[db_load_into_daily_data]: TYPE ( {} ) is NOT Match")
            return None

        if dd_df is None or dd_df.empty or t_name is None:
            wx.info("[db_load_into_daily_data] Err: Daily Data Frame or Table Name is Empty,")
            return -1
        dd_array = dd_df.values.tolist()
        i = 0
        while i < len(dd_array):
            dd_array[i] = tuple(dd_array[i])
            i += 1
        if mode == 'full':
            sql = "REPLACE INTO " + t_name + " SET id=%s, date=%s, open=%s, high=%s, low=%s, " \
                                             "close=%s, pre_close=%s, chg=%s,  pct_chg=%s,vol=%s, amount=%s, " \
                                             "qrr=%s, tor=%s, pct_up_down=%s, pe=%s, pb=%s"
        elif mode == 'basic':
            sql = "REPLACE INTO " + t_name + " SET id=%s, date=%s, open=%s, high=%s, low=%s, " \
                                             "close=%s, pre_close=%s, chg=%s,  pct_chg=%s,vol=%s, amount=%s"
        else:
            sql = "REPLACE INTO " + t_name + " SET id=%s, date=%s, open=%s, high=%s, low=%s, " \
                                             "close=%s, pre_close=%s, chg=%s,  pct_chg=%s,vol=%s, amount=%s"

        i_scale = 1000
        for i in range(0, len(dd_array), i_scale):
            tmp_array = dd_array[i: i + i_scale]
            wx.info("[db_load_into_daily_data][{}][{}] Loaded {} ~ {} , total {} " .format(type, t_name, i, i + i_scale, len(dd_array)))
            self.db.cursor.executemany(sql, tmp_array)
            self.db.handle.commit()

        # self.db.cursor.executemany(sql, dd_array)
        # self.db.handle.commit()
        # wx.info(dd_array)

    def east_ws_json_parse(self, json_str=None):
        if json_str is not None:
            json_obj = json.loads(json_str)
        self.page_count = json_obj['pages']
        if len(json_obj['data']) == 0:
            return None
        dt = jsonpath(json_obj, '$..TDATE')
        date = [re.sub(r'-', '', tmp[0:10]) for tmp in dt]
        id = jsonpath(json_obj, '$..SECUCODE')
        disc = jsonpath(json_obj, '$..Zyl')
        price = jsonpath(json_obj, '$..PRICE')
        vol = jsonpath(json_obj, '$..TVOL')
        v_t = jsonpath(json_obj, '$..Cjeltszb')
        vol_tf = [float(tmp) * 100 for tmp in v_t]  # 换算成百分比，交易量占流动股的百分比
        amount = jsonpath(json_obj, '$..TVAL')
        b_code = jsonpath(json_obj, '$..BUYERCODE')
        s_code = jsonpath(json_obj, '$..SALESCODE')
        close_price = jsonpath(json_obj, '$..CPRICE')
        pct_chg = jsonpath(json_obj, '$..RCHANGE')
        b_name = jsonpath(json_obj, '$..BUYERNAME')
        s_name = jsonpath(json_obj, '$..SALESNAME')
        ws_data = [date, id, disc, price, vol, vol_tf, amount, b_code, s_code, close_price, pct_chg, b_name, s_name]
        df = pd.DataFrame(ws_data)
        df1 = df.T
        df1.rename(columns={0: 'Date', 1: 'ID', 2: 'Disc', 3: 'Price', 4: 'Vol', 5: 'Vol_tf', 6: 'Amount', 7: 'B_code',
                            8: 'S_code', 9: 'Close_price', 10: 'Pct_chg', 11: 'B_name', 12: 'S_name'}, inplace=True)
        return df1

    def db_load_into_ws(self, ws_df=None):
        wx = lg.get_handle()
        if ws_df is None:
            wx.info("[db_load_into_ws]Err: ws flow dataframe is Empty,")
            return -1
        ws_array = ws_df.values.tolist()
        i = 0
        while i < len(ws_array):
            ws_array[i] = tuple(ws_array[i])
            i += 1
        sql = "REPLACE INTO stock.ws_201901 SET date=%s, id=%s, disc=%s, price=%s, vol=%s, vol_tf=%s, " \
              "amount=%s, b_code=%s, s_code=%s, close_price=%s, pct_chg=%s, b_name=%s, s_name=%s"
        self.db.cursor.executemany(sql, ws_array)
        self.db.handle.commit()

    def db_load_into_ws_share_holder(self, df_share_holder=None):
        wx = lg.get_handle()
        if df_share_holder is None:
            wx.info("[db_load_into_ws_share_holder]Err: ws share holder dataframe is Empty,")
            return -1
        ws_sh_array = df_share_holder.values.tolist()
        i = 0
        while i < len(ws_sh_array):
            ws_sh_array[i] = tuple(ws_sh_array[i])
            i += 1

        sql = "REPLACE INTO STOCK.ws_share_holder SET id=%s, h_code=%s, b_vol=%s, b_price=%s, b_vol_tf=%s, s_vol=%s, " \
              "s_price=%s, s_vol_tf=%s"
        self.db.cursor.executemany(sql, ws_sh_array)
        self.db.handle.commit()

    def db_load_into_hot_industry(self, df_hot_industry=None):
        wx = lg.get_handle()
        if df_hot_industry is None or df_hot_industry.empty:
            wx.info("[db_load_into_hot_industry]Err: df_hot_industy is Empty,")
            return -1
        hot_industy_array = df_hot_industry.values.tolist()
        i = 0
        while i < len(hot_industy_array):
            hot_industy_array[i] = tuple(hot_industy_array[i])
            i += 1

        sql = "REPLACE INTO "+self.dd_hot_industry+" SET id=%s, date=%s, name=%s, industry_code=%s, industry_name=%s, pct_chg=%s"

        i_scale = 1000
        for i in range(0, len(hot_industy_array), i_scale):
            tmp_arry = hot_industy_array[i : i+i_scale]
            wx.info("[db_load_into_hot_industry][{}] Loaded {} ~ {} , total {} " .
                    format(self.dd_hot_industry, i, i + i_scale, len(hot_industy_array)))
            self.db.cursor.executemany(sql, hot_industy_array)
            self.db.handle.commit()



    def whole_sales_stock_id(self):
        sql = "select distinct id from ws_201901"

        iCount = self.db.cursor.execute(sql)  # 返回值
        if iCount > 0:
            arr_id = self.db.cursor.fetchall()
            return arr_id
        else:
            return None

    def whole_sales_start_date(self):
        # sql = "select date from stock.ws_201901  where str_to_date(date,'%Y%m%d') " \
        #       "between str_to_date("+ start +",'%Y%m%d') and str_to_date(" + end+",'%Y%m%d'); "
        sql = "select date from ws_201901 as w order by w.date desc limit 1"

        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        if iCount == 1:
            result = self.db.cursor.fetchone()
            record_date = datetime.strptime(result[0], "%Y%m%d")  # 日期字符串 '20190111' ,转换成 20190111 日期类型
            start_date = (record_date + timedelta(days=1)).strftime('%Y-%m-%d')  # 起始日期 为记录日期+1天
            return start_date
        else:
            return None

    def whole_sales_remove_expired_data(self):
        expire_date = (date.today() + timedelta(days=-550)).strftime('%Y%m%d')

        # select * from ws_201901 where str_to_date(date, '%Y%m%d') < str_to_date('20170906', '%Y%m%d');
        # sql = "select date from stock.ws_201901  where str_to_date(date,'%Y%m%d') " \
        #       "between str_to_date("+ start +",'%Y%m%d') and str_to_date(" + end+",'%Y%m%d'); "
        sql = "delete from ws_201901 where str_to_date(date,'%Y%m%d') < str_to_date(" + expire_date + ", '%Y%m%d' )"
        # wx.info("[whole_sales_remove_expired_data]: {}".format(sql))
        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        return iCount

    def whole_sales_analysis(self, s_id=None):
        # wx = lg.get_handle()
        if s_id is None:
            return -1

        # 查询该股票所有的大宗交易流水，输出成 Dataframe 格式
        sql = "select b_code, b_name, s_code, s_name, vol, vol_tf, price, amount from ws_201901 " \
              "where id = %s order by date asc"
        self.db.cursor.execute(sql, (s_id))
        self.db.handle.commit()
        ws_flow = self.db.cursor.fetchall()
        columnDes = self.db.cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        df_ws_flow = pd.DataFrame([list(i) for i in ws_flow], columns=columnNames)

        # 买方信息归集整理成 Dataframe
        buyer_vol = df_ws_flow['vol'].groupby(df_ws_flow['b_code']).sum()
        buyer_vol_tf = df_ws_flow['vol_tf'].groupby(df_ws_flow['b_code']).sum()
        buyer_amount = df_ws_flow['amount'].groupby(df_ws_flow['b_code']).sum()
        buyer_price = buyer_amount / buyer_vol

        df_buyer = pd.concat([buyer_vol, buyer_price, buyer_vol_tf], axis=1)
        # df_buyer['id'] = s_id
        df_buyer['s_vol'] = 0
        df_buyer['s_price'] = 0
        df_buyer['s_vol_tf'] = 0
        df_buyer = df_buyer.reset_index()
        df_buyer.columns = ['h_code', 'b_vol', 'b_price', 'b_vol_tf', 's_vol', 's_price', 's_vol_tf']
        df_buyer = df_buyer.reindex(
            columns=['h_code', 'b_vol', 'b_price', 'b_vol_tf', 's_vol', 's_price', 's_vol_tf'])

        # 卖方信息归集整理成 Dataframe
        seller_vol = df_ws_flow['vol'].groupby(df_ws_flow['s_code']).sum()
        seller_vol_tf = df_ws_flow['vol_tf'].groupby(df_ws_flow['s_code']).sum()
        seller_amount = df_ws_flow['amount'].groupby(df_ws_flow['s_code']).sum()
        seller_price = seller_amount / seller_vol
        df_seller = pd.concat([seller_vol, seller_price, seller_vol_tf], axis=1)
        # df_seller['id'] = s_id
        df_seller['b_vol'] = 0
        df_seller['b_price'] = 0
        df_seller['b_vol_tf'] = 0
        df_seller = df_seller.reset_index()
        df_seller.columns = ['h_code', 's_vol', 's_price', 's_vol_tf', 'b_vol', 'b_price', 'b_vol_tf']
        df_seller = df_seller.reindex(
            columns=['h_code', 'b_vol', 'b_price', 'b_vol_tf', 's_vol', 's_price', 's_vol_tf'])

        # 买方、卖方 Dataframe 合并成一个 Dataframe
        df_share_holder = pd.concat([df_buyer, df_seller], axis=0, join='outer')
        df_share_holder = df_share_holder.groupby('h_code').sum()  # 合并 h_code相同 的 买家 、卖家 数据
        df_share_holder = df_share_holder.reset_index()  # 重新整理成一个 Dataframe
        df_share_holder['id'] = s_id
        df_share_holder = df_share_holder.reindex(
            columns=['id', 'h_code', 'b_vol', 'b_price', 'b_vol_tf', 's_vol', 's_price', 's_vol_tf'])

        return df_share_holder

    def whole_sales_supplement_info(self):
        wx = lg.get_handle()
        try:
            self.db.cursor.callproc("ws_supplement")
            self.db.handle.commit()
        except Exception as e:
            wx.info("[whole_sales_supplement_info] Err : {}".format(e))
            return False
        return True

    def whole_sales_data_remove(self):
        sql = "delete from ws_201901 "
        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        return iCount

    def dgj_trading_data_remove(self, start_date=None):
        if start_date is not None:
            sql = "delete from dgj_201901 where date <= " + start_date
        else:
            sql = "delete from dgj_201901"
        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        return iCount

    def dgj_remove_expired_data(self):
        expire_date = (date.today() + timedelta(days=-550)).strftime('%Y%m%d')
        sql = "delete from dgj_201901 where str_to_date(date,'%Y%m%d') < str_to_date(" + expire_date + ", '%Y%m%d' )"
        # wx.info("[whole_sales_remove_expired_data]: {}".format(sql))
        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        return iCount

    def dgj_repo_start_date(self, table_name=None):
        sql = "select distinct date from " + table_name + " as dgj order by dgj.date desc limit 3"

        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        if iCount > 1:
            result = self.db.cursor.fetchall()
            # first_date = result[0][0]
            # second_date = result[1][0]
            third_date = result[2][0]
            # record_date = datetime.strptime(result[0], "%Y%m%d")  # 日期字符串 '20190111' ,转换成 20190111 日期类型
            # start_date = record_date.strftime('%Y%m%d')  # 起始日期 为记录日期+1天
            sql = "delete from " + table_name + " where date > " + third_date
            iCount = self.db.cursor.execute(sql)  # 返回值
            self.db.handle.commit()

            start_date = datetime.strptime(third_date, "%Y%m%d")  # 日期字符串 '20190111' ,转换成 20190111 日期类型
            start_date = start_date.strftime('%Y-%m-%d')  # 起始日期 为记录日期+1天

            return start_date
        else:
            return None

    def repo_remove_data(self, start_date=None):
        if start_date is not None:
            sql = "delete from repo_201901 where notice_date < " + start_date
        else:
            sql = "delete from repo_201901"
        iCount = self.db.cursor.execute(sql)  # 返回值
        self.db.handle.commit()
        return iCount

    def east_repo_json_parse(self, json_str=None):
        if json_str is not None:
            json_obj = json.loads(json_str)

        self.total_pages = json_obj['pages']

        if len(json_obj['data']) == 0:
            return None
        id = jsonpath(json_obj, '$..dim_scode')
        name = jsonpath(json_obj, '$..securityshortname')
        notice_date_stamp = jsonpath(json_obj, '$..updatedate')
        notice_date = self._datestamp_to_srt(notice_date_stamp)
        start_date_stamp = jsonpath(json_obj, '$..repurstartdate')
        start_date = self._datestamp_to_srt(start_date_stamp)
        end_date_stamp = jsonpath(json_obj, '$..repurenddate')
        end_date = self._datestamp_to_srt(end_date_stamp)
        # progress = [float(tmp) * 100 for tmp in v_t]
        progress = jsonpath(json_obj, '$..repurprogress')
        plan_low_price = jsonpath(json_obj, '$..repurpricelower')
        plan_high_price = jsonpath(json_obj, '$..repurpricecap')
        plan_low_vol = jsonpath(json_obj, '$..repurnumlower')
        plan_high_vol = jsonpath(json_obj, '$..repurnumcap')
        plan_low_amount = jsonpath(json_obj, '$..repuramountlower')
        plan_high_amount = jsonpath(json_obj, '$..repuramountlimit')
        buy_in_low_price = jsonpath(json_obj, '$..repurpricelower1')
        buy_in_high_price = jsonpath(json_obj, '$..repurpricecap1')
        buy_in_vol = jsonpath(json_obj, '$..repurnum')
        buy_in_amount = jsonpath(json_obj, '$..repuramount')
        repo_data = [id, name, notice_date, start_date, end_date, progress, plan_low_price, plan_high_price,
                     plan_low_vol, plan_high_vol, plan_low_amount, plan_high_amount,
                     buy_in_low_price, buy_in_high_price, buy_in_vol, buy_in_amount]
        df = pd.DataFrame(repo_data)
        df1 = df.T
        df1.rename(columns={0: 'id', 1: 'name', 2: 'notice_date', 3: 'start_date', 4: 'end_date', 5: 'progress',
                            6: 'plan_low_price', 7: 'plan_high_price', 8: 'plan_low_vol', 9: 'plan_high_vol',
                            10: 'plan_low_amount', 11: 'plan_high_amount', 12: 'buy_in_low_price',
                            13: 'buy_in_high_price', 14: 'buy_in_vol', 15: 'buy_in_amount'}, inplace=True)
        return df1

    def _datestamp_to_srt(self, date_arr):
        ret_date_arr = list()
        for datestamp in date_arr:
            if datestamp is None:
                ret_date_arr.append('19900101')
            else:
                x = time.strftime('%Y%m%d', time.localtime(datestamp / 1000))
                ret_date_arr.append(x)
        return ret_date_arr

    def db_load_into_repo(self, df_repo=None, t_name='repo_201901'):
        wx = lg.get_handle()
        if df_repo is None:
            wx.info("[db_load_into_repo] Err: Repo DataFrame is Empty,")
            return -1
        repo_arr = df_repo.values.tolist()
        i = 0
        while i < len(repo_arr):
            repo_arr[i] = tuple(repo_arr[i])
            i += 1
        sql = "REPLACE INTO " + t_name + " SET id=%s, name=%s, notice_date=%s, start_date=%s, " \
                                         "end_date=%s, progress=%s, plan_low_price=%s, plan_high_price=%s, " \
                                         "plan_low_vol=%s, plan_high_vol=%s, plan_low_amount=%s, plan_high_amount=%s, " \
                                         "buy_in_price_low=%s, buy_in_price_high=%s, buy_in_vol=%s, buy_in_amount=%s"
        self.db.cursor.executemany(sql, repo_arr)
        self.db.handle.commit()

        """
        sql = "select distinct b_code from ws_201901 where id = %s order by date asc"
        self.db.cursor.execute(sql, (s_id))
        self.db.handle.commit()
        arr_buyer = self.db.cursor.fetchall()
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        df_buyer = pd.DataFrame([list(i) for i in arr_buyer], columns=columnNames)

        # wx.info("[whole_sales_analysis] Stock {} record {}",format(s_id, arr_buyer))

        sql = "select distinct s_code from ws_201901 where id = %s order by date asc"
        self.db.cursor.execute(sql, (s_id))
        self.db.handle.commit()
        arr_seller = self.db.cursor.fetchall()
        # wx.info("[whole_sales_analysis] Stock {} record {}", format(s_id, arr_seller))
"""

# wx.info("[whole_sales_analysis] Stock {} record {}",format(s_id, ws_flow))
