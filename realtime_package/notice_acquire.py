import json
from datetime import date, timedelta
from jsonpath import jsonpath
import new_logger as lg
import pandas as pd
import urllib3
import requests
import chardet
import re
from db_package import db_ops
import os
import sys
import numpy as np

class notice_watcher():
    def __init__(self, id_arr=None, keywords_arr=None):
        if id_arr is not None:
            self.id_arr=id_arr
        if keywords_arr is not None:
            self.keywords_arr = keywords_arr

        # 建立数据库对象
        self.db = db_ops()

    def get_sz_noitce(self, date_arr = []):
        wx = lg.get_handle()
        if len(date_arr) == 0 :
            start_date = (date.today()).strftime('%Y-%m-%d')
            end_date = start_date
        else:
            start_date = date_arr[0]
            end_date = date_arr[1]
        postUrl = 'http://www.szse.cn/api/disc/announcement/annList?random=0.3947163813671919'

        # 请求头设置
        payloadHeader = {
            'Host': 'www.szse.cn',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
            'Content-Type': 'application/json',
            'Referer': 'http://www.szse.cn/disclosure/listed/notice/index.html'
        }
        # 下载超时
        timeOut = 25

        page_num = 1
        notice_counter = 1
        notice_df = pd.DataFrame()
        while True:
            payloadData = {
                'seDate': [start_date, end_date],
                'channelCode': ["listedNotice_disc"],
                'pageSize': 30,
                'pageNum': page_num
            }
            # r = requests.post(postUrl, data=json.dumps(payloadData), headers=payloadHeader)
            dumpJsonData = json.dumps(payloadData)
            res = requests.post(postUrl, data=dumpJsonData, headers=payloadHeader, timeout=timeOut, allow_redirects=True)
            notice_data = self._sz_json_parse(json_str=res.text)
            if notice_data[0] == 0:
                wx.info("[深证公告获取][{}-{}]公告数量为[0]，第{}页，退出 ".format(start_date, end_date, page_num))
                break
            elif notice_df is None or len(notice_df) == 0 :
                notice_df = notice_data[1]
            else:
                notice_df = notice_df.append(notice_data[1])
            wx.info("[深证公告获取][{}-{}]公告总数[{}],已获得第[{}]页，获取下一页".
                    format(start_date, end_date, notice_data[0], page_num))
            page_num += 1
        return notice_df

    def _sz_json_parse(self, json_str=None):
        wx = lg.get_handle()
        if json_str is not None:
            json_obj = json.loads(json_str)
        else:
            return [0,None]
        if json_obj is None:
            # wx.info("[深证公告获取] JSON 为空，退出")
            return [0, None]
        if len(json_obj['data']) == 0:
            # wx.info("[深证公告获取] JSON 为空，退出")
            return [0, None]
        notice_qty = json_obj['announceCount']
        # name = jsonpath(json_obj, '$..data..secName[0]')
        title = jsonpath(json_obj, '$..data..title')
        ann_time = jsonpath(json_obj, '$..data..publishTime')
        id_array = jsonpath(json_obj, '$..data..secCode')
        id = np.array(id_array).flatten().tolist()

        # notice_data = [ann_id, ann_time, id, name, title]
        notice_data = [ann_time, id, title]
        df = pd.DataFrame(notice_data)
        df1 = df.T
        df1.rename(columns={0: 'ann_time', 1: 'id', 2: 'title'}, inplace=True)
        df1['ann_time'] = df1['ann_time'].apply(lambda x:x[:10])
        return [notice_qty,df1]


    def get_sh_notice(self, date_arr = []):
        wx = lg.get_handle()
        if len(date_arr) == 0 :
            start_date = (date.today()).strftime('%Y-%m-%d')
            end_date = start_date
        else:
            start_date = date_arr[0]
            end_date = date_arr[1]
        page_num  = 1

        header = {
            'Cookie': 'yfx_c_g_u_id_10000042=_ck18112210334212135454572121490; yfx_mr_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10000042=; VISITED_COMPANY_CODE=%5B%22603017%22%2C%22600354%22%2C%22601975%22%2C%22600000%22%5D; VISITED_STOCK_CODE=%5B%22603017%22%2C%22600354%22%2C%22601975%22%2C%22600000%22%5D; seecookie=%5B601975%5D%3AST%u957F%u6CB9%2C%5B600000%5D%3A%u6D66%u53D1%u94F6%u884C; JSESSIONID=CA764F4C8465140437D5F6B868137460; yfx_f_l_v_t_10000042=f_t_1542854022203__r_t_1553650507322__v_t_1553651393256__r_c_23; VISITED_MENU=%5B%229055%22%2C%228536%22%2C%228451%22%2C%228453%22%2C%228454%22%2C%229057%22%2C%229062%22%2C%229056%22%2C%228466%22%2C%228523%22%2C%228528%22%5D',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
            'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/'
        }
        http = urllib3.PoolManager()
        notice_df = pd.DataFrame()
        while True:
            url = "http://query.sse.com.cn/security/stock/queryCompanyBulletin.do?" \
                  "jsonCallBack=jsonpCallback43958&isPagination=true&productId=&keyWord=&" \
                  "securityType=0101%2C120100%2C020100%2C020200%2C120200&reportType2=&" \
                  "reportType=ALL&beginDate=" + start_date + "&endDate=" + end_date + "&" \
                  "pageHelp.pageSize=100&pageHelp.pageCount=50&pageHelp.pageNo=" + str(page_num) + \
                  "&pageHelp.beginPage=" + str(page_num) + "&pageHelp.cacheSize=1&" \
                  "pageHelp.endPage=" + str(page_num) + "1&_=1581167068639"

            try:
                raw_data = http.request('GET', url, headers=header)
            except Exception as e:
                return None
            finally:
                if raw_data.status >= 300:
                    return None

            # 获得html源码,utf-8解码
            str_type = chardet.detect(raw_data.data)
            unicode = lg.str_decode(raw_data.data, str_type['encoding'])
            json_str = re.sub(r'jsonpCallback\w+\(', r'', unicode)[:-1]
            notice_data = self._sh_json_parse(json_str=json_str)
            if notice_data[0] == 0:
                wx.info("[上证公告获取][{}-{}]公告数量为[0]，第{}页，退出 ".format(start_date, end_date, page_num))
                break
            elif notice_df is None or len(notice_df) == 0 :
                notice_df = notice_data[1]
            else:
                notice_df = notice_df.append(notice_data[1])
            wx.info("[上证公告获取][{}-{}]公告总数[{}],已获得第[{}]页，获取下一页".
                    format(start_date, end_date, notice_data[0], page_num))
            page_num += 1
            # return unicode
        return notice_df

    def _sh_json_parse(self, json_str=None):
        wx = lg.get_handle()
        if json_str is not None:
            json_obj = json.loads(json_str)
        else:
            return [0,None]
        if json_obj is None:
            # wx.info("[上证公告获取] JSON 为空，退出")
            return [0, None]
        if len(json_obj['pageHelp']['data']) == 0:
            # wx.info("[上证公告获取] JSON 为空，退出")
            return [0, None]

        notice_qty = json_obj['pageHelp']['total']
        id = jsonpath(json_obj, '$..result..SECURITY_CODE')
        title = jsonpath(json_obj, '$..result..TITLE')
        ann_time = jsonpath(json_obj, '$..result..SSEDATE')

        notice_data = [ann_time, id, title]
        df = pd.DataFrame(notice_data)
        df1 = df.T
        df1.rename(columns={0: 'ann_time', 1: 'id', 2: 'title'}, inplace=True)
        return [notice_qty,df1]


    def noitce_finder(self, n_df=None):
        if n_df is None or len(n_df) == 0:
            return None

        n_id_found_df = pd.DataFrame()

        if self.id_arr is not None:
            for id in self.id_arr:
                tmp = n_df.loc[(n_df['id'] == id)].copy()
                if tmp is None or len(tmp) == 0:
                    continue
                if len(n_id_found_df)==0:
                    n_id_found_df = tmp
                else:
                    n_id_found_df = n_id_found_df.append(tmp,sort=False)
        else:
            n_id_found_df = None

        n_title_found = pd.DataFrame()
        for keywords_arr in self.keywords_arr:
            if keywords_arr is not None and len(keywords_arr) > 0 :
                # str = '|'.join(keywords_arr)
                for keyword in keywords_arr:
                    n_tmp = n_df[n_df.title.str.contains(keyword)].copy()
                    if n_tmp is not None and len(n_tmp) >0:
                        n_tmp['keyword'] = keyword
                    if n_title_found is None or len(n_title_found) == 0:
                        n_title_found = n_tmp
                    else:
                        n_title_found = n_title_found.append(n_tmp,sort=False)
            else:
                continue
        if n_title_found is not None and len(n_title_found) > 0:
            work_path = os.path.dirname(os.path.abspath(sys.argv[0]))
            output_path = work_path + '\\log\\'
            today = date.today().strftime('%Y%m%d')
            filename = output_path + today + "_公告关键词过滤.xlsx"

            n_title_found.to_excel(filename, index=False, sheet_name='关键词过滤', encoding='utf_8_sig')
        return n_id_found_df


