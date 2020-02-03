from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import re
from datetime import datetime, time, date, timedelta
import pandas as pd
import time
import json
from jsonpath import jsonpath
import urllib3
import chardet
from urllib import parse

class rt_sina:
    def __init__(self, id_arr=None, date_str = '', items_page=200):
        self.wx = lg.get_handle()
        if id_arr is None:
            self.wx.info("[rt_sina] id_arr is None , __init__ EXIT !")
            return None
        else:
            self.id_arr = id_arr
            self.items_page = items_page

        if date_str is None or len(date_str) ==0:
            self.date_str = (date.today()).strftime('%Y-%m-%d')
        else:
            self.date_str = date_str



    def get_json_str(self):
        wx = lg.get_handle()

        url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList?" \
              "symbol="+id+"&num="+self.items_page+"&page=1&sort=ticktime&asc=0&volume=0&amount=0&type=0&" \
                                                   "day="+self.date_str
        header = {
            'Cookie': 'yfx_c_g_u_id_10000042=_ck18112210334212135454572121490; yfx_mr_10000042=%3A%3Amarket_type_free_search%3A%3A%3A%3Abaidu%3A%3A%3A%3A%3A%3A%3A%3Awww.baidu.com%3A%3A%3A%3Apmf_from_free_search; yfx_key_10000042=; VISITED_COMPANY_CODE=%5B%22603017%22%2C%22600354%22%2C%22601975%22%2C%22600000%22%5D; VISITED_STOCK_CODE=%5B%22603017%22%2C%22600354%22%2C%22601975%22%2C%22600000%22%5D; seecookie=%5B601975%5D%3AST%u957F%u6CB9%2C%5B600000%5D%3A%u6D66%u53D1%u94F6%u884C; JSESSIONID=CA764F4C8465140437D5F6B868137460; yfx_f_l_v_t_10000042=f_t_1542854022203__r_t_1553650507322__v_t_1553651393256__r_c_23; VISITED_MENU=%5B%229055%22%2C%228536%22%2C%228451%22%2C%228453%22%2C%228454%22%2C%229057%22%2C%229062%22%2C%229056%22%2C%228466%22%2C%228523%22%2C%228528%22%5D',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
            'Referer': 'http://www.sse.com.cn/assortment/stock/list/share/'
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