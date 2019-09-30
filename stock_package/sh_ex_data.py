# from logger_package import myLogger
from ex_data import ex_web_data

import pandas as pd
import json
from jsonpath import jsonpath


class sh_web_data(ex_web_data):

    def __init__(self):
        ex_web_data.__init__(self)
        # self.logger = ex_data.logger
    def __del__(self):
        ex_web_data.__del__(self)

    def industry_df_build(self):
        industry_dict = {'A': u'农、林、牧、渔业', 'B': u'采矿业', 'C': u'制造业',
                         'D': u'电力、热力、燃气及水生产和供应业', 'E': u'建筑业',
                         'F': u'批发和零售业', 'G': u'交通运输、仓储和邮政业',
                         'H': u'住宿和餐饮业', 'I': u'信息传输、软件和信息技术服务业',
                         'J': u'金融业', 'K': u'房地产业', 'L': u'租赁和商务服务业',
                         'M': u'科学研究和技术服务业', 'N': u'水利、环境和公共设施管理业',
                         'O': u'居民服务、修理和其他服务业', 'P': u'教育',
                         'Q': u'卫生和社会工作', 'R': u'文化、体育和娱乐业', 'S': '综合'}

        self.industry_df = pd.DataFrame()

        for key in industry_dict.keys():
            # sh_data.logger.wt.info("key:{}==> industry:{}".format(key, industry_dict[key]))
            sh_industry_list_url = 'http://query.sse.com.cn/security/stock/getStockListData.do?&' \
                                   'jsonCallBack=jsonpCallback61935&isPagination=true&stockCode=&' \
                                   'csrcCode='+ key +'&areaName=&stockType=1&pageHelp.cacheSize=1&' \
                                                     'pageHelp.beginPage=1&pageHelp.pageSize=50&' \
                                                     'pageHelp.pageNo=1&_=1555658239650'

            # sh_industry_list_url = 'http://query.sse.com.cn/security/stock/queryIndustryIndex.do?&' \
            #                        'jsonCallBack=jsonpCallback61167&isPagination=false&' \
            #                        'csrcName=' + self.url_encode(industry_dict[key]) + \
            #                        '&csrcCode=' + key + '&_=1545309860667'

            json_str = self.get_json_str(url=sh_industry_list_url, web_flag='sh_basic' )
            cur_df = self.industry_info_json_parse(json_str[19:-1])
            cur_df['industry'] = industry_dict[key]
            cur_df['industry_code'] = key  # 沪深两市使用统一的 行业类别 代码

            if self.industry_df.size > 0:
                self.industry_df = pd.concat([self.industry_df, cur_df])  # 连接多个页面的 Dataframe
            else:
                self.industry_df = cur_df
        return 1

    def industry_info_json_parse(self, json_str=None):
        # self.logger.wt.info("start to parse INDUSTRY INFO ...\n")
        if json_str is not None:
            json_obj = json.loads(json_str)
            company_code = jsonpath(json_obj, '$..result..COMPANY_CODE')  # 公司/A股代码
            company_fname = jsonpath(json_obj, '$..result..COMPANY_ABBR')  # 公司全名
            industry_matix = [company_code, company_fname]
            df = pd.DataFrame(industry_matix)
            df1 = df.T
            df1.rename(columns={0: 'ID', 1: 'F_Name'}, inplace=True)
            # df1.sort_values(by=['Total Shares'], inplace=True)
            # print(df1.describe())
            # print(df1)
            return df1
        else:
            return None

    def basic_info_json_parse(self, json_str=None):
        # self.logger.wt.info("start to parse BASIC INFO ...\n")
        if json_str is not None:
            json_obj = json.loads(json_str)
            company_code = jsonpath(json_obj, '$..pageHelp..COMPANY_CODE')  # 公司/A股代码
            company_abbr = jsonpath(json_obj, '$..pageHelp..COMPANY_ABBR')  # 公司/A股简称
            # 网站已更新，这两项数据为空
            # 为保持数据一致，在后面用 reindex设置为0
            # total_shares = jsonpath(json_obj, "$..pageHelp..totalShares")  # A股总资本
            # total_flow_shares = jsonpath(json_obj, '$..pageHelp..totalFlowShares')  # A股流动资本
            list_date = jsonpath(json_obj, '$..pageHelp..LISTING_DATE')  # A股上市日期
            full_name = jsonpath(json_obj, '$..pageHelp..SECURITY_ABBR_A') # 全名
            total_page = jsonpath(json_obj, '$..pageHelp.pageCount')
            self.total_page = total_page

            stock_matix = [company_code, company_abbr, list_date, full_name]
            df = pd.DataFrame(stock_matix)
            df1 = df.T
            df1.rename(columns={0: 'ID', 1: 'Name', 2: 'List_Date', 3: 'Full_Name'}, inplace=True)
            df1 = df1.reindex(columns=['ID','Name','total_shares','total_flow_shares','List_Date','Full_Name'],fill_value=0 )

            return df1
        else:
            return None
