# from logger_package import myLogger
from ex_data import ex_web_data

import pandas as pd
import json
from jsonpath import jsonpath


class sz_web_data(ex_web_data):

    def __init__(self):
        ex_web_data.__init__(self)

    def __del__(self):
        ex_web_data.__del__(self)

    def basic_info_json_parse(self, json_str=None):
        # self.logger.wt.info("start to parse BASIC INFO ...\n")
        if json_str is not None:
            json_obj = json.loads(json_str)
            company_code = jsonpath(json_obj, '$..data..zqdm')  # 公司/A股代码
            company_abbr = jsonpath(json_obj, '$..data..agjc')  # 公司/A股简称
            ts = jsonpath(json_obj, "$..data..agzgb")  # A股总资本 , 单位 亿股
            total_shares = [float(amount)*10000 for amount in ts]  # 换算成 万股 单位
            tfs = jsonpath(json_obj, '$..data..agltgb')  # A股流动资本, 单位 亿股
            total_flow_shares = [float(amount)*10000 for amount in tfs] # 换算成 万股 单位
            list_date = jsonpath(json_obj, '$..data..agssrq')  # A股上市日期
            industry = jsonpath(json_obj, '$..data..sshymc') # 所属行业及代码
            industry_code = [code[0] for code in industry]
            total_page = jsonpath(json_obj, '$..metadata.pagecount')
            self.total_page = total_page

            stock_matix = [company_code, company_abbr ,total_shares, total_flow_shares, list_date, company_abbr, industry, industry_code]
            df = pd.DataFrame(stock_matix)
            df1 = df.T
            df1.rename(columns={0: 'ID', 1: 'Name',2: 'Total Shares',3: 'Flow Shares', 4:'List_Date',5:'Full_Name', 6:'Industry', 7:'Industry_code'}, inplace=True)
            return df1
        else:
            self.logger.wt.info("json string is Null , exit ...\n")
            return None
