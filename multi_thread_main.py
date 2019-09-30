# -*- coding: utf-8 -*-
# __author__ = "WUX"
# dev version

import threading
import time
import os

import new_logger as lg
lg._init_()
wx = lg.get_handle()

from functions import *


"""
thrd_sh = threading.Thread(target=update_sh_basic_info_2,args=())
thrd_sz = threading.Thread(target=update_sz_basic_info,args=())

thrd_sh.start()
thrd_sz.start()
thrd_sh.join()
thrd_sz.join()

thrd_sw_industry_code = threading.Thread(target=update_sw_industry_into_basic_info,args=())
thrd_daily_date_from_eastmoney = threading.Thread(target = update_daily_data_from_eastmoney, args=(False,))
thrd_dgj_trading_data = threading.Thread(target = update_dgj_trading_data_from_eastmoney, args=(False,))
thrd_repo_data = threading.Thread(target = update_repo_data_from_eastmoney, args=(False,))

thrd_sw_industry_code.start()
thrd_daily_date_from_eastmoney.start()
thrd_dgj_trading_data.start()
thrd_repo_data.start()
"""