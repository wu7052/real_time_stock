from db_package import db_ops
from conf import conf_handler
import new_logger as lg
import re
from datetime import datetime, time, date, timedelta
import pandas as pd
import time
from ex_data import ex_web_data
import json
from jsonpath import jsonpath

class rt_sina:
    def __init__(self, id_arr=None):
        self.wx = lg.get_handle()
        if id_arr is None:
            self.wx.info("[rt_sina] id_arr is None , __init__ EXIT !")
        else:
            self.id_arr = id_arr