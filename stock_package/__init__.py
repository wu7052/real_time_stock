import sys
import os

workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath + '\\stock_package'
sys.path.insert(0, pack_path)

# pack_path = workpath + '\\logger_package'
# sys.path.insert(0, pack_path)
# print("@__init__ sys.path",sys.path)

from tushare_data import ts_data
from sh_ex_data import sh_web_data
from sz_ex_data import sz_web_data
from ex_data import ex_web_data
from ma import ma_kits
from psy import psy_kits
# from logger_package import myLogger