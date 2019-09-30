import sys
import os
#
workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath +  '\\conf'
sys.path.insert(0, pack_path)
# print("@__init__ sys.path",sys.path)

from config_handler import conf_handler