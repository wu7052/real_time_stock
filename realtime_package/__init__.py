import sys
import os
#
workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath + '\\realtime_package'
sys.path.insert(0, pack_path)
# print("@__init__ sys.path",sys.path)

from realtime_sina import rt_sina
from realtime_east import rt_east
