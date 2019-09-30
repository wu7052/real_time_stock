import sys
import os
#
workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath + '\\realtime_package'
sys.path.insert(0, pack_path)
# print("@__init__ sys.path",sys.path)

from realtime_eyes import rt_eyes