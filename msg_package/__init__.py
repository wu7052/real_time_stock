import sys
import os
#
workpath = os.path.dirname(os.path.abspath(sys.argv[0]))
pack_path = workpath + '\\msg_package'
sys.path.insert(0, pack_path)
# print("@__init__ sys.path",sys.path)

from file_msg import f_msg
# from realtime_east import rt_east
# from realtime_163 import rt_163
# from rt_timer import wx_timer
# from rt_analyzer import rt_ana