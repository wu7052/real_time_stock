import new_logger as lg
import os
import sys


class f_msg():
    def __init__(self):
        pass

    def output(self, msg=None):
        wx = lg.get_handle()
        if msg is not None:
            wx.info("[f_msg] 输出：{}".format(msg))