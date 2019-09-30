import os
import logging.config
from datetime import datetime
import re

def _init_():
    log_dir = os.path.abspath('.')
    logfile_dir = os.path.dirname(log_dir) + '\\log\\'  # log文件的目录

    # 定义三种日志输出格式 开始
    standard_format = '[%(asctime) -s][%(threadName)s:%(thread)d][task_id:%(name)s][%(filename)s:%(lineno)d]' \
                           '[%(levelname)s][%(message)s]'
    simple_format = '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s'
    id_simple_format = '[%(levelname)s][%(asctime)s] %(message)s'

    # 如果不存在定义的日志目录就创建一个
    if not os.path.isdir(logfile_dir):
        os.mkdir(logfile_dir)

    logfile_name = datetime.now().strftime('%Y-%m-%d') + '.log'  # log文件名

    # log文件的全路径
    logfile_path = os.path.join(logfile_dir, logfile_name)

    # log配置字典
    LOGGING_DIC = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': standard_format,
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'simple': {
                'format': simple_format
            }
        },
        'filters': {},
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',  # 打印到屏幕
                'formatter': 'simple'
            },
            'default': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',  # 保存到文件
                'filename': logfile_path,  # 日志文件
                'maxBytes': 1024 * 1024 * 5,  # 日志大小 5M
                'backupCount': 5,
                'formatter': 'standard',
                'encoding': 'utf-8',  # 日志文件的编码，再也不用担心中文log乱码了
            }
        },
        'loggers': {
            '': {
                'handlers': ['default', 'console'],  # 这里把上面定义的两个handler都加上，即log数据既写入文件又打印到屏幕
                'level': 'INFO',
                'propagate': True,  # 向上（更高level的logger）传递
            }
        },
    }
    logging.config.dictConfig(LOGGING_DIC)  # 导入上面定义的配置
    global wt
    wt = logging.getLogger(__name__)

def get_handle():
    return wt

def str_decode(re_data = None, type = None):
    try:
        wx = get_handle()
        re_data = re_data.decode(type)
    except Exception as error:
        wx.info('delete illegal string,try again...')
        # err_msg = str(error)
        pos = re.findall(r'([\d]+)inposition([\d]+):illegal', str(error).replace(' ', ''))
        if len(pos) == 1:
            re_data = re_data[0:int(pos[0][1])] + re_data[int(pos[0][1])+1:]
            re_data = str_decode(re_data, type)
            return re_data
    return re_data

