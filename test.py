# -*- coding:utf-8 -*-

# 配置日志,通过日志记录运行节点
from logging import getLogger, DEBUG, FileHandler, Formatter
import sys
import time

reload(sys)
sys.setdefaultencoding('utf-8')

logger = getLogger('mylogger')
logger.setLevel(DEBUG)
m_handler = FileHandler(u'success.log', 'a')  # 追加写入日志,断开后重新运行接着上次
m_handler.setLevel(DEBUG)
m_handler.setFormatter(Formatter("%(message)s"))

logger.addHandler(m_handler)


def my_callback(res):
    """写入文件统一回调函数"""
    with open(u'sign.csv', 'a+') as f:
        print u'start'
        f.write(res + '\n')
        print u'end'


def parse_data(sus):
    for c in range(10000):
        if unicode(str(c) + '\n') not in sus:
            time.sleep(1)
            logger.debug(u'%s' % c)
            print u'处理逻辑-', c


def get_success():
    success = []
    with open(u'success.log', 'a+') as f:
        results = f.readlines()
        for res in results:
            success.append(unicode(res))
        return success


if __name__ == '__main__':
    su = get_success()
    time.sleep(3)
    parse_data(su)
