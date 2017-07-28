#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import multiprocessing
import os
import commands
import atexit
import traceback
import signal

from run_env_record import *


sys.path.append('./access')
from log import log


g_record_env_o = RunEnvRecordCls('access')

@atexit.register
def record_exit():
    g_record_env_o.record_end()

def signal_handler(sig, frame):
    msg = ''
    if sig == signal.SIGINT:
        msg = 'ctrl + c'
    elif sig == signal.SIGTERM:
        msg = 'kill -15'
    else:
        msg = 'unknow sig'

    g_record_env_o.record_one_bill('![exit with sig: %s]'%msg)
    sys.exit(-1)

def chk_live_process():
    cmd = "ps aux | grep access.py | grep -v %s | grep -v grep"
    cmd = cmd % str(os.getpid())

    s, o = commands.getstatusoutput(cmd)
    if s == 0:
        return False
    return True

def chk_port_available():
    # 10100 proxy, 10102 hproxy 
    ports = ['10100', '10102']

    for port in ports:
        if commands.getstatusoutput('netstat -apn |grep %s'%port)[0] == 0:
            g_record_env_o.record_one_bill('port[%s] not available!' % str(port))
            return False
        
    return True

def ready_env():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    '''
    # 10100和10102(tornado也设置了)都是设置了SO_REUSEADDR，所以没必要判断
    if not chk_port_available():
        g_record_env_o.record_one_bill('![port not available]')
        sys.exit(-1)
    '''
        
    if not chk_live_process():
        g_record_env_o.record_one_bill('![access is alive]')
        sys.exit(-1)
        
def run():
    # 要等初始化环境后，才能引用common_pb2.py
    from access.proxy  import Proxy
    from access.hproxy import HProxy

    try:
    #初始化proxy模块                
        proxy = Proxy()
        
    #初始化hproxy模块                
        hproxy = HProxy()     
        pipe   = multiprocessing.Pipe()
        proxy.add_pipe(pipe,False)
        hproxy.add_pipe(pipe)    
        hproxy.run()

        proxy.run()
    except:
        log.error(traceback.format_exc())

if __name__ == "__main__":
    ready_env()
    run()
