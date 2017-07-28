#!/usr/bin/python
#-*- coding:utf-8 -*-
import os
import commands
import atexit
import traceback
from datetime import datetime


__all__ = ['RunEnvRecordCls', ]


class RunEnvRecordCls(object):
    env = {
         'time': 'date -R', 
         'ps': 'ps -ef',
         'net': 'netstat -apn'
    }
   
    @staticmethod
    def get_env_dict():
        d = dict()

        for typ, cmd in RunEnvRecordCls.env.iteritems():
            d[typ] = commands.getoutput(cmd)

        return d
    
    @staticmethod
    def get_env_string():
        s = ''

        for typ, out in RunEnvRecordCls.get_env_dict().iteritems():
            s += '[%s]\n%s\n'%(typ, out)

        return s


    def __init__(self, srv, path='/data/logs/service/'):
        self.log = os.path.join(path, srv)
        self.log = '{name}.blackbox'.format(name=self.log)
        
    def _record(self, msg):
        with open(self.log, 'a') as f:
            f.write(msg)

    def record_one_bill(self, msg):
        out = '[%s]:%s\n'%(str(datetime.now()), msg)
        self._record(out)

    def record_begin(self):
        out = '[%s]\n%s\n'%('begin', RunEnvRecordCls.get_env_string())
        self._record(out)

    def record_end(self):
        out = '[%s]\n%s\n'%('end', RunEnvRecordCls.get_env_string())
        self._record(out)
        
    def record_bt(self):
        self.record_one_bill(str(traceback.format_exc()))
        
        
if __name__ == '__main__':
    record_env_o = RunEnvRecordCls('access')
    record_env_o.record_begin()
    record_env_o.record_end()
    record_env_o.record_one_bill('love you')
    record_env_o.record_bt()
