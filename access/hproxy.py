#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
import logging
import random
import traceback
import multiprocessing

import tornado.options
import tornado.ioloop
import tornado.web

from log import log
import json_helper


global session_keeper


class SessionKeeper:
    
    seq_map      = {}
    
    pending_reqs = []    
    
    
    hproxy_seq   = 0
    
    def __init__(self):
        self.hproxy_seq =  random.randint(10000, 100000000)
        log.debug("init HPROXY seq .  start from (%d) " % (self.hproxy_seq) )
    
    def generate_seq(self):
        self.hproxy_seq = self.hproxy_seq + 1
        return self.hproxy_seq
    
    def add_session(self, req_data, callback_fun):
        hproxy_seq = self.generate_seq()
        self.seq_map[hproxy_seq] = {
            'callback': callback_fun,
            'create_time': time.time(),
            'push_seq': 0
        }

        status, header, body_data = json_helper.decode_header(req_data)
        if not status:
            return

        header['seq'] = hproxy_seq
        status, new_req_data = json_helper.replace_header(req_data, header)
        if not status:
            return

        self.pending_reqs.append(new_req_data)
        log.debug("[HPROXY] Register seq(%d),len(%d). waiting pkg cnt(%d) " % (hproxy_seq,len(new_req_data),len(self.pending_reqs)) )

    def pop_session_rsp_func(self, seqno):
        if seqno not in self.seq_map:
            return None,None
        callback =  self.seq_map[seqno]["callback"]
        push_seq =  self.seq_map[seqno]["push_seq"]
        del self.seq_map[seqno]
        
        return callback,push_seq
        

    def pop_pending_req(self):
        if len(self.pending_reqs) == 0:
            return None
        req_data = self.pending_reqs[0]        
        del self.pending_reqs[0]        
        return req_data
                                          
    def check_timeout(self,now,timeout):        
        
        log.debug('[HPROXY] check time out seqs')        
        need_drop = []                        
        for (k,v) in self.seq_map.items():
            if (now - v["create_time"]) > timeout:
                need_drop.append(k)                
        for ele in need_drop:
            log.debug('[HPROXY] del the timeout seq_no (%d)' % (ele) )
            del self.seq_map[ele]

session_keeper = SessionKeeper()

class PushHttpHandler(tornado.web.RequestHandler):
    
    @tornado.web.asynchronous 
    def post(self, *args, **kwargs):
        global session_keeper;
        
        body_len = int(self.request.headers["Content-Length"])
        
        log.debug("[HPROXY] recv HTTP POST req. request info  = [{}] , body len = [{}]" . format(self.request,body_len) )
        
        session_keeper.add_session(self.request.body, self.on_resp)
        
            
    def get(self, *args, **kwargs):                
        log.debug("[HPROXY] recv HTTP GET req. request = [{}]" . format(self.request) )
        
        
    def on_resp(self, resp_data,ret_code,seq_no):
        log.debug("on_resp is call, build HTTP RESP. seqno(%d),ret_code(%d)" % (seq_no,ret_code) )
        self.set_status(200)
        self.set_header('Content-type','application/octet-stream')
        self.write(resp_data)
        self.flush()        
        self.finish()            


def log_handler():
    pass


class HProxy:
    
    pipe = None
    
    def __init__(self):
        self.port                       = 10102
        self.check_interval             = 60
        self.seqno_timeout              = 120
        self.last_check_time            = time.time()
    
    def add_pipe(self,pipe):                
        self.pipe = pipe[1] 
                
    def pipe_event_handler(self, fd, event):
        if event & tornado.ioloop.IOLoop.READ:
            self.handle_read(fd, event)
        elif event & tornado.ioloop.IOLoop.WRITE:      
            self.handle_write(fd, event)
        elif event & tornado.ioloop.IOLoop.ERROR:
            self.handle_error(fd, event)
            
    def handle_read(self,fd,event):
        
        global session_keeper
        
        resp_data = ''
        
        try:
            resp_data = self.pipe.recv()
            
        except Exception,e:
            log.error('[HPROXY] recv data from pipe failed. [%s] ,remain pkg cnt(%d)' % (e,len( session_keeper.pending_reqs) ) )            
        
        log.debug('[hproxy] recv package from pip (%s)', resp_data)
        
        try:
            while len(resp_data) > 0:
                this_pkg, resp_data = json_helper.cut_one_package_out(resp_data)
                if not this_pkg:
                    continue

                status, header, body = json_helper.decode_header(this_pkg)
                if not status:
                    continue

                callback, push_seq = session_keeper.pop_session_rsp_func(header['seq'])
                if not callback:
                    log.error("[HPROXY] package seqno (%d) not found! drop it!!!!" % (header['seq']))
                    continue
                            
                log.debug("[HPROXY] get RSP, seq_no(%d) for data. len(%d). recved buff len(%d) " % (header['seq'], len(this_pkg), len(resp_data)))
                callback(this_pkg, header['code'], header['seq'])
        except Exception, e:
            log.error("[hproxy] handle read error! (%s)", str(e))
                 
        
    def handle_write(self,fd,event):
        
        global session_keeper
        
        while(True):
            req = session_keeper.pop_pending_req()
            if req is None:
                #无读数据，sleep 20毫秒,降低cpu使用率
                time.sleep(0.02)
                break
            try:
                self.pipe.send( req )                
                log.debug("[HPROXY] send PUSH REQ 2 proxy through PIPE. len(%d)" % ( len(req)) )
            except Exception,e:
                log.error('[HPROXY] send data 2 pipe failed. [%s] ,remain pkg cnt(%d)' % (e, len( session_keeper.pending_reqs) ) )                
                
        now = time.time()
        if (now - self.last_check_time) > self.check_interval:
            session_keeper.check_timeout(now,self.seqno_timeout)
            self.last_check_time = now        
               
    def handle_error(self,fd,event):                
        log.error("[HPROXY] fd (%d) recv error event(%d)" % (fd,event) )
        return
   
                          
    def do(self):
        try:
            application = tornado.web.Application([(r"/", PushHttpHandler),])  
            application.listen(self.port)
            tornado.ioloop.IOLoop.instance().add_handler( self.pipe.fileno(),
                                                          self.pipe_event_handler, 
                                                          tornado.ioloop.IOLoop.READ|tornado.ioloop.IOLoop.WRITE|tornado.ioloop.IOLoop.ERROR )

            logging.getLogger("tornado.access").addHandler(log_handler)
            logging.getLogger("tornado.access").propagate = False
            logging.getLogger("tornado.application").addHandler(log_handler)
            logging.getLogger("tornado.application").propagate = False
            logging.getLogger("tornado.general").addHandler(log_handler)
            logging.getLogger("tornado.general").propagate = False

            tornado.ioloop.IOLoop.instance().start()
        except:
            log.error(str(traceback.format_exc()))
    
    def run(self):
        p = multiprocessing.Process(target=self.do)
        p.start()

