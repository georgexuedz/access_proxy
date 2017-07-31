#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys

import os
import time
import select
import socket
import traceback
import multiprocessing
import random
import struct
import errno
import uuid
import json

import redis

from log import log
import socket_helper
import json_helper
import header_helper

_g_fd_type_socket = 1
_g_fd_type_pipe = 2
_g_fd_type_push = 3


class Proxy:
    # fd map保存了这个proxy监听的所有fd的信息，包括类型、发送buffer、接收buffer等
    fd_map = {}
    # seq_map 保存了这个proxy生成的seq no 列表，包括来源的fd等
    # 为什么需要替换seq? 因为不同的客户端无法保证seq是不同的，所以只能通过proxy再转化为唯一的seq
    # seq_map     = {}
    # session_map 保存了fd和session的映射关系，push需要用
    # session_map = {}

    # 保持台灯的mac对应的tcp长连接
    mac_map = {}

    # =======================================
    worker_fd_list = []

    push_fd = None

    seq_no = None

    listen_fd    = None

    epoller      = None

    bind_addr    = "0.0.0.0"

    bind_port    = 10100

    #链接10秒无操作则超时
    keep_alive_timeout = 10
    #60秒无响应则删除seqno
    seqno_timeout      = 60

    #多久检查一次超时
    check_interval     = 60

    last_check_timestamp = time.time()

    my_mid             = 10100

    def __init__(self):

        self.host               = '0.0.0.0'
        self.port               = 10100
        self.keep_alive_timeout = 600
        self.seqno_timeout      = 120
        self.check_interval     = 60
        self.seq_no             = random.randint(0, 100000000)
        self.my_mid             = 10100

	'''
        self.redis_o = redis.Redis(
            host='localhost',
            port=6379,
            db=0
        )
        self.redis_mac_set_key = 'redis_mac_set'
	'''

        log.init({
            'module': 'access',
            "path": "/data/logs/service/access",
            'level': 'debug',    # debug|info|warning|error|none
            "level": "DEBUG",
            "handler": "rotating_file"
        })
        log.debug("[PROXY] ====== init proxy seq no start from [%d] ======" % (self.seq_no) )



    def add_pipe(self, pipe, is_worker=True):
        if pipe[0].fileno() in self.fd_map:
            log.error("[PROXY] the fd dict has the key ! fd[%d]" % (pipe[0].fileno()) )
            return

        value = {}
        value["pipe"]            = pipe[0]
        value["fd"]              = pipe[0].fileno()
        value["recv_buff"]       = ''
        value["recv_len_needed"] = 0
        value["send_buff"]       = ''
        if is_worker == True:
            value["type"]            = _g_fd_type_pipe
            self.worker_fd_list.append(value["fd"])
        else:
            value["type"]            = _g_fd_type_push
            self.push_fd             = value["fd"]
        self.fd_map[pipe[0].fileno()] = value


    def generate_seq(self):
        self.seq_no = self.seq_no + 1
        return self.seq_no

    def get_worker_fd(self):
        index = random.randint(0,100000000) % len(self.worker_fd_list)
        return self.worker_fd_list[index]

    def listen(self):

        fd = None
        try:
            fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            fd.bind((self.host, self.port))
            fd.listen(1024)
            fd.setblocking(0)

        except socket.error, msg:
            log.error("[PROXY] try to listen fd fail.host=[{}],port=[{}], msg[{}]" . format(self.host,self.port,msg) )
            sys.exit(1)
            #return None

        return fd

    def handle_close_fd(self,fd):
        if self.fd_map[fd]["type"] != _g_fd_type_socket:
            log.error("[PROXY] You can not close the NON socket fd (%d)!" %(fd))
            return

        log.debug("[PROXY] close the fd(%d), from (%s)" % (fd,self.fd_map[fd]["addr"]))

        #要从epoll取消注册
        self.epoller.unregister(fd)

        #log.debug("[PROXY] the fd seq_list = [{}]" . format(self.fd_map[fd]["seq_list"]))
        #log.debug("[PROXY] the proxy seq_map = [{}]" . format(self.seq_map))

        #要把本fd关闭
        conn = self.fd_map[fd]["conn"]
        del self.fd_map[fd]
        conn.close()  #因为fd有引用的时候，就不会发fin包，所以先拷贝出来，删除map以后再最后close

    def check_timeout(self):
        #这里需要遍历所有的fd，应该放在一个线程里面去检查的，这样子会效率更高，先简单做
        now = time.time()

        #检查时间没到，啥也不干
        if (now - self.last_check_timestamp) < self.check_interval:
            return

        log.debug('[PROXY] check time out seqs')

        self.last_check_timestamp = now

        timeout_fds = []
        for (k, v) in self.fd_map.items():
            #如果是socket才处理
            if v["type"] == _g_fd_type_socket:
                time_idle = now - v["last_actvie_time"]
                #log.debug("fd(%d) idle(%d) sec" %(k,time_idle))
                if time_idle > self.keep_alive_timeout:
                    timeout_fds.append(k)

        if len(timeout_fds) > 0:
            log.debug("[PROXY] timeout fds = {}" . format(timeout_fds))

        for v in timeout_fds:
            self.handle_close_fd(v)

    def handle_epoll_connect(self):
        try:
            conn, addr = self.listen_fd.accept()
        except socket.error, msg:
            log.info("[PROXY]  accept exception: %s" % msg)
            return

        if conn.fileno() in self.fd_map:
            log.error("[PROXY] something wrong when connected with the fd(%d),already have one! close" % (conn.fileno()) )
            conn.close()
            return

        conn.setblocking(0)

        conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)

        value = {}
        value["fd"]   = conn.fileno()
        value["conn"] = conn
        value["type"] = _g_fd_type_socket
        value["addr"] = addr
        value["conn_time"]        = time.time()
        value["last_actvie_time"] = time.time()
        value["recv_buff"]        = ''
        value["recv_len_needed"]  = 0
        value["send_buff"]        = ''
        value["seq_list"]         = []
        value["session_key"]      = ''

        self.fd_map[conn.fileno()] = value
        self.epoller.register(conn.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)

        log.debug('[PROXY] accept conn from[%s] fd[%s]' % (addr, conn.fileno()))
        #log.debug("[PROXY] the fd map is {}" .format(self.fd_map))

        return

    def handle_epoll_in(self,fd):
        if fd not in self.fd_map:
            log.error("[PROXY] unkown fd(%d) ,not found in map" % (fd) )
            return
        #log.debug("[PROXY] try to recv data from fd[{}]" . format(self.fd_map[fd]) )
        if self.fd_map[fd]["type"] == _g_fd_type_socket:
            return self.handle_sock_in(self.fd_map[fd])
        else:
            return self.handle_pipe_in(self.fd_map[fd])

        #log.error("[PROXY] unknow fd [%d] type [%d] " % (fd,self.fd_map[fd]["type"]) )
        return

    def handle_pipe_in(self,fd_info):
        try:
            pipe_data = fd_info["pipe"].recv()
        except Exception,e:
            log.error('[PROXY] recv data from pipe failed. [%s]' % e)

        fd_info["recv_buff"]       = pipe_data
        fd_info["recv_len_needed"] = len(pipe_data)

        self.handle_full_recved_pkg_no_seq(fd_info)

    def handle_sock_in(self,fd_info):
        ret = None
        if fd_info["recv_len_needed"] == 0:
            ret = socket_helper.sock_recv_whole_data(fd_info)
        elif fd_info["recv_len_needed"] > len(fd_info["recv_buff"]):
            ret = socket_helper.sock_recv_remain_data(fd_info)
        else:
            log.error("[PROXY] something wrong")
            self.handle_close_fd(fd_info["fd"])
            return

        #说明出错了，需要关闭fd
        if ret < 0:
            self.handle_close_fd(fd_info["fd"])
            return

        fd_info["last_actvie_time"] = time.time()

        #ret == 0说明包已经收完整了，可以处理了
        if ret == 0:
            self.handle_full_recved_pkg_no_seq(fd_info)
        return


    def handle_full_recved_pkg_no_seq(self,fd_info):
        status, header, body_data = json_helper.decode_header(fd_info["recv_buff"])
        if not status:
            log.debug("[PROXY ] recv package => %s", fd_info["recv_buff"])
            log.error("[PROXY ] recv UNKOWN PACKAGES!!! data_len(%d) drop it! " % (len(fd_info["recv_buff"])) )
            fd_info["recv_buff"]       = ''
            fd_info["recv_len_needed"] = 0
            return
        
        log.debug("header=>(%s)\n", header_helper.serialize_header(header))
        self.add_data2dst_fd_buff_no_seq(header, fd_info)

    def deal_sendcmd_to_device(self, header, fd_info):
        mac = header[header_helper._g_header_mac_key]

        if mac not in self.mac_map:
            log.error("[!] unrecognized mac(%s)!", mac)
            return -1

        return self.mac_map[mac]

    def deal_device_conneted_msg(self, fd_info, header):
        mac = header_helper.get_mac(header)
	
	'''
        if not self.redis_o.sismember(
                self.redis_mac_set_key,
                mac):
            log.error("![unbinded mac device]")
            return False
	'''

        self.mac_map[mac] = fd_info['fd']
        log.debug("fd(%d) bind to mac(%s)", fd_info['fd'], mac)
        return True

    def send_buf_to_dst_fd(self, fd_info, dst_fd):
        self.fd_map[dst_fd]["send_buff"]   += fd_info["recv_buff"]
        fd_info["recv_buff"]       = ''
        fd_info["recv_len_needed"] = 0

    def add_data2dst_fd_buff_no_seq(self, header, fd_info):
        dst_fd = -1

        if header_helper.if_req(header):
            if fd_info["type"] == _g_fd_type_socket:
                # 设备请求登记信息
                self.deal_device_conneted_msg(fd_info, header)
                fd_info["recv_buff"]       = ''
                fd_info["recv_len_needed"] = 0
                # 直接返回了
                return True
            elif fd_info["type"] == _g_fd_type_push:
                # 请求设备
                # dst_fd = xxx
                dst_fd = self.deal_sendcmd_to_device(header, fd_info)
                if dst_fd == -1:
                    fd_info["recv_buff"]       = ''
                    fd_info["recv_len_needed"] = 0
                    return False
        else:
            if fd_info["type"] == _g_fd_type_socket:
                # 设备响应
                dst_fd = self.push_fd

        self.send_buf_to_dst_fd(fd_info, dst_fd)
        self.handle_epoll_out(dst_fd)
        return True

    def _init_fd_read_event(self, fd):
        #已经全部发送完毕, 不需要注册写事件了
        self.fd_map[fd]["send_buff"] = ''
        self.epoller.modify(fd, select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)

    def _init_fd_read_write_event(self, fd):
        #要注册epoll可写事件，下次触发的时候重发
        self.epoller.modify(fd, select.EPOLLOUT | select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)

    def _send_buf_to_pipe(self, fd):
        if 'pipe' not in self.fd_map[fd] or not self.fd_map[fd]["pipe"]:
            log.error('fd[%s] none pipe_obj' % str(fd))
            return

        buf = self.fd_map[fd]["send_buff"]
        buf_len = len(buf)
        pipe_obj = self.fd_map[fd]["pipe"]

        try:
            log.debug("will send data 2 pipe fd(%d) ,len(%d)", fd, buf_len)

            pipe_obj.send(buf)
            self._init_fd_read_event(fd)
        except Exception, e:
            log.error(str(traceback.format_exc()))
            log.error('send_to_pipe fail! register read write!')
            self.monitor_o.pipe_send_err(self.fd_map[fd]["proxy_seq"],
                                         fd)
            self._init_fd_read_write_event(fd)

    def _send_buf_to_sock(self, fd):
        if 'conn' not in self.fd_map[fd] or not self.fd_map[fd]["conn"]:
            log.error('fd[%s] none conn_obj' % str(fd))
            return

        buf = self.fd_map[fd]["send_buff"]
        buf_len = len(buf)
        conn_obj = self.fd_map[fd]["conn"]

        log.debug("will send data 2 conn fd(%d),len(%d)", fd, buf_len)

        try:
            send_len = conn_obj.send(buf)
            log.debug('send_buf len[%d]' % send_len)

        except Exception, e:
            log.error(str(traceback.format_exc()))
            log.error(str(e))

            if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK, errno.EINTR]:
                self._init_fd_read_write_event(fd)
                log.error('send_to_sock fail! block err! register read write!')
            else:
                self.monitor_o.sock_send_err(self.fd_map[fd]["proxy_seq"],
                                             fd)
                self.handle_close_fd(fd)
                log.error('send_to_sock fail! unknow err! close fd!')

            return

        if buf_len == send_len:
            self._init_fd_read_event(fd)
        else:
            if fd in self.fd_map:
                if 'send_buff' in self.fd_map[fd]:
                    self.fd_map[fd]["send_buff"] = buf[send_len:]
                if 'last_actvie_time' in self.fd_map[fd]:
                    self.fd_map[fd]["last_actvie_time"] = time.time()

            log.debug('send some buf, register read write, next time!')
            self._init_fd_read_write_event(fd)


    def handle_epoll_out(self, fd):
        f = None
        msg = ''

        if len(self.fd_map[fd]["send_buff"]) <= 0:
            msg = 'buf len <= 0'
            f = self._init_fd_read_event
        elif self.fd_map[fd]["type"] == _g_fd_type_socket:
            msg = 'send to sock'
            f = self._send_buf_to_sock
        elif self.fd_map[fd]["type"] == _g_fd_type_pipe:
            msg = 'send to pipe'
            f = self._send_buf_to_pipe
        elif self.fd_map[fd]["type"] == _g_fd_type_push:
            msg = 'send to push'
            f = self._send_buf_to_pipe
        else:
            log.error('invalid fd_type for fd[%s], fd_map[%s]' %
                      (str(fd), str(self.fd_map[fd])))
            return

        f(fd)
        log.debug('handle epoll out! fd[%s] %s' % (str(fd), msg))

    def handle_fd(self):
        #do something
        while(True):
            try:
                #0.1秒超时
                epoll_list = self.epoller.poll(0.1)
            except IOError as e:
                if e.errno != errno.EINTR:
                    raise
                else:
                    log.debug("[PROXY] catch an interupt")
                    continue

            # print _clients
            for fd, events in epoll_list:

                if fd == self.listen_fd.fileno():
                    #log.debug("[PROXY] listen fd[%d] get events" % (fd))
                    self.handle_epoll_connect()
                elif select.EPOLLIN & events:
                    log.debug("[PROXY] epoll =======================================> fd[%d] get read events(%d)" % (fd,events))
                    self.handle_epoll_in(fd)
                    continue
                elif select.EPOLLOUT & events:
                    self.handle_epoll_out(fd)
                    continue
                elif (select.EPOLLHUP & events) or (select.EPOLLERR & events):
                    log.error("[PROXY] fd: [%d] error" % fd)
                    self.handle_close_fd(fd)
                    continue
                else:
                    continue

            # check time out
            self.check_timeout()

    def run(self):

        self.epoller = select.epoll()

        for (k, v) in self.fd_map.items():
            log.debug("[PROXY] init, reg pipe fd [%d] into PROXY !" % (k) )
            self.epoller.register(k,select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)

        self.listen_fd = self.listen()
        self.epoller.register(self.listen_fd.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)
        log.debug("[PROXY] init, reg listen fd [%d] into PROXY !" % (self.listen_fd.fileno()))

        self.handle_fd()

