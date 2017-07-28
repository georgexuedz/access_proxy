#!/usr/bin/python
#-*- coding:utf-8 -*-

import fcntl
import struct
import socket
import log
import errno
from json_helper import _PACKAGE_MIN_LEN_,_PACKAGE_MAGIC_NUM_,get_pkg_len

#最大包长度5M
_MAX_PKG_LEN_ = 5242880


#fd_info 是一个dictionary，所以传的是引用，可以修改内容

def sock_recv_remain_data(fd_info):
    fd   = fd_info["conn"].fileno()
    addr = fd_info["addr"]
    remain_len = fd_info["recv_len_needed"] - len(fd_info["recv_buff"])
    
    if remain_len <= 0:
        log.error("[PROXY] something wrong in recv,fd(%d),needed(%d),got(%d),remain(%d) " % (fd,recv_len_needed,len(recv_buff),remain_len) )
        return -1
    data = None
    while(True):
        try:
            data = fd_info["conn"].recv(remain_len)
            if not data:
                log.error("[PROXY] no data recv, fd[{}] addr[{}],remain[{}]" . format(fd, addr,remain_len))
                #return remain_len
                return -1
            else:
                fd_info["recv_buff"] += data
                remain_len = remain_len - len(data)
                log.debug("[PROXY] get data.len[%d], remain[%d],fd[%d], addr[%s]" % (len(data),remain_len ,fd, addr))                
                if remain_len == 0:
                    return 0
            
        except socket.error, msg:
            if (msg.errno == errno.EAGAIN) or (msg.errno == errno.EWOULDBLOCK) or (msg.errno == errno.EINTR):
                log.debug("[PROXY] fd[{}] is not readable,try to recv next time,remain[{}], msg[{}]" . format(fd,remain_len,msg) )
                return remain_len
            else:
                log.error("[PROXY] fd[{}] ,remain[{}] ,recv error ,unknow occured, close msg[{}]" . format(fd,remain_len,msg) )
                return -1


def sock_recv_whole_data(fd_info):
    
    fd   = fd_info["conn"].fileno()
    addr = fd_info["addr"]
    try:
        header_data = fd_info["conn"].recv(_PACKAGE_MIN_LEN_,socket.MSG_PEEK)   
        if not header_data:
            log.error("[PROXY] no header data recv, fd[{}] addr[{}],close fd" . format(fd, addr))                            
            return -1
                     
        if len(header_data) < _PACKAGE_MIN_LEN_ :
            log.error("[PROXY] recv header too short, only [%d],but[%d] wanted,close fd[%d]" % (len(header_data),_PACKAGE_MIN_LEN_,fd) )
            return -1 
                   
        pkg_len = get_pkg_len(header_data)        
        
        log.debug("[PROXY] recv header data ok, pkg_len = %d" % (pkg_len))
        
        if (pkg_len <= _PACKAGE_MIN_LEN_) or (pkg_len > _MAX_PKG_LEN_):
            log.error("[PROXY] invalid pkg len [{}], from fd [{}], addr[{}]". format(pkg_len,fd,addr))
            return -1
            
        fd_info["recv_len_needed"] = pkg_len            
        remain_len = sock_recv_remain_data(fd_info)
        
        if remain_len < 0:
            log.error("[PROXY] something wrong happen!pkg len [{}], from fd [{}], addr[{}]". format(pkg_len,fd,addr))
            return -1
        
        return remain_len
        
    except socket.error, msg:
        if (msg.errno == errno.EAGAIN) or (msg.errno == errno.EWOULDBLOCK) or (msg.errno == errno.EINTR):
            log.debug("[PROXY] fd[{}] is not readable,try to recv next time,remain[{}], msg[{}]" . format(fd,_PACKAGE_MIN_LEN_,msg) )
            return 0
        else:
            log.error("[PROXY] something wrong happen!pkg len [{}], from fd [{}], addr[{}]". format(_PACKAGE_MIN_LEN_,fd,addr))
            return -1    

def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])
