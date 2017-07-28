#!/usr/bin/python
#-*- coding:utf-8 -*-

from proxy_monitor import ProxyMonitor

ACCESS_MID = 10100


class AccessMonitor(ProxyMonitor):
    def __init__(self):
        ProxyMonitor.__init__(self, ACCESS_MID)
