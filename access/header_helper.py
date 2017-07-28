#!/usr/bin/python
#-*- coding:utf-8 -*-

import json

from log import log


_g_header_type_key = 'header_type'
_g_header_type_req = 0
_g_header_type_rsp = 1

_g_header_mac_key = 'mac'
_g_header_openid_key = 'openid'
_g_header_seq_key = 'seq'
_g_header_code_key = 'code'


def serialize_header(header):
    return json.dumps(header)


def if_req(header):
    if header[_g_header_type_key] == _g_header_type_req:
        return True
    return False


def get_mac(header):
    return header[_g_header_mac_key]

