#!/usr/bin/python
# -*- coding:utf-8 -*-

import json
import struct

from log import log


_PACKAGE_MIN_LEN_ = 12       # 最小包长度, 包括前置 4 字节 magic_num, 4 字节 header_len, 4 字节 body_len
_PACKAGE_MAGIC_NUM_ = (ord('*') << 24) + (ord('B') << 16) + (ord('I') << 8) + (ord('T'))


def get_pkg_len(data):
    status, header_len, body_len = extra_pre_msg(data)
    if not status:
        return -1

    return _PACKAGE_MIN_LEN_ + header_len + body_len


def extra_pre_msg(data):
    data_len = len(data)

    if data_len < _PACKAGE_MIN_LEN_:
        log.error("pre_msg too small, len[%d]" % data_len)
        return False, 0, 0

    (magic_num, header_len, body_len) = struct.unpack('!3I', data[:_PACKAGE_MIN_LEN_])
    if magic_num != _PACKAGE_MAGIC_NUM_:
        log.error("check_package: magic_num[%s] should be[%s] data.length[%s]"
                   % (magic_num, _PACKAGE_MAGIC_NUM_, len(data)))
        return False, 0, 0

    return True, header_len, body_len


def pack_one_msg(header, body=None):
    try:
        if not header:
            return ''
        header_data = json.dumps(header)
        header_len = len(header_data)

        body_len = 0
        body_data = ''
        if body:
            body_data = json.dumps(body)
            body_len = len(body_data)

        data = struct.pack('!3I', _PACKAGE_MAGIC_NUM_, header_len, body_len)
        data += header_data + body_data
        return data
    except Exception, e:
        log.error("pack_one_msg err!" + str(e))
        return ''


def replace_header(data, new_header):
    try:
        status, header_len, body_len = extra_pre_msg(data)
        encoded_new_header = json.dumps(new_header)
        
        new_data = struct.pack('!3I', _PACKAGE_MAGIC_NUM_, len(encoded_new_header), body_len)
        new_data += encoded_new_header + data[_PACKAGE_MIN_LEN_ + header_len:]

        return True, new_data
    except Exception, e:
        log.error("replace_header err!" + str(e))
        return False, ''


def cut_one_package_out(data):
    status, header_len, body_len = extra_pre_msg(data)
    if not status:
        return '', ''

    pkg_len = _PACKAGE_MIN_LEN_ + header_len + body_len

    return_pkg = ''
    data_len = len(data)

    if data_len < pkg_len:
        log.error('pkg is too short to cut!')
        return '', ''

    return data[:pkg_len], data[pkg_len:]


def decode_header(data):
    data_len = len(data)

    status, header_len, body_len = extra_pre_msg(data)
    if not status:
        return False, None, ''

    if data_len < _PACKAGE_MIN_LEN_ + header_len + body_len:
        log.error("package too small, len[%d]" % data_len)
        return False, None, ''

    status = False
    header = None
    body_data = ''
    try:
        header = json.loads(data[_PACKAGE_MIN_LEN_: _PACKAGE_MIN_LEN_ + header_len])
        status = True
        body_data = data[_PACKAGE_MIN_LEN_ + header_len:]
    except Exception, e:
        header = None
        log.error("loads header(%s) fail! %s", data[_PACKAGE_MIN_LEN_: _PACKAGE_MIN_LEN_ + header_len], str(e))

    return status, header, body_data


if __name__ == '__main__':
    pass
