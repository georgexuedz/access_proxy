import socket
import json
import struct

import tornado
import tornado.options
import tornado.ioloop
import tornado.web



_PACKAGE_MIN_LEN_ = 12
_PACKAGE_MAGIC_NUM_ = (ord('*') << 24) + (ord('B') << 16) + (ord('I') << 8) + (ord('T'))


def get_pkg_len(data):
    status, header_len, body_len = extra_pre_msg(data)
    if not status:
        return -1

    return _PACKAGE_MIN_LEN_ + header_len + body_len


def extra_pre_msg(data):
    data_len = len(data)

    if data_len < _PACKAGE_MIN_LEN_:
        return False, 0, 0

    (magic_num, header_len, body_len) = struct.unpack('!3I', data[:_PACKAGE_MIN_LEN_])
    if magic_num != _PACKAGE_MAGIC_NUM_:
        return False, 0, 0

    return True, header_len, body_len


def replace_header(data, new_header):
    try:
        status, header_len, body_len = extra_pre_msg(data)
        encoded_new_header = json.dumps(new_header)

        new_data = struct.pack('!3I', _PACKAGE_MAGIC_NUM_, len(encoded_new_header), body_len)
        new_data += encoded_new_header + data[_PACKAGE_MIN_LEN_ + header_len:]

        return True, new_data
    except Exception, e:
        return False, ''


def cut_one_package_out(data):
    status, header_len, body_len = extra_pre_msg(data)
    if not status:
        return '', ''

    pkg_len = _PACKAGE_MIN_LEN_ + header_len + body_len

    return_pkg = ''
    data_len = len(data)

    if data_len < pkg_len:
        return '', ''

    return data[:pkg_len], data[pkg_len:]


def decode_header(data):
    data_len = len(data)

    status, header_len, body_len = extra_pre_msg(data)
    if not status:
        return False, None, ''

    if data_len < _PACKAGE_MIN_LEN_ + header_len + body_len:
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

    return status, header, body_data


fd_map = {}
buf_map = {}


def handle_read(fd, event):
    global fd_map, buf_map
    sock = fd_map[fd]
    buf_map[fd] = sock.recv(1024)
    print 'fd(%d) recv buf(%s)\n', fd, buf_map[fd]


def handle_write(fd, event):
    global fd_map, buf_map

    if fd in buf_map and buf_map[fd]:
        fd_map[fd].send(get_resp_payload(buf_map[fd]))
        buf_map[fd] = ''


def handle_error(fd, event):
    global fd_map, buf_map


def pipe_event_handler(fd, event):
    if event & tornado.ioloop.IOLoop.READ:
        handle_read(fd, event)
    elif event & tornado.ioloop.IOLoop.WRITE:
        handle_write(fd, event)
    elif event & tornado.ioloop.IOLoop.ERROR:
        handle_error(fd, event)


def connect_access(mac):
    global fd_map
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # sock.connect(('39.108.145.92', 10100))
    sock.connect(('localhost', 10100))
    sock.send(get_req_payload(mac))
    sock.setblocking(0)
    fd_map[sock.fileno()] = sock
    tornado.ioloop.IOLoop.instance().add_handler(
        sock.fileno(),
        pipe_event_handler,
        tornado.ioloop.IOLoop.READ|tornado.ioloop.IOLoop.WRITE|tornado.ioloop.IOLoop.ERROR
    )

    return sock


def get_req_payload(mac):
    header = {
        'header_type': 0,
        'mac': mac,
    }
    body = {
        'cmd': 'my_cmd',
        'arg': 23423
    }
    header_buf = json.dumps(header)
    body_buf = json.dumps(body)
    payload = struct.pack('!3I', _PACKAGE_MAGIC_NUM_, len(header_buf), len(body_buf)) + header_buf + body_buf
    return payload


def get_resp_payload(buf):
    status, header, body_data = decode_header(buf)
    header['header_type'] = 1
    header['code'] = 0

    body = {
        'status': 'ok',
        'msg': {
            'color': 'read',
            'light': 232
        }
    }
    header_buf = json.dumps(header)
    body_buf = json.dumps(body)
    payload = struct.pack('!3I', _PACKAGE_MAGIC_NUM_, len(header_buf), len(body_buf)) + header_buf + body_buf
    return payload


for i in xrange(10):
    connect_access('mac' + str(i))
tornado.ioloop.IOLoop.instance().start()

