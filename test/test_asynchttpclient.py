import json
import struct

import requests

import tornado
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado import gen

# url = 'https://mlogin.plaync.com/login/signin?site_id=13&return_url=http%3A//kr.plaync.com/'


def get_req(mac):
    _PACKAGE_MAGIC_NUM_ = (ord('*') << 24) + (ord('B') << 16) + (ord('I') << 8) + (ord('T'))
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


def http_get(mac):
    payload = get_req(mac)
    url = 'http://192.168.1.109:10102/'
    r = requests.post(url, data=payload)
    if r.status_code != 200:
        print ('[!] opp, mac(%s) is err!\n', mac)
    else:
	print r.text


def post_callback(response):
    if response.code != 200:
        print 'err'
    else:
	print response.body


@gen.coroutine
def aync_post(payload):
    url = 'http://localhost:10102/'

    http_client = AsyncHTTPClient()
    response = yield http_client.fetch(
        url,
        callback=post_callback,
        method='POST',
        body=payload
    )
    # raise gen.Return(response.body)


@gen.coroutine
def run():
    mac_ls = ['mac' + str(i) for i in range(10)]
    payload_ls = [get_req(mac) for mac in mac_ls]
    yield [aync_post(payload) for payload in payload_ls]


IOLoop.current().run_sync(run)

