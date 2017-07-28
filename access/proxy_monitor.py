#!/usr/bin/python
# -*- coding:utf-8 -*-

'''
***************2015-10-21*******************
1.access的配置加bill;
2.access的配置加monitor的编译;
3.* 目前push先不做，迭代
4.* 目前没有rpc，这个应该放到worker去做
********************************************
'''
from monitor.monitor_api_pb2 import ReportBillReq, MsgStats
import socket
import struct
import traceback
from log import log
from bill import bill
import pb_helper
import time
from pb_rpc_agent import PbRpcAgent


__all__ = ['ProxyMonitor', ]


###########################################
class LimitMap(object):
    def __init__(self, max_key):
        self.o = dict()
        self.max_key_cnt = max_key
        self.key_cnt = 0
        self.is_drop = False

    def has_drop(self):
        return self.is_drop

    def set(self, k, v):
        if self.key_cnt >= self.max_key_cnt:
            self.is_drop = True
            return False

        self.o[k] = v
        self.key_cnt += 1

    def get(self, k, default):
        if self.has(k):
            return self.o[k]

        return default

    def get_key_ls(self):
        return self.o.keys()

    def has(self, k):
        if k in self.o:
            return True
        return False

    def length(self):
        return len(self.o)

    def get_statistic(self):
        MAX_SHOW_CNT = 50
        return '[max: %d, length: %d, %s]' % \
            (self.max_key_cnt,
             self.length(),
             str(self.o.keys()[:MAX_SHOW_CNT]))

    def clear(self):
        self.o.clear()
        self.key_cnt = 0

    def del_one_key(self, key):
        if not self.has(key):
            return

        self.key_cnt -= 1
        del self.o[key]

    def del_keys(self, keys):
        valid_keys = set(keys) & set(self.o.keys())
        for key in valid_keys:
            self.key_cnt -= 1
            del self.o[key]


###########################################
class TimeableLimitMap(LimitMap):
    def set(self, k,
            v):
        key = k
        val = (time.time(), v)
        LimitMap.set(self, key, val)

    def get_time(self, k,
                 default):
        if not LimitMap.has(self, k):
            return default

        info = LimitMap.get(self, k, default)
        return info[0]

    def get_val(self, k,
                default):
        if not LimitMap.has(self, k):
            return default

        info = LimitMap.get(self, k, default)
        return info[1]

###########################################
class ErrCollection(TimeableLimitMap):
    def __init__(self, max_key):
        TimeableLimitMap.__init__(self, max_key)
        self.set_errinfo()

    def set_errinfo(self):
        from context import context
        self.errinfo = context.errcode['SUCCESS']

    def get_err_id(self):
        return self.errinfo[0]

    def get_err_info(self):
        return self.errinfo[1]

    def get_err_msg(self):
        return self.errinfo[2]

class ErrPushSessionKeyMiss(ErrCollection):
    def set_errinfo(self):
        from context import context
        self.errinfo = context.errcode['PROXY_MONITOR_PUSH_SESSION_KEY_MISS']

    def set(self, seq,
            session_key):
        ErrCollection.set(self, seq, session_key)

class ErrPipeSend(ErrCollection):
    def set_errinfo(self):
        from context import context
        self.errinfo = context.errcode['PROXY_MONITOR_PIPE_SEND']

    def set(self, seq, fd):
        ErrCollection.set(self, seq, fd)

class ErrSockSend(ErrCollection):
    def set_errinfo(self):
        from context import context
        self.errinfo = context.errcode['PROXY_MONITOR_SOCK_SEND']

    def set(self, seq,
            fd):
        ErrCollection.set(self, seq, fd)

class ErrSockSeqMiss(ErrCollection):
    def set_errinfo(self):
        from context import context
        self.errinfo = context.errcode['PROXY_MONITOR_SOCK_SEQ_MISS']

    def set(self, seq):
        ErrCollection.set(self, seq, None)

class ErrRspMiss(ErrCollection):
    def set_errinfo(self):
        from context import context
        self.errinfo = context.errcode['PROXY_MONITOR_SOCK_RSP_MISS']

    def set(self, seq):
        ErrCollection.set(self, seq, None)


###########################################
class Data(object):
    def __init__(self, max_key):
        self.req_o = TimeableLimitMap(max_key)
        self.rsp_o = TimeableLimitMap(max_key)

    def record_req(self, seq,
                   header):
        self.req_o.set(seq, header)

    def record_rsp(self, seq,
                   header):
        self.rsp_o.set(seq, header)

    def show_statistic(self):
        log.debug('data->req:%s, data->rsq:%s',
                  self.req_o.get_statistic(),
                  self.rsp_o.get_statistic())

    def in_req(self, seq):
        return self.req_o.has(seq)

    def clear(self):
        self.req_o.clear()
        self.rsp_o.clear()

    def _del_lost_rsp(self):
        req_keys = self.req_o.get_key_ls()
        rsp_keys = self.rsp_o.get_key_ls()
        del_keys = set(rsp_keys) - set(req_keys)
        self.rsp_o.del_keys(del_keys)

    def del_keys(self, keys):
        self.req_o.del_keys(keys)
        self.rsp_o.del_keys(keys)

        # 可能发生req已经被删了，但是rsp还没回复
        self._del_lost_rsp()


class SockData(Data):
    def __init__(self, max_key,
                 rsq_err_o):
        Data.__init__(self, max_key)
        self.rsp_miss_o = rsq_err_o

    def record_req(self, seq,
                   header):
        Data.record_req(self, seq, header)
        self.rsp_miss_o.set(seq)

    def record_rsp(self, seq,
                   header):
        Data.record_rsp(self, seq, header)
        self.rsp_miss_o.del_one_key(seq)


class PushData(Data):
    pass


###########################################
class Rpc(object):
    '''
    notes: 在worker得到的东西
    '''
    pass


class Bill(object):
    '''
    notes: 在proxy得到的东西
    '''
    def __init__(self, data_o,
                 seq_map,
                 fd_map,
                 err_o_ls,
                 mid):
        self.data_o = data_o
        self.seq_map = seq_map
        self.fd_map = fd_map
        self.err_o_ls = err_o_ls
        self.my_mid = mid

    def get_uid(self, seq):
        header = self.data_o.req_o.get_val(seq, None)
        if not header:
            return ''
        return header.str_uid

    def get_cmd(self, seq):
        header = self.data_o.req_o.get_val(seq, None)
        if not header:
            return ''
        return header.str_cmd

    def get_aid(self, seq):
        header = self.data_o.req_o.get_val(seq, None)
        if not header:
            return ''
        return header.str_aid

    def get_skey(self, seq):
        return self._get_conn_info_by_seq(seq, 'session_key', '')

    def get_tid(self, seq):
        header = self.data_o.req_o.get_val(seq, None)
        if not header:
            return ''
        return header.str_tid

    def get_client_addr(self, seq):
        addr = self._get_conn_info_by_seq(seq, 'addr', ('', 0))
        return '{}:{}'.format(addr[0], addr[1])

    def get_seq(self, seq):
        key = 'client_seqno'
        return self._get_info_from_seq(seq, key, 0)

    def _find_err_o(self, seq):
        for err in self.err_o_ls:
            if err.has(seq):
                return err

        return None

    def get_client_code(self, seq):
        '''
        # 如果找不到响应包，就返回0；否则则为调用service的返回码
        '''
        header = self.data_o.rsp_o.get_val(seq, None)
        if not header:
            return 0
        return header.i_code

    def get_code(self, seq):
        code = 0

        err_o = self._find_err_o(seq)
        if err_o:
            code = err_o.get_err_id()

        return code

    def get_errinfo(self, seq):
        errinfo = 'succ'

        err_o = self._find_err_o(seq)
        if err_o:
            errinfo = err_o.get_err_info()

        return errinfo

    def get_result(self, seq):
        return self.get_errinfo(seq)

    def get_cost(self, seq):
        code = self.get_code(seq)

        # 1.未知错误，不计算了
        if code == -1:
            return 0

        # 2.没有错误，返回响应包的时间
        if code == 0:
            end_o = self.data_o.rsp_o
        else:
            # 3.有错误，就返回错误的时间
            end_o = self._find_err_o(seq)

        end_time = end_o.get_time(seq, 0)
        start_time = self.data_o.req_o.get_time(seq, 0)

        cost = int((end_time - start_time) * 1000)
        return cost

    def get_duration(self, seq):
        return self.get_cost(seq)

    def get_caller_mid(self, seq):
        # 调用者->app
        return self.get_client_mid(seq)

    def get_callee_mid(self, seq):
        # 被调用者
        return self.my_mid

    def get_client_mid(self, seq):
        # 客户端->app
        header = self.data_o.req_o.get_val(seq, None)
        if not header:
            return 0
        return header.msg_client_info.ui_mid

    def _get_info_from_seq(self, seq, key, default):
        if not self.seq_map:
            return default

        if seq not in self.seq_map:
            return default

        if key not in self.seq_map[seq]:
            return default

        return self.seq_map[seq][key]

    def _get_conn_info_by_seq(self, seq, key, default):
        client_fd = self._get_info_from_seq(seq, "client_fd", None)
        if client_fd is None:
            log.error("client_fd is absetn for seq: %d", seq)
            return default

        conn_info = self.fd_map.get(client_fd)
        if not conn_info:
            log.error("conn info is absent for client fd: %d", client_fd)
            return default

        return conn_info.get(key, default)

    def get_req_len(self, seq):
        return self._get_info_from_seq(seq, 'req_len', 0)

    def _get_bt_str(self, buf):
        '''
        notes: 需要获取req的cls，然后才能解析，暂时先不搞了
        '''
        return ''

        try:
            return str(pb_helper.to_string(buf))
        except:
            log.error(traceback.format_exc())
            return buf

    def _get_proto_str(self, o):
        return ''

        if not o:
            return ''

        try:
            return str(pb_helper.to_short_string(o))
        except:
            log.error(traceback.format_exc())
            return ''

    def get_proto_req_header(self, seq):
        header = self.data_o.req_o.get_val(seq, None)
        return self._get_proto_str(header)

    def get_proto_rsp_header(self, seq):
        header = self.data_o.rsp_o.get_val(seq, None)
        return self._get_proto_str(header)

    def get_bt_req_header(self, seq):
        raw_header = self._get_info_from_seq(seq, 'req_header', '')
        return self._get_bt_str(raw_header)

    def get_bt_req_body(self, seq):
        raw_body = self._get_info_from_seq(seq, 'req_body', '')
        return self._get_bt_str(raw_body)

    def get_caller_ip(self, seq):
        # 调用者IP
        header = self.data_o.req_o.get_val(seq, None)
        if not header:
            return ''

        ui_ip = header.msg_client_info.msg_ip_info.ui_ip_v4
        try:
            str_ip = socket.inet_ntoa(struct.pack("!I", ui_ip))
        except:
            log.error(traceback.format_exc())
            str_ip = ''

        return str_ip

    def get_client_ip(self, seq):
        # 在access来说，caller_ip和client_ip是一样的
        return self.get_caller_ip(seq)

    def get_client_version(self, seq):
        header = self.data_o.req_o.get_val(seq, None)
        if not header:
            return ''

        try:
            big_ver = header.msg_client_info.msg_version.ui_major_version
            sma_ver = header.msg_client_info.msg_version.ui_minor_version
            str_ver = '{b}.{s}'.format(b=big_ver, s=sma_ver)
        except:
            log.error(traceback.format_exc())
            str_ver = ''

        return str_ver

    def get_rsp_len(self, seq):
        return self._get_info_from_seq(seq, 'rsp_len', 0)

    def get_bt_rsp_header(self, seq):
        raw_header = self._get_info_from_seq(seq, 'rsp_header', '')
        return self._get_bt_str(raw_header)

    def get_bill_info(self, seq):
        bill_info_map = \
            {
                'uid': self.get_uid(seq),
                'cmd': self.get_cmd(seq),
                'ret_code': self.get_client_code(seq),
                'result': self.get_result(seq),
                'cost': self.get_cost(seq),
                'duration': self.get_duration(seq),
                'mid': self.get_caller_mid(seq),
                'client_mid': self.get_client_mid(seq),
                'seq': self.get_seq(seq),
                'skey': self.get_skey(seq),
                'tid': self.get_tid(seq),
                'reqlen': self.get_req_len(seq),
                'rsplen': self.get_rsp_len(seq),
                'caller': self.get_caller_ip(seq),
                'ver': self.get_client_version(seq),
                'aid': self.get_aid(seq),
                'client_addr': self.get_client_addr(seq),
                'backend_seq': seq,
                'header': self.get_proto_req_header(seq),
                'body': self.get_bt_req_body(seq),
            }

        k_v_ls = ['{k}:{v}'.format(k=key, v=val)
                  for key, val in bill_info_map.iteritems()]
        str_bill_info = '|'.join(k_v_ls)

        return str_bill_info

    def get_bt_bill(self, seq):
        stats = MsgStats()
        stats.d_bill_time = time.time()
        stats.str_uid = self.get_uid(seq)
        stats.str_cmd = self.get_cmd(seq)
        stats.ui_seq = self.get_seq(seq)
        stats.i_code = self.get_code(seq)
        stats.str_info = self.get_errinfo(seq)
        stats.ui_cost = self.get_cost(seq)
        stats.ui_duration = self.get_duration(seq)
        stats.ui_req_len = self.get_req_len(seq)
        stats.ui_rsp_len = self.get_rsp_len(seq)
        stats.str_biz_info = self.get_bill_info(seq)
        stats.ui_caller_mid = self.get_caller_mid(seq)
        stats.str_caller_ip = self.get_caller_ip(seq)
        stats.ui_callee_mid = self.get_callee_mid(seq)
        stats.ui_client_mid = self.get_client_mid(seq)
        stats.str_client_version = self.get_client_version(seq)
        stats.str_client_ip = self.get_client_ip(seq)
        stats.bt_req_header = self.get_proto_req_header(seq)
        stats.bt_req_body = self.get_bt_req_body(seq)
        stats.bt_rsp_header = self.get_proto_rsp_header(seq)

        return stats

class LogicProxy(object):
    def __init__(self, id):
        self.logic_id = id

    @classmethod
    def send_to_monitor(cls, data):
        try:
            report_bill_cmd = "monitor.ReportBill"
            report_bill_req = ReportBillReq()
            stats = report_bill_req.msg_stats
            stats.CopyFrom(data)
            rpc_report_bill = PbRpcAgent(
                report_bill_cmd,
                None,
                body=report_bill_req
            )

            log.debug("rpc_report_bill invoking...")
            rpc_report_bill.invoke()
        except:
            log.error(traceback.format_exc())

    @classmethod
    def find_exclude_err_keys(cls, keys, err_o_ls):
        exclude_keys = list()

        for key in keys:
            for err in err_o_ls:
                if err.has(key):
                    exclude_keys.append(key)
                    break

        return exclude_keys

    @classmethod
    def find_req_rsp_pair_ls(cls, data_o):
        if not data_o:
            return []

        req_seq_ls = data_o.req_o.get_key_ls()
        rsp_seq_ls = data_o.rsp_o.get_key_ls()

        common_seq_ls = set(req_seq_ls) & set(rsp_seq_ls)
        return list(common_seq_ls)


class AppLogicProxy(LogicProxy):
    APP_ID = 8888

    def __init__(self):
        LogicProxy.__init__(self, self.APP_ID)

    def report(self, seq_ls, seq_map,
               fd_map, data_o, err_o_ls,
               mid):

        try:
            bill_o = Bill(data_o, seq_map, fd_map, err_o_ls, mid)

            for req_seq in seq_ls:
                log.debug('app_logic_proxy -> proxy_seq[%s] -> start!' % req_seq)

                if not data_o.in_req(req_seq):
                    # 不然可能会导致空req的header
                    log.warning('seq:%s is not in req, contiue!' % str(req_seq))
                    continue

                try:
                    # 1.上报
                    self.send_to_monitor(bill_o.get_bt_bill(req_seq))
                except:
                    log.error(traceback.format_exc())
                log.debug('app_logic_proxy -> send_monitor finish!')

                try:
                    # 2.记流水
                    bill.log(bill_o.get_bill_info(req_seq))
                except:
                    log.error(traceback.format_exc())
                log.debug('app_logic_proxy -> record_bill finish!')
        except:
            log.error(traceback.format_exc())


###########################################
class ProxyMonitor(object):
    '''
    notes:
    # 监控业务(不监控网络问题,先简单处理proxy,在handle_close_fd批量处理)：
    #     开始：收到完整的请求包，能够解析数据包；
    #
    #     1.异常：
    #        a.socket
    #            I) 管道发送给worker失败(no rpc);
    #            II) client_seq找不到;
    #            III) socket发送响应包给app失败;
    #        b.push
    #            I) session_key找不到；
    #            II) 发送给socket失败；
    '''
    TYPE_SOCKE = 1
    TYPE_PIPE = 2

    def __init__(self, mid):
        self.init_collections()
        self.my_mid = mid

    ###################################################
    def init_collections(self):
        self.err_push_miss_sessionkey_o = ErrPushSessionKeyMiss(1000)
        self.err_pipe_send_o = ErrPipeSend(1000)
        self.err_sock_send_o = ErrSockSend(1000)
        self.err_sock_miss_seq_o = ErrSockSeqMiss(1000)
        self.err_rsp_miss_seq_o = ErrRspMiss(1000)

        self.sock_data_o = SockData(1000, self.err_rsp_miss_seq_o)
        self.push_data_o = PushData(1000)

        self.data_o_ls = [self.sock_data_o,
                          self.push_data_o]
        self.err_o_ls = [self.err_push_miss_sessionkey_o,
                         self.err_pipe_send_o,
                         self.err_sock_miss_seq_o,
                         self.err_sock_send_o,
                         self.err_rsp_miss_seq_o]
        self.clearable_o_ls = self.data_o_ls + self.err_o_ls

    ###################################################
    def report(self, seq_ls,
               seq_map,
               fd_map):
        '''
        notes: 先做app的监控
        '''
        self.sock_data_o.show_statistic()

        report_o = AppLogicProxy()
        report_o.report(seq_ls,
                        seq_map,
                        fd_map,
                        self.sock_data_o,
                        self.err_o_ls,
                        self.my_mid)

        [o.del_keys(seq_ls) for o in self.clearable_o_ls]

    ###################################################
    def record_sock_req(self, seq,
                        header):
        self.sock_data_o.record_req(seq, header)

    def record_sock_rsp(self, seq,
                        header):
        self.sock_data_o.record_rsp(seq, header)

    def record_push_req(self, seq,
                        header):
        self.push_data_o.record_req(seq, header)

    def record_push_rsp(self, seq,
                        header):
        self.push_data_o.record_rsp(seq, header)

    ###################################################
    def push_session_key_miss(self, proxy_seq,
                              session_key):
        self.err_push_miss_sessionkey_o.set(proxy_seq, session_key)

    def sock_send_err(self, proxy_seq,
                      fd):
        self.err_sock_send_o.set(proxy_seq, fd)

    def pipe_send_err(self, proxy_seq,
                      fd):
        self.err_pipe_send_o.set(proxy_seq, fd)

    def sock_seq_miss(self, proxy_seq):
        self.err_sock_miss_seq_o.set(proxy_seq)
    ###################################################
