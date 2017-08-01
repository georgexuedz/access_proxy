## acces_proxy是什么?
可以通过http的方式发送命令控制一批客户端。

![image](https://github.com/georgexuedz/access_proxy/raw/master/image/access_proxy.jpg)

## 安装使用
* pip install tornado
* pip install ConcurrentLogHandler
* mkdir -p /data/logs/service/access/

## access_proxy实现细节介绍

* 一个tcp服务端
    *  作为一个代理，和设备保持tcp长连接，接收来自http服务器的请求，转发给设备。
    *  通过使用epoll模型，可以轻松处理上万个长连接。
* 一个http服务器
    *  提供http接口，发送命令给设备。
    *  使用tornado的web框架。
    *  使用管道发送命令发送给设备。

## access_proxy协议介绍
![image](https://github.com/georgexuedz/access_proxy/raw/master/image/access_proxy_protocol.jpg)

## 有问题反馈
在使用中有任何问题，欢迎反馈给我，可以用以下联系方式跟我交流

* 邮件(845094708@qq.com)
