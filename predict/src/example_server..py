# @Time   : 2018-10-23
# @Author : zxh
from zutils.zrpc.server.threadpool_server import ThreadpoolServer, map_handle
from zutils.logger import Logger
import time
import datetime
import math
import happybase
import numpy as np
import json
from kafka import *
import configparser
import threading
from myclass import MyClass
from myclass import data
#阈值告警
# 创建服务，单例，如果资源有竞争，创建放在func里
@map_handle('/add', 'handle1', 10)
@map_handle('/delete', 'handle2', 10)
@map_handle('/modify', 'handle3', 10)
class ExampleSvr():
        #新增某些处理节点
    def handle1(self, task):
        print(task)
        record=task["data"]
        # record=task
        for name in record:
            data[name]={}
        return data
        #删除某些容器节点
    def handle2(self,task):
        # record=task
        record=task["data"]
        for name in record:
            del data[name]
        return data
        #修改时间
    def handle3(self, task):
        record=task["time"]
        data={}
        # record=task["data"]
        data["time"]=record
        return data

print(12)
t=MyClass()
t.start()
print(11)
logger = Logger(Logger.INFO, 'example', True)
# 参数：日志级别   日志文件名   日志是否控制台输出


server = ThreadpoolServer('192.168.213.51:6379', [ExampleSvr], logger, 'JSON',30, 30,  1)
#          消息系统地址    服务列表     日志     交互协议    线程数量

server.start()




