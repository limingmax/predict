# -*- coding: UTF-8 -*-

#预测告警
from zutils.zrpc.zmq.redismq import RedisMQ
import traceback
import time
import datetime
import math
import numpy as np
from kafka import *
import json
import configparser
from zutils.logger import Logger

import os
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase_thrift.hbase import Hbase
from hbase_thrift.hbase.ttypes import *


cf = configparser.ConfigParser()
cf.read("python.ini")
kafka_host = cf.get("kafka", "host")
kafka_port = cf.getint("kafka", "port")
hbase_host = cf.get("hbase", "host")
hbase_port = cf.getint("hbase", "port")
register_address=cf.get("register","address")
producer_topic=cf.get("kafka","producer_topic")

def send( message):
    # kafka_host = '192.168.212.71'
    # kafka_port = '9092'
    producer = KafkaProducer(bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
        kafka_host=kafka_host,
        kafka_port=kafka_port
    )],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 调用send方法，发送名字为'ai_threshold_alert'的topicid ，发送的消息为message_string
    response = producer.send('ai_predictor_alert', message)
    producer.flush()
def time_handle( year, month, day):
    year = int(year)
    month = int(month)
    day = int(day)

    if (day > 1):
        day = day - 1

    else:
        if (month > 1):
            month = month - 1
            if (month in [1, 3, 5, 7, 8, 10, 12]):
                day = 31
            elif (month in [4, 6, 9, 11]):
                day = 30
            elif year % 400 == 0 or year % 4 == 0 and year % 100 != 0:
                day = 29
            else:
                day = 28
        else:
            year = year - 1
            month = 12
            day = 31
    if (month >= 10):
        month = str(month)
    else:
        month = '0' +str(month)
    if (day >= 10):
        day = str(day)
    else:
        day = '0' + str(day)
    year = str(year)
    return year, month, day


def select( metric, resource, year, month, day, namespace):
    print("===start===")
    # hbase_host = '192.168.195.1'
    # hbase_port = 9090
    os.system('kinit -kt /etc/hbase.keytab hbase')
    sock = TSocket.TSocket("hbase-master", hbase_port)
    transport = TTransport.TSaslClientTransport(sock, "hbase-master", "hbase")
    # Use the Binary protocol (must match your Thrift server's expected protocol)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    transport.open()
	
    print("===end===")
    table='Monitor_record'
    year, month, day = time_handle(year, month, day)
    year, month, day = time_handle(year, month, day)
    year, month, day = time_handle(year, month, day)
    pre=year+"-"+month+"-"+day
    result_list = []
    print("namespace", namespace)
    if (namespace):
        
	filter1 =  "RowFilter(=, 'substring:{pre}')AND SingleColumnValueFilter ('Metric', 'resourceName', =, 'binary:{resource}') AND SingleColumnValueFilter ('Metric', 'index_name', =, 'binary:{metric}') AND SingleColumnValueFilter ('Metric', 'time', =, 'regexstring:{year}-{month}-.*T.*:00:00') AND SingleColumnValueFilter ('Metric', 'type', =, 'binary:pod')AND SingleColumnValueFilter ('Metric', 'namespace_name', =, 'binary:{namespace}')".format(
                resource=resource, metric=metric, year=year, month=month, namespace=namespace,pre=pre)
	tscan=TScan(filterString=filter1)
	list1 = []
        sid=client.scannerOpenWithScan(table,tscan,{})
	result=client.scannerGet(sid)		
	while result:
			print result
			list1.append(float(result[0].columns.get("Metric:index_value").value))
			result=client.scannerGet(sid)
        
        
        
       

        result_list = list1
    else:
        
        filter1 ="RowFilter(=, 'substring:{pre}')AND SingleColumnValueFilter ('Metric', 'resourceName', =, 'binary:{resource}') AND SingleColumnValueFilter ('Metric', 'index_name', =, 'binary:{metric}') AND SingleColumnValueFilter ('Metric', 'time', =, 'regexstring:{year}-{month}-.*T.*:00:00')AND SingleColumnValueFilter ('Metric', 'type', =, 'binary:node') ".format(
                resource=resource, metric=metric, year=year, month=month)
	tscan=TScan(
				filterString=filter1
				)    
	list1 = []
        sid=client.scannerOpenWithScan(table,tscan,{})
	result=client.scannerGet(sid)			
        while result:
			print result
			list1.append(float(result[0].columns.get("Metric:index_value").value))
			result=client.scannerGet(sid)
        
        
		
        result_list = list1
    transport.close()
	# print(result_list)
    # if not result_list:
    #     raise Exception("历史数据不足")
    return result_list


def currentTime():
    timestamp = time.time()
    a = datetime.datetime.fromtimestamp(timestamp)
    b = str(a)
    c = b.split(" ")
    year, month, day = c[0].split("-")
    return year, month, day


def NDtest( value_list, t):
    # 正态性检验
    XX = np.array(value_list)
    m = XX.mean()
    s = XX.std()
    value_list = [x for x in value_list if x >= m - 3 * s and x <= m + 3 * s]
    if s==0:
        return m,m
    # X_scaled = preprocessing.scale(XX)
    # print(X_scaled.mean())
    # print(kstest(X_scaled,'norm'))
    # print(normaltest(X_scaled))
    p = np.array(value_list)
    # print(len(p))
    mu = np.mean(p)
    sigma = np.std(p)
    med = np.median(p)
    # print(mu)
    # print(sigma)
    # 偏度系数sk，衡量正太分布的偏态程度
    sk = (mu - med) / sigma
    # print(sk)
    # 记录是否取过对数
    flag = 0
    # 偏度系数太大，考虑偏态分布，对数正太分布
    if -1 > sk or sk > 1:
        p = np.log2(p)
        flag = 1

    mu = np.mean(p)
    sigma = np.std(p)
    # 3sigma准则？

    # 标准化,似乎不用了，阈值直接公式算一下
    # p=(p-mu)/sigma
    # mu = np.mean(p)
    # sigma = np.std(p)
    # print(p)
    # print(mu)
    # print(sigma)
    hign = 1.1503 * sigma * math.sqrt(t) + mu
    low = 0.4538 * sigma * math.sqrt(t) + mu
    if flag == 1:
        hign = pow(2, hign)
        low = pow(2, low)

    # 阈值   正负1.96*sigma+mu
    print("高阈值", hign)
    print("低阈值", low)
    return hign, low





task_name = '/predict'
# register_address = '192.168.213.51:6379'
rmq = RedisMQ(register_address, 10, 30)
rmq.set_queue_maxsize(task_name, 10, 0)
logger = Logger(Logger.INFO, 'predict', True)
while True:
    try:
        request = rmq.pop([task_name], protocol='JSON', is_throw_except=False)
        if request is None:
            # logger().info(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+'task is free')
            continue
        print(request.task,request.task_name)
        # logger().info(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+": "+request.task_name+" "+ request.task)



        # time.sleep(3)

        # print('jielai  ')
        record = request.task
        deviceName=0
        ruleId=0
        if "ruleId" not in record.keys():
            request.response_error("ruleId不能为空")
        else:
            ruleId=record["ruleId"]
        if "deviceName" not in record.keys():
            request.response_error("deviceName不能为空")

        else:
            deviceName=record["deviceName"]
        deviceNameSplit = deviceName.split("_")
        namespace = 0
        resourceName = 0
        if (len(deviceNameSplit) == 4):
            namespace = deviceNameSplit[-2]
            resourceName = deviceNameSplit[-1]
        elif(len(deviceNameSplit)==2):
            resourceName = deviceNameSplit[-1]
        else:
            request.response_error("deviceName长度错误")

        metric = 0
        if "metricName"not in record.keys():
            request.response_error("metricName不能为空")
        else:
            metric=record["metricName"]
        # 几小时以内是否会达到
        forecastTime = 0

        # print(forecastTime)
        # print(type(forecastTime))
        if  "forecastTimeRange"not in record.keys():
            request.response_error("forecastTimeRange不能为空")
        else:
            forecastTime = int(record["forecastTimeRange"][:-1])

        if forecastTime < 0:
            request.response_error("forecastTimeRange不能为负数")

        value = float(record["threshold"])
        request.response_succ('ok')



        year, month, day = currentTime()
        # year = "2018"
        # month = "10"
        # day = "25"
        logger().info(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+" :select:hbase_port "+str(hbase_host)+" hbase_port: "+str(hbase_port)+"metric:"+metric+" year:"+year+" month: "+month+" day: "+ day+" namespace: "+ str(namespace)+"resourceName:"+ resourceName)
        resultlist = select(metric, resourceName, year, month, day, namespace)
        if not resultlist:
            logger().info(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+"缺少历史数据")
        hign=0
        low=0
        if  resultlist:
            hign, low = NDtest(resultlist, forecastTime)
        logger().info(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+" : "+" hign: "+str(hign)+" low: "+str(low))
        result = 0
        if not resultlist:
            result="empty"
        elif value > hign:
            result = "low"
        elif value < low:
            result = "high"
        else:
            result = "medium"
        metric=metric.split("/")[0]
        returndict = {
            "deviceName": deviceName,
            "predictor": metric,
            "forecastTime": str(forecastTime)+"h",
            "result": result,
			"ruleId":ruleId
        }

        logger().info(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+":send:kafka_host:"+str(kafka_host)+" kafka_port: "+str(kafka_port)+"deviceName:"+returndict["deviceName"]+" predictor: "+returndict["deviceName"]+" forecastTime: "+returndict["forecastTime"]+" result: "+returndict["result"]+" ruleId: "+returndict["ruleId"])
        send(returndict)


    except:
        traceback.print_exc()
