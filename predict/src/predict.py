# 预测告警
from zutils.zrpc.zmq.redismq import RedisMQ
import traceback
import time
import datetime
import math
import happybase
import numpy as np
from kafka import *
import json
import configparser
cf = configparser.ConfigParser()
cf.read("python.ini")
kafka_host = cf.get("kafka", "host")
kafka_port = cf.getint("kafka", "port")
hbase_host = cf.get("hbase", "host")
hbase_port = cf.getint("hbase", "port")
register_address = cf.get("register", "address")
producer_topic = cf.get("kafka", "producer_topic")


def send(message):
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


def time_handle(year, month, day):
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
        month = '0' + month
    if (day >= 10):
        day = str(day)
    else:
        day = '0' + str(day)
    year = str(year)
    return year, month, day


def select(metric, resource, year, month, day, namespace):
    print("===start===")
    # hbase_host = '192.168.195.1'
    # hbase_port = 9090
    connection = happybase.Connection(hbase_host, hbase_port)
    print("===end===")
    t = connection.table('Monitor_record')

    result_list = []
    print("namespace", namespace)
    if (namespace):
        year, month, day = time_handle(year, month, day)
        year, month, day = time_handle(year, month, day)
        filter1 = bytes(
            "SingleColumnValueFilter ('Metric', 'resourceName', =, 'binary:{resource}') AND SingleColumnValueFilter ('Metric', 'index_name', =, 'binary:{metric}') AND SingleColumnValueFilter ('Metric', 'time', =, 'regexstring:{year}-{month}-.*T.*:00:00') AND SingleColumnValueFilter ('Metric', 'type', =, 'binary:pod')AND SingleColumnValueFilter ('Metric', 'namespace_name', =, 'binary:{namespace}')".format(
                resource=resource, metric=metric, year=year, month=month, namespace=namespace), encoding='utf-8')
        result = t.scan(filter=filter1, row_start=bytes('{year}-{month}-{day}'.format(year=year, month=month, day=day),
                                                        encoding='utf-8'))
        list1 = []
        for k, v in result:
            print(k, v)
            list1.append(float(v[b'Metric:index_value'].decode()))

        result_list = list1
    else:
        year, month, day = time_handle(year, month, day)
        year, month, day = time_handle(year, month, day)

        filter1 = bytes(
            "SingleColumnValueFilter ('Metric', 'resourceName', =, 'binary:{resource}') AND SingleColumnValueFilter ('Metric', 'index_name', =, 'binary:{metric}') AND SingleColumnValueFilter ('Metric', 'time', =, 'regexstring:{year}-{month}-.*T.*:00:00')AND SingleColumnValueFilter ('Metric', 'type', =, 'binary:node') ".format(
                resource=resource, metric=metric, year=year, month=month), encoding='utf-8')
        result = t.scan(filter=filter1,
                        row_start=bytes('{year}-{month}-{day}'.format(year=year, month=month, day=day),
                                        encoding='utf-8'))
        list1 = []
        for k, v in result:
            print(k, v)
            list1.append(float(v[b'Metric:index_value'].decode()))
            # last = k
        result_list = list1
    print(result_list)
    if not result_list:
        raise Exception("历史数据不足")
    return result_list


def currentTime():
    timestamp = time.time()
    a = datetime.datetime.fromtimestamp(timestamp)
    b = str(a)
    c = b.split(" ")
    year, month, day = c[0].split("-")
    return year, month, day


def NDtest(value_list, t):
    # 正态性检验
    XX = np.array(value_list)
    m = XX.mean()
    s = XX.std()
    value_list = [x for x in value_list if x >= m - 3 * s and x <= m + 3 * s]
    if s == 0:
        return m, m
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
while True:
    try:
        request = rmq.pop([task_name], protocol='JSON', is_throw_except=False)
        if request is None:
            print('task is free')
            continue
        print(request.task_name, request.task)
        request.response_succ('ok')

        # request.response_error('error')
        # time.sleep(3)

        print('jielai  ')
        record = request.task
        deviceName = record["deviceName"]
        ruleId = record["ruleId"]
        if not ruleId:
            raise Exception("ruleId不能为空")
        if not deviceName:
            raise Exception("deviceName不能为空")
        deviceNameSplit = deviceName.split("_")
        namespace = 0
        resourceName = 0
        if (len(deviceNameSplit) == 4):
            namespace = deviceNameSplit[-2]
            resourceName = deviceNameSplit[-1]
        else:
            resourceName = deviceNameSplit[-1]
        metric = record["metricName"]
        if not metric:
            raise Exception("metricName不能为空")
        # 几小时以内是否会达到
        forecastTime = record["forecastTimeRange"]
        forecastTime = int(record["forecastTimeRange"][:-1])
        print(forecastTime)
        print(type(forecastTime))
        if not forecastTime:
            raise Exception("forecastTimeRange不能为空")
        if forecastTime < 0:
            raise Exception("forecastTimeRange不能为负数")
        value = float(record["threshold"])

        year, month, day = currentTime()
        # year = "2018"
        # month = "10"
        # day = "25"
        print(metric, year, month, day, namespace, resourceName)
        resultlist = select(metric, resourceName, year, month, day, namespace)
        hign, low = NDtest(resultlist, forecastTime)
        result = 0
        if value > hign:
            result = "low"
        elif value < low:
            result = "high"
        else:
            result = "medium"
        metric = metric.split("/")[0]
        returndict = {
            "deviceName": deviceName,
            "predictor": metric,
            "forecastTime": str(forecastTime)+"h",
            "result": result,
            "ruleId": ruleId
        }
        print(returndict)
        send(returndict)

        # print(request.task_name, request.task)
    except:
        traceback.print_exc()
