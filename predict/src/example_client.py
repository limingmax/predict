# @Time   : 2018-10-24
# @Author : zxh


# 测试
import requests
#
#
# r = requests.get('http://192.168.213.51:8899/test1')
#
# print(r.json())
payload=["default_k8s-alpha-node1_default_kafka-2","default_k8s-alpha-node1_default_kafka-1","default_k8s-alpha-node1","default_k8s-alpha-node2","default_k8s-alpha-node1_0pk2uk_d123-7768b46578-vm78b"]
payload={"data":payload}
# payload={"data":["default_k8s-alpha-node1_default_kafka-2"]}
# r = requests.post('http://192.168.213.51:8899/delete', json=payload)
r = requests.post('http://192.168.213.51:8899/add', json=payload)

print(r.json())