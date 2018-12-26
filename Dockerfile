FROM registry.cn-hangzhou.aliyuncs.com/limingmax-test/ai-base:v1

ENV LANG C.UTF-8

ENV REDIS_IP REDIS_IP
ENV REDIS_PORT REDIS_PORT

ENV KAFKA_IP KAFKA_IP
ENV KAFKA_PORT KAFKA_PORT

ENV HBASE_IP HBASE_IP
ENV HBASE_PORT HBASE_PORT

ENV PRODUCER_TOPIC PRODUCER_TOPIC

COPY predict /service/predict
WORKDIR /service/predict/src

ADD start.sh /service/predict/src
RUN chmod -R 777 /service/predict/src/start.sh

WORKDIR /service/predict/src

CMD ["/service/predict/src/start.sh"]

#防docker容器自动退出
ENTRYPOINT tail -f /dev/null
