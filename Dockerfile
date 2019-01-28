FROM registry.cn-hangzhou.aliyuncs.com/limingmax-test/ai-base:v3

ENV LANG C.UTF-8

ADD hbase.keytab /etc
ADD krb5.conf /etc

COPY predict /service/predict
ADD start.sh /service/predict/src
RUN chmod -R 777 /service/predict/src/start.sh

WORKDIR /service/predict/src

CMD ["/service/predict/src/start.sh"]

#防docker容器自动退出
ENTRYPOINT tail -f /dev/null
