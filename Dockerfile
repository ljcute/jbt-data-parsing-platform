FROM registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/jbt-data-parsing-platform:base

ADD jbt-data-parsing-platform-master.tar.gz /data
WORKDIR /data/data/ms
CMD python3 basehandler.py
