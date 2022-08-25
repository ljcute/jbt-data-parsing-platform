FROM registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/jbt-data-parsing-platform:base1

ADD jbt-data-parsing-platform.tar.gz /data
WORKDIR /data/data/ms
CMD python3 basehandler.py
