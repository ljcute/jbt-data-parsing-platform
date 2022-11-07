FROM registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/jbt-data-parsing-platform:base1

ADD jbt-data-parsing-platform.tar.gz /data
RUN pip3 install -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com openpyxl mysql-connector==2.2.9
WORKDIR /data/data/ms
CMD python3 basehandler.py
