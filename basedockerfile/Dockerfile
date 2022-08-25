FROM registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/python:3.9

RUN pip3 install -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com \
    requests==2.28.1 \
    pandas==1.4.3 \
    PyMySQL==1.0.2 \
    SQLAlchemy==1.4.39 && \
    python3 -m pip install kafka-python
