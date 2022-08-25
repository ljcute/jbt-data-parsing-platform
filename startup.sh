#!/bin/bash
BASEDIR=$(dirname $(readlink -f $0) )
APP_NAME=jbt-data-parsing-platform
#宿主机日志目录
LOG_DIR=/data/logs/${APP_NAME}
#配置文件
CONFIG_DIR=${BASEDIR}/config

[ -e ${LOG_DIR} ] || mkdir -p ${LOG_DIR}

sh ${BASEDIR}/stop.sh
cd ${BASEDIR}


#docker pull registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/${APP_NAME}:latest
# 容器去掉两参数
docker run -d \
  -e LANG=en_US.UTF-8 \
  -v ${CONFIG_DIR}:/data/config \
  -v ${LOG_DIR}:/data/data/ms/logs \
  -v /etc/localtime:/etc/localtime:ro \
  -v /etc/timezone:/etc/timezone:ro \
  -w /data/data/ms/ \
  registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/${APP_NAME}:latest \
  python3 basehandler.py
echo "完成"
