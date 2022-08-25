#!/bin/bash
SERVICE_NAME="jbt-data-parsing-platform"
container_ids=$(docker ps | grep registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/${SERVICE_NAME} | awk '{print $1}')

if [ $(echo $container_ids | wc -c) -lt  12 ];then
    echo "没有正在运行的${SERVICE_NAME}容器"
else

    echo "$container_ids" | xargs -i docker stop {}
fi
