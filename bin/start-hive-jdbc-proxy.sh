#!/bin/bash

cd `dirname $0`
cd ..
export PROXY_HOME=`pwd`

export PROXY_PID=$PROXY_HOME/bin/hive-jdbc-proxy.pid

if [[ -f "${PROXY_PID}" ]]; then
    pid=$(cat ${PROXY_PID})
    if kill -0 ${pid} >/dev/null 2>&1; then
      echo "Hive JDBC Proxy is already running."
      return 0;
    fi
fi

export PROXY_LOG_PATH=$PROXY_HOME/logs
export PROXY_HEAP_SIZE="1G"
export PROXY_JAVA_OPTS="-Xms$PROXY_HEAP_SIZE -Xmx$PROXY_HEAP_SIZE -XX:+UseG1GC -XX:MaxPermSize=500m"

nohup java $PROXY_JAVA_OPTS -cp $PROXY_HOME/conf:$PROXY_HOME/lib/* com.enjoyyin.hive.proxy.jdbc.server.JDBCProxyServer 2>&1 > $PROXY_LOG_PATH/proxy.out &
pid=$!
if [[ -z "${pid}" ]]; then
    echo "Hive JDBC Proxy start failed!"
    exit 1
else
    echo "Hive JDBC Proxy start succeeded!"
    echo $pid > $PROXY_PID
    sleep 1
fi
