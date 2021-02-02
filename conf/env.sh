#!/usr/bin/env bash

# symlink and absolute path should rely on SS_HOME to resolve
if [ -z "${SS_HOME}" ]; then
  export SS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
export SS_CONF_DIR="${SS_CONF_DIR:-"${SS_HOME}/conf"}"

export LD_PRELOAD=/usr/lib64/libjemalloc.so
export SS_GC_OPT=" -Xmx1000m -server -XX:+UseG1GC -XX:MetaspaceSize=128m -XX:MaxGCPauseMillis=500 -XX:ParallelGCThreads=24 -XX:ConcGCThreads=6 -XX:+AggressiveOpts -XX:+DisableExplicitGC -XX:+ParallelRefProcEnabled -XX:-ResizePLAB -XX:+UseStringDeduplication -XX:+PrintAdaptiveSizePolicy -XX:InitiatingHeapOccupancyPercent=75 -XX:+UnlockExperimentalVMOptions -XX:G1HeapWastePercent=5 -XX:G1MixedGCLiveThresholdPercent=85 -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1024M"
export SS_OPTS=$SS_GC_OPT" -Xloggc:/home/var/log/yarn/spark-shuffle-gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/var/lib/yarn/ -Dio.netty.maxDirectMemory=3242700310"

# some Java parameters
# export JAVA_HOME=/home/y/libexec/jdk1.6.0/
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java$SS_OPTS

#SS_LOG_DIR=/home/var/log/yarn/ss/log/
#SS_PID_DIR=/home/var/log/yarn/ss/pid/