#!/bin/bash

for i in "$@"
do
case ${i} in
  -redisproxy.port=*)
  REDISPROXY_PORT="${i#*=}"
  shift
  ;;
  -redisproxy.logpath=*)
  REDISPROXY_LOGPATH="${i#*=}"
  shift
  ;;
  -redisproxy.config=*)
  REDISPROXY_CONFIG="${i#*=}"
  shift
  ;;
  -env=*)
  ENV="${i#*=}"
  shift
  ;;
  -gc.set=*)
  GC_SET="${i#*=}"
  shift
  ;;
  -heap.set=*)
  HEAP_SET="${i#*=}"
  shift
  ;;
  *)
  ;;
esac
done

if [ -z ${REDISPROXY_PORT} ] || [ -z ${REDISPROXY_CONFIG} ]; then
  echo "Usage:"
  echo "  -redisproxy.port=redis-proxy绑定的端口"
  echo "  -redisproxy.logpath=redis-proxy日志目录路径"
  echo "  -redisproxy.config=redis-proxy配置文件"
  echo "  -env=local/test/production"
  exit 0
fi
if [ -z ${REDISPROXY_LOGPATH} ]; then
  REDISPROXY_LOGPATH=`pwd`/logs
fi
echo "redisproxy.port = ${REDISPROXY_PORT}"
echo "redisproxy.logpath = ${REDISPROXY_LOGPATH}"
echo "redisproxy.config = ${REDISPROXY_CONFIG}"

if [ ! -d ${REDISPROXY_LOGPATH}/${REDISPROXY_PORT} ]; then
  mkdir -p ${REDISPROXY_LOGPATH}/${REDISPROXY_PORT}
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SYSTEM_PROPS="-DWORKDIR=${REDISPROXY_LOGPATH}/${REDISPROXY_PORT} \
 -DREDISPROXY_PORT=${REDISPROXY_PORT} -DREDISPROXY_LOGPATH=${REDISPROXY_LOGPATH} \
 -Dlog4j.configuration=file:${DIR}/config/log4j.properties \
 -Dredisproxy.internal.bytebuf.preferDirect=yes"

echo "SYSTEM_PROPS=${SYSTEM_PROPS}"

if [[ ! -d ${REDISPROXY_LOGPATH}/${REDISPROXY_PORT}/gclog ]]; then
  mkdir ${REDISPROXY_LOGPATH}/${REDISPROXY_PORT}/gclog
fi

if [ -z ${GC_LOG_LEVEL} ]; then
  GC_LOG_LEVEL=info
fi
TIME=`date +"%Y%m%d%H%M%S"`
GC_LOG_FILE=${REDISPROXY_LOGPATH}/${REDISPROXY_PORT}/gclog/gc-${TIME}.log
JVM_PROPS="-Xlog:gc*=${GC_LOG_LEVEL},refine*=info:file=${GC_LOG_FILE}:time,tags,tid -XX:+PrintExtendedThreadInfo"

if [ -z ${HEAP_SET} ]; then
  HEAP_SET="HEAP_SET_MEDIUM"
fi
echo "HEAP_SET=${HEAP_SET}"
case ${HEAP_SET} in
  "HEAP_SET_SMALL")
  JVM_PROPS="-Xms2g -Xmx2g ${JVM_PROPS}"
  ;;
  "HEAP_SET_MEDIUM")
  JVM_PROPS="-Xms4g -Xmx4g ${JVM_PROPS}"
  ;;
  "HEAP_SET_LARGE")
  JVM_PROPS="-Xms8g -Xmx8g ${JVM_PROPS}"
  ;;
esac

if [ -z ${GC_SET} ]; then
  GC_SET="G1"
fi
echo "GC_SET=${GC_SET}"
case ${GC_SET} in
  "GC_SET_PARALLEL")
  JVM_PROPS="${JVM_PROPS} -XX:+UseParallelOldGC"
  ;;
  "G1")
  JVM_PROPS="${JVM_PROPS} -XX:+UseG1GC -XX:MaxGCPauseMillis=30 -XX:+AlwaysPreTouch"
  ;;
  "ZGC")
  JVM_PROPS="${JVM_PROPS} -XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
  ;;
esac

JVM_PROPS="${JVM_PROPS} \
  -XX:+UseCondCardMark \
  -XX:+ShowCodeDetailsInExceptionMessages"
echo "JVM_PROPS=${JVM_PROPS}"

APP_ARGS="-c ${REDISPROXY_CONFIG}"

if [ -z ${ENV} ]; then
  ENV="production"
fi
echo "env=${ENV}"
case ${ENV} in
  "local")
  JVM_PROPS="-ea -XX:+PrintCompilation \
   -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime ${JVM_PROPS}"
  APP_ARGS="${APP_ARGS} -local"
  ;;
  "test")
  JVM_PROPS="-ea -XX:+PrintCompilation \
   -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime  ${JVM_PROPS}"
  ;;
  "profiler")
  JVM_PROPS="-agentpath:${DIR}/profiler/liblagent-centos7.so=interval=7,logPath=/tmp/oneproxy-profiler.hpl \
    ${JVM_PROPS}"
  ;;
esac

echo "JVM_PROPS=${JVM_PROPS}"
echo "APP_ARGS=${APP_ARGS}"

JAVA=java
if [ ! -z ${JAVA_HOME} ]; then
  echo "using JAVA_HOME=${JAVA_HOME}"
  JAVA=${JAVA_HOME}/bin/java
fi
${JAVA} -version

exec ${JAVA} ${JVM_PROPS} ${SYSTEM_PROPS} -cp ${DIR}/config/*:${DIR}/lib/* \
  org.jrp.Bootstrap ${APP_ARGS} "$@"
