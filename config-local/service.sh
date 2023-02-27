#!/bin/bash

project_name="characteristics"
profiles_active="local"
# shellcheck disable=SC2046
# shellcheck disable=SC2164
# shellcheck disable=SC2006
# shellcheck disable=SC2034
project_path=$(cd `dirname $0`; pwd)

# shellcheck disable=SC2121
project_jar="${project_name}-*.jar"

project_log="run_char.log"

#jvm_opts="-Xms512m -Xmx512m -Xss1204k"

usage() {
 echo "Usage: sh $0 [start|stop|restart|status]"
 exit 1
}

#检查程序是否在运行
is_exist(){
 # shellcheck disable=SC2006
 # shellcheck disable=SC2009
 pid=`ps -ef|grep ${project_jar}|grep -v grep|awk '{print $2}' `
 #如果不存在返回1，存在返回0
 if [[ -z "${pid}" ]]; then
    return 1
 else
    return 0
 fi
}

#启动
start(){
 is_exist
 # shellcheck disable=SC2181
 if [[ $? -eq "0" ]]; then
    echo "${project_name} is already running. Pid is ${pid}."
 else
    echo "${project_name} starting..."
    nohup java -jar ${project_jar} --spring.profiles.active=${profiles_active} > ${project_log} 2>&1 &
    sleep 1
    is_exist
    if [[ "$?" -eq 0 ]]; then
 		echo "${project_name} start success."
 	else
 		echo "${project_name} start failed, please see ${project_log} for details."
 	fi
 fi
}

#停止
stop(){
 is_exist
 # shellcheck disable=SC2181
 if [[ $? -eq "0" ]]; then
    kill -9 "${pid}"
    echo "${project_name} is stopped."
 else
    echo "${project_name} is not running."
 fi
}
  
#输出运行状态
status(){
 is_exist
 # shellcheck disable=SC2181
 if [[ $? -eq "0" ]]; then
    echo "${project_name} is running."
 else
    echo "${project_name} is not running."
 fi
}
  
#重启
restart(){
 stop
 while is_exist -eq "0"; do
     sleep 0.1
 done
  sleep 1
 start
}
  
#根据输入参数，选择执行对应方法，不输入则执行使用说明
case $1 in
 "start")
 start
 ;;
 "stop")
 stop
 ;;
 "status")
 status
 ;;
 "restart")
 restart
 ;;
 *)
 usage
 ;;
esac
