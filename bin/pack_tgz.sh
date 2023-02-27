#!/bin/bash
old_path=$(pwd)

env_run="$1"
release_publish_tag="$2"

echo "${old_path}"

cd ../

rm -rf feature-engineering
# shellcheck disable=SC2035
rm -rf feature-engineering_*.tgz

mkdir feature-engineering

# shellcheck disable=SC2034
env_host=172.16.0.197
# shellcheck disable=SC2016
echo "东航测试环境 ${env_host}"

echo "${env_run}"

push_tgz(){
  env_pwd="$1"
  echo "${env_pwd}"
  # shellcheck disable=SC2078
  # shellcheck disable=SC2107
  if [[ "$env_run" = "T" && "$release_publish_tag" -eq 1 ]]; then
    if command -v expect; then
      echo "上传release压缩包到 ${env_host} ，请稍等"
      ./bin/push_etl_tgz.sh "${env_host}" "${env_pwd}"
      echo "正在解压并发布，请稍等"
      ./bin/unpack_and_start.sh "${env_host}" "${env_pwd}"
    else
      echo '不支持expect，请手动输入测试服务器密码'
      # shellcheck disable=SC2035
      scp *.tgz root@"${env_host}":/home/airline-etl
    fi
  fi
}

run_file="test"

# shellcheck disable=SC1009
# shellcheck disable=SC2170
if [ "$env_run" = "P" ]; then
  run_file="pro"
  cp -r -f target/characteristics-*.jar config-${run_file}/service.sh config-${run_file}/readme.txt src/main/resources/application-pro.properties feature-engineering/
elif [ "$env_run" = "D" ]; then
  run_file="dev"
  cp -r -f target/characteristics-*.jar config-${run_file}/service.sh feature-engineering/
else
  run_file="test"
  cp -r -f target/characteristics-*.jar config-${run_file}/service.sh feature-engineering/
fi

# shellcheck disable=SC2046
tar -zcvf feature-engineering_$(date +%Y%m%d_%H%M).tgz feature-engineering
push_tgz "Ka*st!Ancv0"

rm -rf feature-engineering

python bin/notify.py "${release_publish_tag}" "${env_run}"
# shellcheck disable=SC1073
# shellcheck disable=SC1072
# shellcheck disable=SC1035
# shellcheck disable=SC1020
# shellcheck disable=SC1009
# shellcheck disable=SC1034
# shellcheck disable=SC1033
if [ "$release_publish_tag" -eq 1 ];
then
  echo '删除打包文件'
  # shellcheck disable=SC2035
  rm -rf *.tgz
else
  desk=$HOME/DESKTOP
  # shellcheck disable=SC2006
  var=`eval echo $desk`
  echo "$var"
  # shellcheck disable=SC2035
  mv *.tgz "$var"
  echo '已将压缩文件放在桌面。'
fi
