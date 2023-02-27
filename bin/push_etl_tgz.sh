#!/usr/bin/expect -f
#filename: scp_expect.sh

# shellcheck disable=SC2121
# shellcheck disable=SC2154
set host [lindex $argv 0]

set password [lindex $argv 1]

# shellcheck disable=SC1083
set a [exec sh -c {find . -name '*.tgz'}]
# shellcheck disable=SC2035
spawn scp -r "$a" root@$host:/home/aograph

set timeout 10

# shellcheck disable=SC1083
# shellcheck disable=SC1123
# shellcheck disable=SC1089
expect {
  "yes/no" {send "yes\r";exp_continue}
  "fingerprint" {send "yes\r";exp_continue}
  "password" {send "$password\r"}}

# 下面这种方式虽然也可以，但是不能自主分辨多个输入
# shellcheck disable=SC2154
#expect "password"
#send "$password\r"

##传输需要的时间

set timeout 300

send "exit\r"

expect eof