#!/usr/bin/expect -f
#filename: scp_expect.sh

# shellcheck disable=SC2121
# shellcheck disable=SC2154
set host [lindex $argv 0]

set password [lindex $argv 1]

# shellcheck disable=SC1083
set a [exec sh -c {find . -name '*.tgz'}]

spawn ssh root@$host

set timeout 1

# shellcheck disable=SC1083
expect {undefined "yes/no" {send "yes\r";exp_continue}}

set timeout 1

expect "password:"

send "$password\r"

expect "~]$"
# shellcheck disable=SC2006
send "cd /home/aograph\r"

set timeout 1

send "rm -rf feature-engineering\r"

set timeout 1

send "tar -zxvf $a\r"

set timeout 3

send "rm -rf $a\r"

set timeout 1

send "cd feature-engineering\r"

set timeout 1

send "chmod a+x service.sh\r"

set timeout 1

send "./service.sh restart\r"

set timeout 10

send "ls\r"

send "ps -ef | grep chara\r"

set timeout 10
send "exit\r"

expect eof