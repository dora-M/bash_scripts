#!/bin/bash

login_user="myuser"
password=`cat /usr/local/pbx/etc/pass.file | openssl enc -aes-128 -pass pass:sslpass -d -a -salt -pbkdf2`
PTH=/usr/local/pbx/incidents
PTH_psql=/usr/local/pbx/sbin/psql
psql_file=insert_incidents.sql
_now=$(date +"%Y_%m_%d_%H")
mkdir "$PTH/inc_$_now"
nodes=`cat $PTH/node.lis`
log_file_failed="$PTH/inc_$_now/inc.failed"
PTH_lcd="$PTH/inc_$_now"
PTH_cd="/usr4/incid"
command_string_lcd="lcd $PTH_lcd"
command_string_cd="cd $PTH_cd"

ex() {

host="172.30.3.$node"

expect -c "
match_max 100000
set timeout 800
spawn ftp $host
expect {
  \"Network is unreachable\"	{ log_file $log_file_failed; send_user \"\n$node Network is unreachable\n\" ; exit 1 }
  \"No route to host\"		{ log_file $log_file_failed; send_user \"\n$node No route to host\n\"          	; exit 1 }
  timeout 			{ log_file $log_file_failed; send_user \"\n$node Failed to get login prompt-timeout\n\"	; exit 1 }
  eof     			{ log_file $log_file_failed; send_user \"\n$node Failed to get login prompt-eof\n\"	; exit 1 }
  \"Name\"
}
send \"$login_user\r\"
expect {
  timeout 			{ log_file $log_file_failed; send_user \"\n$node Failed to get password prompt-timeout\n\"	; exit 1 }
  eof     			{ log_file $log_file_failed; send_user \"\n$node Failed to get password prompt-eof\n\"		; exit 1 }
  \"Password:\"
}
send \"$password\r\"
expect {
  \"Login incorrect\"		{ log_file $log_file_failed; send_user \"\n$node Login incorrect\n\"   		; exit 1 }
  timeout 			{ log_file $log_file_failed; send_user \"\n$node Failed connection-timeout\n\"	; exit 1 }
  eof     			{ log_file $log_file_failed; send_user \"\n$node Failed connection-eof\n\"	; exit 1 }
  \"User $login_user logged in.\"
}
send \"$command_string_lcd\r\"
expect {
  \"No such file or directory\"	{ log_file $log_file_failed; send_user \"\n$node No such local file or directory: $PTH_lcd\n\"; exit 1 }
  timeout 			{ log_file $log_file_failed; send_user \"\n$node Failed connection-timeout\n\"		  ; exit 1 }
  eof     			{ log_file $log_file_failed; send_user \"\n$node Failed connection-eof\n\"		  ; exit 1 }
  \"Local directory now\"
}
send \"$command_string_cd\r\"
expect {
  \"No such file or directory\"	{ log_file $log_file_failed; send_user \"\n$node No such file or directory: $PTH_cd\n\"	; exit 1 }
  timeout 			{ log_file $log_file_failed; send_user \"\n$node Failed connection-timeout\n\"		; exit 1 }
  eof     			{ log_file $log_file_failed; send_user \"\n$node Failed connection-eof\n\"		; exit 1 }
  \"CWD command successful.\"
}
send \"$command_string\r\"
expect {
  timeout 			{ log_file $log_file_failed; send_user \"\n$node Failed connection-timeout\n\"		; exit 1 }
  eof     			{ log_file $log_file_failed; send_user \"\n$node Failed connection-eof\n\"		; exit 1 }
  \"Transfer complete.\"
}
send \"quit\r\"
expect eof
"

}

function_nodes() {
  for node in $nodes
  do
    command_string="get inc.txt inc-$node.txt"
    ex $command_string
  done
}
		 
function_nodes

mkdir "$PTH_lcd/inc"

ls $PTH_lcd | grep inc-| while read file_list
do
  file_to_import="$PTH_lcd/$file_list"
  firstline=$(head -n1 $file_to_import|awk 'BEGIN{FIELDWIDTHS="2 1 2 1 2 1 2 1 2 1 2"}{print $5 $3 $1 $7 $9 $11}')
  echo ${file_to_import:47:2}$firstline >> $PTH_lcd/inc/inc.list
done

file_diff_old="$PTH/inc.list"
file_diff_new="$PTH_lcd/inc/inc.list"
row_old=( $(diff $file_diff_old $file_diff_new  | grep "<" | sed 's/< //g') )
row_new=( $(diff $file_diff_old $file_diff_new  | grep ">" | sed 's/> //g') )
node_restart=( $(diff $file_diff_old $file_diff_new  | grep ">" | cut -c3-4) )

for i in "${!row_new[@]}"
do
  sed -i "s/^${node_restart[$i]}.*/${row_new[$i]}/" $file_diff_old
done

function_nodes_1() {
  for node in "${node_restart[@]}" 
  do
    command_string="get inc-1.txt inc-$node-1.txt"
    ex $command_string
  done 
}
function_nodes_2() {
  for node in "${node_restart[@]}" 
  do
    command_string="get inc-2.txt inc-$node-2.txt"
    ex $command_string
  done 
}
function_nodes_3() {
  for node in "${node_restart[@]}"
  do
    command_string="get inc-3.txt inc-$node-3.txt"
    ex $command_string
  done 
}

function_nodes_1
function_nodes_2
function_nodes_3

ls $PTH_lcd | grep inc-| while read file_list
do
  file_to_import="$PTH_lcd/$file_list"
  cut -c 2-9,11-27,29-31,32-34,35,37-47,50,52- --output-delimiter '$' $file_to_import | cut -d "=" -f 1,2- | sed 's/=/$/' > $PTH_lcd/inc/$file_list
done

sed -i '1d' $PTH_lcd/inc/inc-*
cat $PTH_lcd/inc/inc-* > $PTH_lcd/inc/inc.csv
psql -d incidents -c "DELETE FROM incidents_import;" 
psql -d incidents -c "\COPY incidents_import FROM '$PTH_lcd/inc/inc.csv' DELIMITER '$';"
psql -d incidents -f "$PTH_psql/$psql_file"
