#!/bin/sh
if [ -z "$1" || -z "$2" ]
then
    echo "usage: load filename dataset"
    exit 1
fi
filename=$1
dataset=$2

hdfs dfs -rm -r "user/user33/hiveinput"
hdfs dfs -mkdir "user/user33/hiveinput"
hdfs dfs -cp "/user/user33/$dataset/*" /user/user33/hiveinput/

hive -f $filename > "outputs/$jname_output"
