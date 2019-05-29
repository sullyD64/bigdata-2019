#!/bin/sh
streaming_lib=/home/user33/hadoop-streaming.jar
srcpath=/home/user33/mapreduce/job1
hpath=/user/user33

local_output=$PWD/output

# input=$hpath/dataset/historical_stock_prices.csv
input=$hpath/testset/dataset_test_100k_header.csv

output=$hpath/out

rm -rf $local_output
hdfs dfs -rm -r $output

yarn jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -file $srcpath/mapper.py \
    -file $srcpath/reducer.py \
    -input $input \
    -output $output \
    -mapper "python mapper.py" \
    -reducer "python reducer.py"

hdfs dfs -get /user/user33/out/ $local_output