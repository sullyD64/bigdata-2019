#!/bin/sh
streaming_lib=$HOME/workspace/hadoop-spark-hive/tools/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar
hpath=hdfs://localhost:9000/user/user33

local_output=../../../outputs/mr_job1

# input=$hpath/dataset/historical_stock_prices.csv
input=$hpath/testset/dataset_test_100k_header.csv

output=$hpath/out

rm -rf $local_output
hdfs dfs -rm -r $output

hadoop jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -input $input \
    -output $output \
    -mapper "python mapper.py" \
    -reducer "python reducer.py"

hdfs dfs -get /user/user33/out/ $local_output