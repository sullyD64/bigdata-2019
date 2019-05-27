#!/bin/sh
HSLIB=tools/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar
HDFS=hdfs://localhost:9000/user/bigdata/
input=$HDFS/historical_stock_prices.csv
output=$HDFS/out

hdfs dfs -rm $output
hadoop jar $HSLIB \
    -mapper $PWD/mapper.py \
    -reducer $PWD/reducer.py  \
    -input $input \
    -output $output \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=8