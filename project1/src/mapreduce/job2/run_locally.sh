#!/bin/sh
streaming_lib=$HOME/workspace/hadoop-spark-hive/tools/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar
hpath=hdfs://localhost:9000/user/user33

local_output=../../../outputs/mr_job2

input=$hpath/testset/
# input=$hpath/dataset/

tmp1=$hpath/tmp1
output=$hpath/out

rm -rf $local_output
hdfs dfs -rm -r $output
hdfs dfs -rm -r "$hpath/tmp*"

hadoop jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -input $input \
    -output $tmp1 \
    -mapper "python firstMapper_join.py" \
    -reducer "python firstReducer_join.py"

hadoop jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -input $tmp1 \
    -output $output \
    -mapper "python secondMapper_copy.py" \
    -reducer "python secondReducer.py"

hdfs dfs -get /user/user33/out/ $local_output