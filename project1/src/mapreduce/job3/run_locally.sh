#!/bin/sh
streaming_lib=$HOME/workspace/hadoop-spark-hive/tools/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar
hpath=hdfs://localhost:9000/user/user33

local_output=../../../outputs/mr_job3

input=$hpath/testset/
# input=$hpath/dataset/

tmp1=$hpath/tmp1
tmp2=$hpath/tmp2
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
    -output $tmp2 \
    -mapper "python secondMapper_copy.py" \
    -reducer "python secondReducer.py"

hadoop jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -input $tmp2 \
    -output $output \
    -mapper "python thirdMapper.py" \
    -reducer "python thirdReducer.py"

hdfs dfs -get /user/user33/out/ $local_output