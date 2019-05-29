#!/bin/sh
streaming_lib=/home/user33/hadoop-streaming.jar
srcpath=/home/user33/mapreduce/job3
hpath=/user/user33

local_output=$PWD/output

# input=$hpath/dataset/
input=$hpath/testset/

tmp1=$hpath/tmp1
tmp2=$hpath/tmp2
output=$hpath/out

rm -rf $local_output
hdfs dfs -rm -r $output
hdfs dfs -rm -r "$hpath/tmp*"

yarn jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -file $srcpath/firstMapper_join.py \
    -file $srcpath/firstReducer_join.py \
    -input $input \
    -output $tmp1 \
    -mapper "python firstMapper_join.py" \
    -reducer "python firstReducer_join.py"

yarn jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -file $srcpath/secondMapper_copy.py \
    -file $srcpath/secondReducer.py \
    -input $tmp1 \
    -output $tmp2 \
    -mapper "python secondMapper_copy.py" \
    -reducer "python secondReducer.py"

yarn jar $streaming_lib \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.reduce.tasks=1 \
    -file $srcpath/thirdMapper.py \
    -file $srcpath/thirdReducer.py \
    -input $tmp2 \
    -output $output \
    -mapper "python thirdMapper.py" \
    -reducer "python thirdReducer.py"

hdfs dfs -get /user/user33/out/ $local_output