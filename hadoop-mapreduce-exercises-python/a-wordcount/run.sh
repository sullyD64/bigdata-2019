#!/bin/bash
HADOOP_VERSION=3.0.3
HADOOP_DIR=/home/lorenzo/Software/hadoop-$HADOOP_VERSION
INPUT=../input/words.txt
OUTPUT=output/

rm -rf output/

$HADOOP_DIR/bin/hadoop jar $HADOOP_DIR/share/hadoop/tools/lib/hadoop-streaming-$HADOOP_VERSION.jar \
  -file mapper.py -mapper "python mapper.py" \
  -file reducer.py -reducer "python reducer.py" \
  -input $INPUT \
  -output $OUTPUT
