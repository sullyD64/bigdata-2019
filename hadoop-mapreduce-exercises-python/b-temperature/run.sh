#!/bin/bash
HADOOP_VERSION=3.0.3
HADOOP_DIR=/home/lorenzo/Software/hadoop-${HADOOP_VERSION}
INPUT=../input/temperature.txt
OUTPUT=output/

rm -rf output/

${HADOOP_DIR}/bin/hadoop jar ${HADOOP_DIR}/share/hadoop/tools/lib/hadoop-streaming-${HADOOP_VERSION}.jar \
  -files mapper.py,reducer.py \
  -mapper "python mapper.py" \
  -reducer "python reducer.py" \
  -input ${INPUT} \
  -output ${OUTPUT}
