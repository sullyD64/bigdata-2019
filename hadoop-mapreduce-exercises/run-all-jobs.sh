#!/bin/bash

A="a-wordcount"
B="b-temperature"
C="c-bigram"
D="d-lastfm"
E="e-cdr"
F="f-topn"
G="g-mean"

bl="build/libs"
in="input"
out="output"

if [ -z "$HADOOP_HOME" ]
then
  echo "HADOOP_HOME is not set"
  exit 1
else
  EXER=$PWD
  echo "jars: " $EXER
  echo "hadoop: " $HADOOP_HOME
  cd $HADOOP_HOME
fi

./bin/hdfs dfs -mkdir /user
./bin/hdfs dfs -mkdir /user/$USER
./bin/hdfs dfs -mkdir /user/$USER/$in
./bin/hdfs dfs -mkdir /user/$USER/$out
./bin/hdfs dfs -put $EXER/$in /user/$USER
./bin/hdfs dfs -rmr /user/$USER/$out/*

./bin/hadoop jar $EXER/$A/build/libs/$A.jar wordcount/WordCount $in/words.txt $out/$A
./bin/hadoop jar $EXER/$A/build/libs/$A.jar wordcount/WordCount $in/words2.txt $out/$A_2
./bin/hadoop jar $EXER/$B/build/libs/$B.jar temperature/MaxTemperature $in/temperature.txt $out/$B
./bin/hadoop jar $EXER/$C/build/libs/$C.jar bigram/BigramCount -input $in/words.txt -output $out/$C
./bin/hadoop jar $EXER/$D/build/libs/$D.jar lastFM/UniqueListeners $in/LastFMlog.txt $out/$D
./bin/hadoop jar $EXER/$E/build/libs/$E.jar cdr/STDSubscribers $in/CDRlog.txt $out/$E
./bin/hadoop jar $EXER/$F/build/libs/$F.jar topn/TopN $in/pg201.txt $out/$F
./bin/hadoop jar $EXER/$G/build/libs/$G.jar mean/Mean $in/temperature.txt $out/$G
