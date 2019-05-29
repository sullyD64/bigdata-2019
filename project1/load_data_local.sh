#!/bin/sh
root=hdfs://localhost:9000/
dataset=$root/user/user33/dataset
testset=$root/user/user33/testset

hdfs dfs -rm -r $root/user
hdfs dfs -mkdir $root/user
hdfs dfs -mkdir $root/user/user33
hdfs dfs -mkdir $dataset
hdfs dfs -mkdir $testset
hdfs dfs -put $PWD/dataset/historical_stocks.csv $dataset/
hdfs dfs -put $PWD/dataset/historical_stock_prices.csv $dataset/
hdfs dfs -put $PWD/dataset/historical_stocks.csv $testset/
hdfs dfs -put $PWD/testdata/dataset_test_100k_header.csv $testset/
hdfs dfs -put $PWD/dataset/history10M.csv $dataset/
hdfs dfs -put $PWD/dataset/history5M.csv $dataset/
