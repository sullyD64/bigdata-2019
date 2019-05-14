/*
 generare in ordine le 10 azioni la cui quotazione (prezzo di chiusura) è cresciuta maggiormente dal 1998 al 2018, indicando per ogni azione: 
 (ticker, growth%, min_global, max_global, avg_daily_volume)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
*/

CREATE EXTERNAL TABLE stocks_legend (
  ticker STRING,
  exc STRING,
  name STRING,
  sector STRING,
  industry STRING ) 
    LOCATION 'file://home/lorenzo/git/bigdata2019/project1/hive/';


CREATE EXTERNAL TABLE stocks_history (
  ticker STRING,
  opening_price FLOAT,
  closing_price FLOAT,
  adj_close FLOAT,
  lowest_price FLOAT,
  highest_price FLOAT,
  volume BIGINT,
  date DATE) 
    LOCATION 'file://home/lorenzo/git/bigdata2019/project1/hive/';

/* sostituire LOCAL con INPATH per pescare dall'HDFS*/
LOAD DATA LOCAL INPATH '//home/lorenzo/git/bigdata2019/project1/dataset/historical_stocks.csv' OVERWRITE INTO TABLE stocks_legend;

LOAD DATA LOCAL INPATH '//home/lorenzo/git/bigdata2019/project1/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE stocks_history;

