CREATE EXTERNAL TABLE history (
  ticker STRING,
  opening_price FLOAT,
  closing_price FLOAT,
  adj_close FLOAT,
  lowest_price FLOAT,
  highest_price FLOAT,
  volume BIGINT,
  stocazzo DATE) 
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ',' STORED AS TEXTFILE
    LOCATION 'file:////home/lorenzo/git/bigdata-2019/project1/hive/history';

LOAD DATA LOCAL INPATH '/home/lorenzo/git/bigdata-2019/project1/dataset/historical_stock_prices.csv' OVERWRITE INTO TABLE history;