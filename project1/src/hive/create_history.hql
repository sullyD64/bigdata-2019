DROP TABLE IF EXISTS `u33_history`;

CREATE TABLE `u33_history` (
  `ticker` STRING,
  `price_open` DOUBLE,
  `price_close` DOUBLE,
  `adj_close` DOUBLE,
  `price_low` DOUBLE,
  `price_high` DOUBLE,
  `volume` BIGINT,
  `date_created` DATE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
-- LOCATION 'hdfs:////user/user33/hive/u33_history'
-- LOCATION 'file:////home/lorenzo/git/bigdata-2019/project1/src/hive/u33_history'
TBLPROPERTIES (
	'serialization.null.format' = '',
	'skip.header.line.count' = '1')
;

-- LOAD DATA LOCAL INPATH '/home/lorenzo/git/bigdata-2019/project1/testdata/historical_stock_prices.csv' 
LOAD DATA INPATH '/user/user33/dataset/historical_stock_prices.csv' 
OVERWRITE INTO TABLE `u33_history`;

SELECT * FROM `u33_history` LIMIT 10;