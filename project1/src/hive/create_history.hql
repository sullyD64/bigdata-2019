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
TBLPROPERTIES (
	'serialization.null.format' = '',
	'skip.header.line.count' = '1')
;

LOAD DATA INPATH '/user/user33/dataset/historical_stock_prices.csv' 
--LOAD DATA INPATH '/user/user33/testset/dataset_test_100k_header.csv' 
OVERWRITE INTO TABLE `u33_history`;

SELECT * FROM `u33_history` LIMIT 10;