DROP TABLE IF EXISTS `u33_legend`;

CREATE TABLE `u33_legend` (
  `ticker` STRING,
  `exchange` STRING,
  `name` STRING,
  `sector` STRING,
  `industry` STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS 
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
	'serialization.null.format' = '',
	'skip.header.line.count' = '1')
;

LOAD DATA INPATH '/user/user33/dataset/historical_stocks.csv' 
OVERWRITE INTO TABLE `u33_legend`;

SELECT * FROM `u33_legend` LIMIT 10;