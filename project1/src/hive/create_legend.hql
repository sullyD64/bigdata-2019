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
-- LOCATION 'hdfs:////user/user33/hive/u33_legend'
-- LOCATION 'file:////home/lorenzo/git/bigdata-2019/project1/src/hive/u33_legend'
TBLPROPERTIES (
	'serialization.null.format' = '',
	'skip.header.line.count' = '1')
;

-- LOAD DATA LOCAL INPATH '/home/lorenzo/git/bigdata-2019/project1/testdata/historical_stocks.csv' 
LOAD DATA INPATH '/user/user33/dataset/historical_stocks.csv' 
OVERWRITE INTO TABLE `u33_legend`;

SELECT * FROM `u33_legend` LIMIT 10;