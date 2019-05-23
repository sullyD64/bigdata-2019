CREATE EXTERNAL TABLE IF NOT EXISTS `legend` (
  `ticker` STRING,
  `exchange` STRING,
  `name` STRING,
  `sector` STRING,
  `industry` STRING) 
ROW FORMAT SERDE 
	'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS INPUTFORMAT 
	'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 
	'file:////home/lorenzo/git/bigdata-2019/project1/hive/legend'
TBLPROPERTIES (
	'serialization.null.format' = '',
	'skip.header.line.count' = '1');

LOAD DATA LOCAL INPATH '/home/lorenzo/git/bigdata-2019/project1/dataset/historical_stocks.csv' 
OVERWRITE INTO TABLE `legend`;
