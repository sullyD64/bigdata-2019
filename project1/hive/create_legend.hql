CREATE EXTERNAL TABLE legend (
  ticker STRING,
  exc STRING,
  name STRING,
  sector STRING,
  industry STRING ) 
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ',' STORED AS TEXTFILE
    LOCATION 'file:////home/lorenzo/git/bigdata-2019/project1/hive/legend';

LOAD DATA LOCAL INPATH '/home/lorenzo/git/bigdata-2019/project1/dataset/historical_stocks.csv' OVERWRITE INTO TABLE legend;
