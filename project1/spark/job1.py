from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import csv

HISTORY_PATH = "hdfs://localhost:9000/user/bigdata/dataset_test_100000.csv"
LEGEND_PATH = "hdfs://localhost:9000/user/bigdata/historical_stocks.csv"
MIN_DATE = "1998-01-01"
MAX_DATE = "2018-12-31"

'''
calculate the following values among all rows with same TICKER between 1998 and 2018:
- %INCREASE = difference(avg(CLOSE in 2018), avg(CLOSE in 1998))
- MIN_PRICE = min(LOW)
- MAX_PRICE = max(HIGH)
- AVG_VOL = avg(VOLUME)

goal: show the top 10 stocks (identified by TICKER) by their %INCREASE.
output: (TICKER, (%INCREASE, MIN_PRICE, MAX_PRICE, AVG_VOL))

strategy:
- map by TICKER but filter out every row out of the date period (ticker, details)
- reduce in the following way:
    input comes sorted by TICKER, so all rows with same TICKER are consecutive.
    to calculate %INCREASE, fill up two lists records_first_year and records_last_year containing CLOSE prices,
      then calculate average values and subtract them
    initialize MIN_PRICE with LOW and MAX_PRICE with HIGH
    to calculate AVG_VOL, fill an empty list with VOLUMEs and calculate avg

'''

spark = SparkSession.builder.appName("job1").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

df = spark.read.format("csv") \
    .option("inferSchema", "true").option("header", "true") \
    .load(LEGEND_PATH)
df.show(truncate=False)

history = df.rdd

for line in history.collect()[:10]:
    print(line["ticker"])
