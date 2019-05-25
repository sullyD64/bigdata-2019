from pyspark.sql import SparkSession, SQLContext
import csv

LEGEND_PATH = "hdfs://localhost:9000/user/bigdata/historical_stocks.csv"

# parsing lines with csv instead of using df
spark = SparkSession.builder.appName("job1").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


def parse_line(line):
    csv_reader = csv.reader([line], delimiter=',')
    return next(csv_reader)

legend = sc.textFile(LEGEND_PATH)
header = sc.parallelize([legend.first()])
legend = legend.subtract(header).map(lambda line: parse_line(line))

for line in legend.collect()[:10]:
    print(line)
