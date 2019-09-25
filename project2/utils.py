from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext


def create_session(app_name):
    return SparkSession.builder \
        .master('local[8]').appName(app_name) \
        .config('spark.executor.memory','20G') \
        .config('spark.driver.memory','40G') \
        .config('spark.driver.maxResultSize', '30G') \
        .getOrCreate()


def load_data(sc, path):
    lines = sc.textFile(path)
    return lines
