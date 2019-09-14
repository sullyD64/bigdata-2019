import math

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext


def create_session(app_name):
    return SparkSession.builder.master('local[8]').appName(app_name).getOrCreate()


def load_data(sc, path):
    lines = sc.textFile(path)
    return lines
