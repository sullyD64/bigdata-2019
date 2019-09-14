from pyspark import SparkContext, SparkConf, Row
from pyspark.sql import SparkSession, SQLContext
import math


def create_session(app_name):
    return SparkSession.builder.master('yarn').appName(app_name).getOrCreate()
    # return SparkSession.builder.master('local[8]').appName(app_name).getOrCreate()


def load_data(sc, path):
    lines = sc.textFile(path)
    return lines

