import re
import shutil

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

PROT = "file://"
INPUT = "/home/freebase/fbsmd2"
# INPUT = "/home/freebase/fb20m"
OUTPUT = INPUT + "-s3"

SKIP_PATTERNS = "\t<http:[^>]*>\t\."



def run_job(rdd):
    rdd = rdd.filter(lambda x: not re.findall(SKIP_PATTERNS, x))
    rdd.repartition(1).saveAsTextFile(PROT + OUTPUT)
    # for l in rdd.collect():
    #     print(l)


if __name__ == "__main__":
    spark = utils.create_session("FB_filtering")
    sc = spark.sparkContext
    
    fb_rdd = utils.load_data(sc, PROT + INPUT)
    run_job(fb_rdd)
