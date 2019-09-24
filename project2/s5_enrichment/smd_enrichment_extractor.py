import re
import shutil
import os
from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

PROT = "file://"
INPUT = "/path/to/input"
JOIN_INPUT = "/path/to/join_input"
# INPUT = "/home/freebase/fb20m"
OUTPUT = INPUT + "/path/to/output"

REG_PATTERN = ""


def map_by_subject(row):
	s, rest = row.split("\t" , 1)
	return (s, rest)


def run_job(rdd , to_join_rdd):

	if to_join_rdd.isEmpty():
		to_join_rdd = to_join_rdd.map(lambda x: x.split("\t")[0]).distinct()


    rdd = rdd.filter(lambda x: re.findall(REG_PATTERN, x))\
    		 .map(map_by_mid)\
    		 .join(to_join_rdd)

    rdd.repartition(1).saveAsTextFile(PROT + OUTPUT)
    # for l in rdd.collect():
    #     print(l)


if __name__ == "__main__":
    spark = utils.create_session("FB_filtering")
    sc = spark.sparkContext    
    fb_rdd = utils.load_data(sc, PROT + INPUT)

    if
    to_join_rdd = utils.load_data(sc, PROT + JOIN_INPUT)
    
    run_job(fb_rdd , to_join_rdd)
