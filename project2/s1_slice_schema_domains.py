import re
import shutil
import os

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

PROT = "file://"
HOME = "/home/freebase/"

INPUT = HOME + "freebase-s0"
OUTPUT = INPUT[:-2] + "s1"

KEY = "-key"
COMMON = "-common"
TYPE = "-type"
FREEBASE = "-freebase"
KG = "-kg"
SMD = "-smd"

DOMAIN_PATTERNS = r"\t<(k:|f:(common|type|freebase|kg))[^>]*>\t(?!\.)"
PATTERN_KEY = r"\t<k:[^>]*>\t(?!\.)"
PATTERN_COMMON = r"\t<f:common[^>]*>\t(?!\.)"
PATTERN_TYPE = r"\t<f:type[^>]*>\t(?!\.)"
PATTERN_FREEBASE = r"\t<f:freebase[^>]*>\t(?!\.)"
PATTERN_KG = r"\t<f:kg[^>]*>\t(?!\.)"


def run_job(rdd):

    # Filter the RDD from all non-domain triples.
    # The resulting RDD contains triples from the subject-matter domains (SMD)
    smd_rdd = rdd.filter(lambda x: not re.findall(DOMAIN_PATTERNS, x))

    # Do the opposite: filter all SMD triples
    schema_rdd = rdd.filter(lambda x: re.findall(DOMAIN_PATTERNS, x))

    # Extract single-domain RDDs from the schema domains RDD
    key_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_KEY, x))
    common_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_COMMON, x))
    type_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_TYPE, x))
    freebase_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_FREEBASE, x))
    kg_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_KG, x))

    # Save RDDs
    smd_rdd.repartition(1) \
        .saveAsTextFile(OUTPUT + KEY)
    key_rdd.repartition(1) \
        .saveAsTextFile(OUTPUT + COMMON)
    common_rdd.repartition(1) \
        .saveAsTextFile(OUTPUT + TYPE)
    type_rdd.repartition(1) \
        .saveAsTextFile(OUTPUT + FREEBASE)
    freebase_rdd.repartition(1) \
        .saveAsTextFile(OUTPUT + KG)
    kg_rdd.repartition(1) \
        .saveAsTextFile(OUTPUT + SMD)

    try:
        os.mkdir(OUTPUT)
    except:
        shutil.rmtree(OUTPUT)
        os.mkdir(OUTPUT)
    shutil.move(OUTPUT + KEY + "/part-00000", OUTPUT + '/freebase-s1' + KEY)
    shutil.move(OUTPUT + COMMON + "/part-00000", OUTPUT + '/freebase-s1' + COMMON)
    shutil.move(OUTPUT + TYPE + "/part-00000", OUTPUT + '/freebase-s1' + TYPE)
    shutil.move(OUTPUT + FREEBASE + "/part-00000", OUTPUT + '/freebase-s1' + FREEBASE)
    shutil.move(OUTPUT + KG + "/part-00000", OUTPUT + '/freebase-s1' + KG)
    shutil.move(OUTPUT + SMD + "/part-00000", OUTPUT + '/freebase-s1' + SMD)

if __name__ == "__main__":
    spark = utils.create_session("FB_filtering")
    sc = spark.sparkContext

    try:
        # shutil.rmtreee(OUTPUT)
        shutil.rmtree(OUTPUT + KEY)
        shutil.rmtree(OUTPUT + COMMON)
        shutil.rmtree(OUTPUT + TYPE)
        shutil.rmtree(OUTPUT + FREEBASE)
        shutil.rmtree(OUTPUT + KG)
        shutil.rmtree(OUTPUT + SMD)
    except:
        pass

    fb_rdd = utils.load_data(sc, PROT + INPUT)

    # test on 1 million
    # test_rdd = sc.parallelize(fb_rdd.take(1000000))
    # run_job(test_rdd)

    run_job(fb_rdd)
