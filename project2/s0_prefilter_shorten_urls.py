import re
import shutil

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

PROT = "file://"
INPUT = "/home/freebase/freebase-rdf-latest"
# INPUT = "/home/freebase/fb20m"
OUTPUT = INPUT + "-s0"

NS_PREFIX = "http://rdf.freebase.com/ns/"
NEW_NS_PREFIX = "f:"

KEY_PREFIX = "http://rdf.freebase.com/key"
NEW_KEY_PREFIX = "k:"

SKIP_PATTERNS = "\"@(?!en)|" \
    + "\"@en-|" \
    + "/common\.(?!topic|document|notable_for)|" \
    + "\t<http:\/\/www\.w3\.org[^>]*>\t(?!\.)|" \
    + "/base\.|" \
    + "/freebase\.(?!type_hints)|" \
    + "/dataworld\.|" \
    + "/user\.|" \
    + "/pipeline\.|" \
    + "/kp_lw\.|" \
    + "/help\.|" \
    + "/usergroup\.|" \
    + "/community\.|" \
    + "/atom\." \


def clean_triple(line):
    # shorten URL prefixes for /ns/ and /key/ namespaces
    line = line.replace(NS_PREFIX, NEW_NS_PREFIX).replace(KEY_PREFIX, NEW_KEY_PREFIX)
    # removes external schema references for literal types (like w3.org's XMLSchema)
    return re.sub(r"\^\^[^\t]*\t", "", line)


def run_job(rdd):
    rdd = rdd \
        .filter(lambda x: not re.findall(SKIP_PATTERNS, x)) \
        .map(clean_triple)

    rdd.repartition(1).saveAsTextFile(PROT + OUTPUT)
    # for l in rdd.collect():
    #     print(l)


if __name__ == "__main__":
    spark = utils.create_session("FB_filtering")
    sc = spark.sparkContext

    try:
        shutil.rmtree(OUTPUT)
    except:
        pass

    fb_rdd = utils.load_data(sc, PROT + INPUT)
    run_job(fb_rdd)
