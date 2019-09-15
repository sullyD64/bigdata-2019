from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
import re
import utils
import shutil

PROT = "file://"
# INPUT = "/home/freebase/freebase-rdf-latest"
INPUT = "/home/freebase/fb1m"
OUTPUT = INPUT + "-s0"

URI_PREFIX = "http://rdf.freebase.com/ns/"
NEW_URI_PREFIX = "f:"

# TODO refine cleaning strategy
SKIP_PATTERNS = "@(?!en)|"\
    + "w3\.org|"\
    + "/type\.|"\
    + "/user.*|"\
    + "/freebase\.(?!domain_category).*|"\
    + "/usergroup\.|"\
    + "/permission\.|"\
    + "/community\.|"\
    + "/common\.(?!document|topic)\b.*|"\
    + "/common\.document\.(?!source_uri)\b.*|"\
    + "/common\.topic\.(description|image|webpage|properties|weblink|notable_for|article).*|"\
    + "/dataworld\.|"\
    + "/key/.*|"\
    + "/base\."


def run_job(rdd):
    rdd = rdd \
        .filter(lambda x: not re.findall(SKIP_PATTERNS, x)) \
        .map(lambda x: x.replace(URI_PREFIX, NEW_URI_PREFIX))

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
