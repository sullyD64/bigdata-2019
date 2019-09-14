from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
import re
import utils

# FB_DUMP_PATH = "file:///home/freebase/freebase-rdf-latest"
FB_DUMP_PATH = "file:///home/freebase/fb100k"
FB_PREFIX = "http://rdf.freebase.com/ns/"
NEW_PREFIX = "f:"

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
    rdd = rdd
        .filter(lambda x: return not re.findall(SKIP_PATTERNS, x)
        .map(lambda x: return x.replace(FB_PREDIX, NEW_PREFIX)

    print(l)  # TODO implement save new rdd
    for l in rdd.collect():


if __name__ == "__main__":
    spark=utils.create_session("FB_filtering")
    sc=spark.sparkContext

    fb_rdd=utils.load_data(sc, FB_DUMP_PATH)
    run_job(fb_rdd)
