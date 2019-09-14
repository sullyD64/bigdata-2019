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
    rdd = rdd \
        .filter(lambda x: not re.findall(SKIP_PATTERNS, x)) \
        .map(lambda x: x.replace(FB_PREFIX, NEW_PREFIX)) \
        .collect()

    # TODO implement save new rdd
    for l in rdd:
        print(l)


if __name__ == "__main__":
    spark=utils.create_session("FB_filtering")
    sc=spark.sparkContext

    fb_rdd=utils.load_data(sc, FB_DUMP_PATH)
    run_job(fb_rdd)
