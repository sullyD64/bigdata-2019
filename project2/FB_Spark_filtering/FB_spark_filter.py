from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
import re
import utils

# HISTORY_PATH = "hdfs://localhost:9000/user/user33/testset/dataset_test_100k_header.csv"
FB_PATH = "path/to/FB"
PREFIX = "http://rdf.freebase.com/ns/"
PREFIX_SUBST = "f:"


def is_triple_allowed(line):
    #se passa attraverso tutte le regex ritorna True, altrimenti False

    SKIP_PATTERNS = "@(?!en)|"\
    +"w3\.org|"\
    +"/type\.|"\
    +"/user.*|"\
    +"/freebase\.(?!domain_category).*|"\
    +"/usergroup\.|"\
    +"/permission\.|"\
    +"/community\.|"\
    +"/common\.(?!document|topic)\\b.*|"\
    +"/common\.document\.(?!source_uri)\b.*|"\
    +"/common\.topic\.(description|image|webpage|properties|weblink|notable_for|article).*|"\
    +"/dataworld\.|"\
    +"/key/.*|"\
    +"/base\."
    

    return not re.findall(SKIP_PATTERNS , line)


def clean_triple(line):
    return line.replace(PREFIX,PREFIX_SUBST)

def run_job(rdd):
    rdd = rdd.filter(is_triple_allowed).map(clean_triple).collect()
    for l in rdd:
        print(l)


if __name__ == "__main__":
    spark = utils.create_session("FB_filtering")
    sc = spark.sparkContext
    # sqlContext = SQLContext(sc)

    fb_rdd = utils.load_data(sc, FB_PATH)
    run_job(fb_rdd)
