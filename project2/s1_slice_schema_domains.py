import re
import shutil
import os

import utils

INPUT = '/home/freebase/freebase-s0'
OUTPUT = '/home/freebase/freebase-s1'

SLICE_IDS = ['-smd', '-key', '-common', '-type', '-freebase', '-kg']

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
    smd_rdd.repartition(1).saveAsTextFile(OUTPUT + SLICE_IDS[0])
    key_rdd.repartition(1).saveAsTextFile(OUTPUT + SLICE_IDS[1])
    common_rdd.repartition(1).saveAsTextFile(OUTPUT + SLICE_IDS[2])
    type_rdd.repartition(1).saveAsTextFile(OUTPUT + SLICE_IDS[3])
    freebase_rdd.repartition(1).saveAsTextFile(OUTPUT + SLICE_IDS[4])
    kg_rdd.repartition(1).saveAsTextFile(OUTPUT + SLICE_IDS[5])


if __name__ == "__main__":
    spark = utils.create_session("FB_slicing")
    sc = spark.sparkContext

    try:
        shutil.rmtree(OUTPUT)
    except:
        os.mkdir(OUTPUT)

    for slice_id in SLICE_IDS:
        try:
            shutil.rmtree(OUTPUT + slice_id)
        except:
            pass  

    fb_rdd = utils.load_data(sc, 'file://' + INPUT)
    run_job(fb_rdd)

    for slice_id in SLICE_IDS:
        shutil.move(OUTPUT + slice_id + "/part-00000", OUTPUT + '/freebase-s1' + slice_id)


