import os
import re
import shutil

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

import utils

PROT = "file://"
ROOT = '/home/freebase/freebase-s3/'
INPUT = ROOT + 'freebase-s3-type'
TMP = ROOT + 'freebase-s32-type-out'
OUTPUT = ROOT + 'freebase-s32-type'

CUSTOM_TYPES = ['\"/base/', '\"/user/']


def is_custom_type_def(row):
    if (row[0].startswith(("<f:m.", "<f:g."))):
        for subject_object_dot in iter(row[1]):
            if subject_object_dot[1].startswith(tuple(CUSTOM_TYPES)):
                return False
        return True


def extract_subject(row):
    fields = row.split("\t")
    return (fields[0], fields[1:])


def reformat_string(row):
    out = [row[0]]
    for token in row[1]:
        out.append(token)
    return('\t'.join(out))


def run_job(rdd):
    rdd = rdd \
        .map(extract_subject) \
        .groupByKey() \
        .filter(is_custom_type_def) \
        .flatMapValues(lambda x: x) \
        .map(reformat_string) \
        .repartition(1) \
        .saveAsTextFile(TMP)


if __name__ == "__main__":
    spark = utils.create_session("FB_filtering_customtypes")
    sc = spark.sparkContext

    try:
        shutil.rmtree(TMP)
    except:
        pass

    rdd = utils.load_data(sc, PROT + INPUT)
    run_job(rdd)

    shutil.move(TMP + "/part-00000", OUTPUT)
    shutil.rmtree(TMP)
