import os
import re
import shutil
import sys

from pyspark import Row, SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from rdflib.plugins.parsers.ntriples import *

import utils

PROT = "file://"
INPUT='/home/freebase/freebase-s1/freebase-s1'
OUTPUT='/home/freebase/freebase-s2'

SLICE_IDS = ['-smd', '-key', '-common', 'type', '-freebase', '-kg']
# SLICE_IDS = ['-smdtest', '-commontest']

TABBED_PATTERN = r'^(([^\t]*\t){2}(<[^>]*>|".*"(@en)?))\.$'

# Filter invalid triples from the slices.
# An invalid triple is a string that raises an exception when is parsed by a NTriplesParser
def validate_triple(row):
    try:
        sys.stdout = open(os.devnull, 'w')
        NTriplesParser().parsestring(row)
        sys.stdout = sys.__stdout__
        return True;
    except:
        return False;

# Restores the tab between the literal string and the dot which has been incorrectly removed in s0
# when removing the CustomDataType declarations (^^)
def fix_missing_tab(row):
    match = re.search(TABBED_PATTERN, row)
    if match:
        return match.group(1) + "\t."
    else:
        return row


def run_job(rdd, slice_id):

    # Filter, transform and save
    rdd.filter(validate_triple) \
        .map(fix_missing_tab) \
        .repartition(1) \
        .saveAsTextFile(OUTPUT + slice_id)


if __name__ == "__main__":
    spark = utils.create_session("FB_validation")
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

    # Run the job for each slice produced in s1
    for slice_id in SLICE_IDS:
        rdd = utils.load_data(sc, PROT + INPUT + slice_id)
        run_job(rdd, slice_id)

    for slice_id in SLICE_IDS:
        shutil.move(OUTPUT + slice_id + "/part-00000", OUTPUT + '/freebase-s2' + slice_id)
