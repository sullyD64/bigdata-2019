import os
import re
import shutil

import utils

'''
S0: PREFILTER & SHORTEN URLs
Input: the full 400GB dump
Output: freebase-s0 (filtered from most useless domains and shortened)
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/'
INPUT = ROOTDIR + 'freebase-rdf-latest'
TMPDIR = ROOTDIR + 'freebase-s0-tmp'
OUTPUT = ROOTDIR + 'freebase-s0'

NS_PREFIX = "http://rdf.freebase.com/ns/"
NEW_NS_PREFIX = "f:"

KEY_PREFIX = "http://rdf.freebase.com/key"
NEW_KEY_PREFIX = "k:"

SKIP_PATTERNS = r"\"@(?!en)|" \
    + r"\"@en-|" \
    + r"/common\.(?!topic|document|notable_for)|" \
    + r"\t<http:\/\/www\.w3\.org[^>]*>\t(?!\.)|" \
    + r"/base\.|" \
    + r"/freebase\.(?!type_hints)|" \
    + r"/dataworld\.|" \
    + r"/user\.|" \
    + r"/pipeline\.|" \
    + r"/kp_lw\.|" \
    + r"/help\.|" \
    + r"/usergroup\.|" \
    + r"/community\.|" \
    + r"/atom\.|" \
    + r"\t<http:[^>]*>\t\." # moved here from s3-0 (which is now deprecated)

def clean_triple(row):
    # Shorten URL prefixes for /ns/ and /key/ namespaces
    row = row.replace(NS_PREFIX, NEW_NS_PREFIX).replace(KEY_PREFIX, NEW_KEY_PREFIX)
    # Remove CustomDataTypes from Literals (like w3.org's XMLSchema)
    return re.sub(r"\^\^[^\t]*", "", row)


def run_job(rdd):
    rdd = rdd \
        .filter(lambda x: not re.findall(SKIP_PATTERNS, x)) \
        .map(clean_triple) \
        .repartition(1) \
        .saveAsTextFile(TMPDIR)


if __name__ == "__main__":
    spark = utils.create_session("FB_S0")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    fb_rdd = utils.load_data(sc, PROT + INPUT)
    run_job(fb_rdd)

    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)