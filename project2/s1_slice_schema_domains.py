import os
import re
import shutil

import utils

'''
S1: SLICE SCHEMA DOMAINS
Input: freebase-s0 (~95GB)
Output: under freebase-s1/, a different dump for each domain slice in SLICE_IDS
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/'
INPUT = ROOTDIR + 'freebase-s0'
TMPDIR = ROOTDIR + 'freebase-s1-tmp'  # -sid
OUTDIR = ROOTDIR + 'freebase-s1'  # -sid

SLICE_IDS = ['smd', 'key', 'common', 'type', 'freebase', 'kg']

DOMAIN_PATTERNS = r"\t<(k:|f:(common|type|freebase|kg))[^>]*>\t(?!\.)"
PATTERN_KEY = r"\t<k:[^>]*>\t(?!\.)"
PATTERN_COMMON = r"\t<f:common[^>]*>\t(?!\.)"
PATTERN_TYPE = r"\t<f:type[^>]*>\t(?!\.)"
PATTERN_FREEBASE = r"\t<f:freebase[^>]*>\t(?!\.)"
PATTERN_KG = r"\t<f:kg[^>]*>\t(?!\.)"


def run_job(rdd):
    # Filter the RDD from all non-domain triples.
    # The resulting RDD contains triples from the Subject-Matter Domains (SMD)
    smd_rdd = rdd.filter(lambda x: not re.findall(DOMAIN_PATTERNS, x))

    # Do the opposite: filter all SMD triples
    schema_rdd = rdd.filter(lambda x: re.findall(DOMAIN_PATTERNS, x))

    # Extract single-domain RDDs from the schema domains RDD
    key_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_KEY, x))
    com_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_COMMON, x))
    typ_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_TYPE, x))
    frb_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_FREEBASE, x))
    kgg_rdd = schema_rdd.filter(lambda x: re.findall(PATTERN_KG, x))

    # Save RDDs
    smd_rdd.repartition(1).saveAsTextFile(f"{TMPDIR}/fbs1tmp-{SLICE_IDS[0]}")
    key_rdd.repartition(1).saveAsTextFile(f"{TMPDIR}/fbs1tmp-{SLICE_IDS[1]}")
    com_rdd.repartition(1).saveAsTextFile(f"{TMPDIR}/fbs1tmp-{SLICE_IDS[2]}")
    typ_rdd.repartition(1).saveAsTextFile(f"{TMPDIR}/fbs1tmp-{SLICE_IDS[3]}")
    frb_rdd.repartition(1).saveAsTextFile(f"{TMPDIR}/fbs1tmp-{SLICE_IDS[4]}")
    kgg_rdd.repartition(1).saveAsTextFile(f"{TMPDIR}/fbs1tmp-{SLICE_IDS[5]}")


if __name__ == "__main__":
    spark = utils.create_session("FB_S1")
    sc = spark.sparkContext

    if os.path.exists(OUTDIR):
        [os.remove(f"{OUTDIR}/freebase-s1-{sid}") for sid in SLICE_IDS]
    else:
        os.mkdir(OUTDIR)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)
    else:
        os.mkdir(TMPDIR)

    fb_rdd = utils.load_data(sc, PROT + INPUT)
    run_job(fb_rdd)

    for sid in SLICE_IDS:
        shutil.move(f"{TMPDIR}/fbs1tmp-{sid}/part-00000",
                    f"{OUTDIR}/freebase-s1-{sid}")
    shutil.rmtree(TMPDIR)
