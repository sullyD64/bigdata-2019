import os
import re
import shutil

import utils

'''
S4: TYPE-INSTANCE-MAP Reference extractor
Input: freebase-s3/freebase-s3-type
Output: freebase-s4/tti_reference (a reference for the type-instance-map which we will build later)
    Note that tti does not include triples in which the type is from the /common/ domanin.
    On ~181M triples of this type, 81 are from the /common/ domain.
    We should change the filtering regex when we will decide to include the domain in the graph.
'''

FILTERING_REGEX = r'^<f:(?!common)[^\>.]{2,}\.[^>]+>\t<f:type\.type\.instance\>'

PROT = 'file://'
ROOTDIR = '/home/freebase/freebase-s4/'
INPUT = '/home/freebase/freebase-s3/freebase-s3-type'
# INPUT = '/home/freebase/freebase-s3/typetest'
TMPDIR = ROOTDIR + 'tim_reference-tmp'
OUTPUT = ROOTDIR + 'tim_reference'


def swap_triple(row):
    tokens = row.split("\t")
    s, o = tokens[0], tokens[2]
    return f"{o}\t<fbo:type>\t{s}\t."


def run_job(rdd):
    rdd = rdd \
        .filter(lambda x: re.findall(FILTERING_REGEX, x)) \
        .map(swap_triple) \
        .repartition(1) \
        .saveAsTextFile(TMPDIR)


if __name__ == "__main__":
    spark = utils.create_session("FB_S4")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    fb_rdd = utils.load_data(sc, PROT + INPUT)
    run_job(fb_rdd)

    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)
