import os
import re
import shutil

import utils

'''
S3: FILTER CUSTOM TYPES
Input: freebase-s2/freebase-s2-type
Output: freebase-s3/freebase-s3-type (filtered from custom types)
'''

PROT = "file://"
ROOTDIR = '/home/freebase/freebase-s3/'
INPUT = '/home/freebase/freebase-s2/freebase-s2-type'
TMPDIR = ROOTDIR + 'freebase-s3-type-tmp'
OUTPUT = ROOTDIR + 'freebase-s3-type'

CUSTOM_TYPES = ['\"/base/', '\"/user/']


# Map s,p,o RDF triples to (s,[p,o]) key-value pairs
def extract_subject(row):
    fields = row.split("\t")
    return (fields[0], fields[1:-1])  # ignore trailing dot (last field)


# Filter (s, [[p1,o1],[p2,o2],...,[pN,oN]]) key-value pairs if one of the objects
# references a custom domain (such as /user/ or /base/). This means that subject s
# is a custom type.
def is_custom_type_def(row):
    if (row[0].startswith(("<f:m.", "<f:g."))):
        for po in iter(row[1]):
            if po[1].startswith(tuple(CUSTOM_TYPES)):
                return False
    return True


# Reform the tab-separated string.
def reformat_string(row):
    out = [row[0]]
    for token in row[1]:
        out.append(token)
    out.append('.')
    return('\t'.join(out))


def run_job(rdd):
    rdd = rdd \
        .map(extract_subject) \
        .groupByKey() \
        .filter(is_custom_type_def) \
        .flatMapValues(lambda x: x) \
        .map(reformat_string) \
        .repartition(1) \
        .saveAsTextFile(TMPDIR)


if __name__ == "__main__":
    spark = utils.create_session("FB_S3")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    rdd = utils.load_data(sc, PROT + INPUT)
    run_job(rdd)

    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)
