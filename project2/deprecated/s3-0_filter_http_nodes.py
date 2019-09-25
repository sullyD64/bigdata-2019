import re
import os
import shutil

import utils

msg = '''
DEPRECATED. This was made to further filter _problematic_ triples from the tump, which
would give problems especially in smd. This is the case for triples containing URIRefs
(mostly on the Object) with absolute http://... URIs, which weren't properly shortened
in s0 and were parsed as :Resource nodes by neosemantics.

Since all this job does is simply filter triples matching the pattern, the following
has been included in s0. For this reason, this job serves no more purpose and shouldn't be
run on future dumps.
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/'
INPUT = ROOTDIR + 'freebase-s2'  # -sid
TMPDIR_ROOT = ROOTDIR + 'freebase-s30-tmp'  # -sid
OUTPUT = ROOTDIR + 'freebase-s30'  # -sid

SLICE_IDS = ['smd', 'type']

SKIP_PATTERNS = r"\t<http:[^>]*>\t\."


def run_job(rdd, sid):

    rdd.filter(lambda x: not re.findall(SKIP_PATTERNS, x)) \
        .repartition(1) \
        .saveAsTextFile(f"{TMPDIR_ROOT}/fbs30tmp-{sid}")


if __name__ == "__main__":
    DeprecationWarning(msg)
    spark = utils.create_session("FB_S3-0")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        [os.remove(f"{OUTPUT}/freebase-s30-{sid}") for sid in SLICE_IDS]
    else:
        os.mkdir(OUTPUT)

    if os.path.exists(TMPDIR_ROOT):
        shutil.rmtree(TMPDIR_ROOT)
    else:
        os.mkdir(TMPDIR_ROOT)

    # Run a job for each slice produced in s1
    for sid in SLICE_IDS:
        rdd = utils.load_data(sc, PROT + INPUT + sid)
        run_job(rdd, sid)

    for sid in SLICE_IDS:
        shutil.move(f"{TMPDIR_ROOT}/fbs30tmp-{sid}/part-00000",
                    f"{OUTPUT}/freebase-s30-{sid}")
    shutil.rmtree(TMPDIR_ROOT)
