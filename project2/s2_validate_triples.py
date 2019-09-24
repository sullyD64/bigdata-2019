import os
import re
import shutil
import sys

from rdflib import NTriplesParser

import utils

'''
S2: VALIDATE TRIPLES (and remove invalid ones)
Input: freebase-s1/freebase-s1-* (one for each slice in SLICE_IDs)
Output: freebase-s2/freebase-s2-* (filtered from invalid triples)
'''

PROT = "file://"
ROOTDIR = '/home/freebase/'
INPUT = ROOTDIR + 'freebase-s1'  # -sid
TMPDIR_ROOT = ROOTDIR + 'freebase-s2-tmp'  # -sid
OUTDIR = ROOTDIR + 'freebase-s2'  # -sid

SLICE_IDS = ['smd', 'key', 'common', 'type', 'freebase', 'kg']


# Filter invalid triples from the slices.
# An invalid triple is a string that raises an exception when is parsed by a NTriplesParser
def is_valid_triple(row):
    try:
        # Silence stdout since NTriplesParser prints the parsed string
        sys.stdout = open(os.devnull, 'w')
        NTriplesParser().parsestring(row)
        sys.stdout = sys.__stdout__
        return True
    except:
        return False

# UNUSED: was added to correct an error produced in s0.
# Restores the tab between the literal string and the dot which has been
# incorrectly removed in s0 when removing the CustomDataTypes (^^)
# def fix_missing_tab(row):
#     TABBED_PATTERN = r'^(([^\t]*\t){2}(<[^>]*>|".*"(@en)?))\.$'
#     match = re.search(TABBED_PATTERN, row)
#     if match:
#         return match.group(1) + "\t."
#     else:
#         return row


def run_job(rdd, sid):
    rdd.filter(is_valid_triple) \
        .repartition(1) \
        .saveAsTextFile(f"{TMPDIR_ROOT}/fbs2tmp-{sid}")
    # .map(fix_missing_tab) \


if __name__ == "__main__":
    spark = utils.create_session("FB_S2")
    sc = spark.sparkContext

    if os.path.exists(OUTDIR):
        [os.remove(f"{OUTDIR}/freebase-s1-{sid}") for sid in SLICE_IDS]
    else:
        os.mkdir(OUTDIR)

    if os.path.exists(TMPDIR_ROOT):
        shutil.rmtree(TMPDIR_ROOT)
    else:
        os.mkdir(TMPDIR_ROOT)

    # Run a job for each slice in SLICE_IDS
    for sid in SLICE_IDS:
        rdd = utils.load_data(sc, f"{PROT}{INPUT}-sid")
        run_job(rdd, sid)

    for sid in SLICE_IDS:
        shutil.move(f"{TMPDIR_ROOT}/fbs2tmp-{sid}/part-00000",
                    f"{OUTDIR}/freebase-s2-{sid}")
    shutil.rmtree(TMPDIR_ROOT)
