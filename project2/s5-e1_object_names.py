import os
import shutil

import s5_enrichment_extractor as extractor
import utils

'''
E1: ENRICH SMD WITH OBJECT NAMES
Input: freebase-s3-smd and freebase-s3-type
Output: e1-fbo_name (triples for adding NAMES to entities in SMD, obtained by scanning TYPE for type.object.name)
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/freebase-s5/'
# TODO change this when s0, s1 and s2 are run again. for now, we keep the "old" s32 dump
# INPUT_DATA = '/home/freebase/freebase-s3/freebase-s30-smd__old'
# INPUT_EXT = '/home/freebase/freebase-s3/freebase-s3-type'
INPUT_DATA = '/home/freebase/freebase-s3/smdtest'
INPUT_EXT = '/home/freebase/freebase-s3/typetest'
TMPDIR = ROOTDIR + 'e1-fbo_name-tmp'
OUTPUT = ROOTDIR + 'e1-fbo_name'

# override this to enable caching Data
USE_CACHE = True
CACHED_DATA_TMP = ROOTDIR + '__cached-smd-mids-tmp'
CACHED_DATA = ROOTDIR + '__cached-smd-mids'

LOOKUP_PRED = 'type.object.name'
OUTPUT_PRED = 'fbo:name'


if __name__ == "__main__":
    spark = utils.create_session("FB_S5_E1")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    if USE_CACHE and os.path.exists(CACHED_DATA):
        data_rdd = utils.load_data(sc, PROT + CACHED_DATA)
    else:
        data_rdd = utils.load_data(sc, PROT + INPUT_DATA)
    
    ext_rdd = utils.load_data(sc, PROT + INPUT_EXT)
    results = extractor.run(data_rdd, ext_rdd, LOOKUP_PRED, OUTPUT_PRED, USE_CACHE, True, None, None)  # distinct true, no given filtering regex or key mapping

    if USE_CACHE:
        data_tocache = results[0]
        data_tocache.repartition(1).saveAsTextFile(CACHED_DATA_TMP)
        shutil.move(f"{CACHED_DATA_TMP}/part-00000", CACHED_DATA)
        shutil.rmtree(CACHED_DATA_TMP)

    out = results[1]
    out.repartition(1).saveAsTextFile(TMPDIR)
    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)
