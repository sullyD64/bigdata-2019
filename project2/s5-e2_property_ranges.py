import os
import re
import shutil

from rdflib import RDFS

import s5_enrichment_extractor as extractor
import utils

'''
E2: ENRICH ONTOLOGY WITH PROPERTY RANGES
Input: s5-ontology and freebase-s3-type
Output: e2-rdfs_range (triples to add RANGE to Properties in the ontology, obtained by scanning TYPE for type.property.expected_type)
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/freebase-s5/'
INPUT_DATA = ROOTDIR + 's5-ontology'
INPUT_EXT = '/home/freebase/freebase-s3/freebase-s3-type'
# INPUT_EXT = '/home/freebase/freebase-s3/typetest'
TMPDIR = ROOTDIR + 'e2-rdfs_range-tmp'
OUTPUT = ROOTDIR + 'e2-rdfs_range'

# override this to enable caching Data
USE_CACHE = False
CACHED_DATA_TMP = ROOTDIR + 'cached-ontology-tmp'
CACHED_DATA = ROOTDIR + 'cached-ontology'

LOOKUP_PRED = 'type.property.expected_type'
OUTPUT_PRED = RDFS.range

# regex for key filtering (select only properties)
IS_PROPERTY = r"^<fbo:([^>\.]+\.){2}[^>\.]+>$"
EXT_KEY_MAPPING = { 
    'pattern': r"^<f:", 
    'replace': r"<fbo:" 
    }

if __name__ == "__main__":
    spark = utils.create_session("FB_S5_E2")
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
    results = extractor.run(data_rdd, ext_rdd, LOOKUP_PRED, OUTPUT_PRED, USE_CACHE, False, IS_PROPERTY, EXT_KEY_MAPPING)  # caching and distinct false

    if USE_CACHE:
        data_tocache = results[0]
        data_tocache.repartition(1).saveAsTextFile(CACHED_DATA_TMP)
        shutil.move(f"{CACHED_DATA_TMP}/part-00000", CACHED_DATA)
        shutil.rmtree(CACHED_DATA_TMP)

    out = results[1]
    out.repartition(1).saveAsTextFile(TMPDIR)
    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)
