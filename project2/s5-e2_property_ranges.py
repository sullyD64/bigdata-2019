import os
import re
import shutil

from rdflib import RDFS

import s5_enrichment_extractor as extractor
import utils

'''
E1: ENRICH ONTOLOGY WITH PROPERTY RANGES
Input: s5-ontology and freebase-s3-type
Output: e2-rdfs_range (triples to add RANGE to Properties in the ontology, obtained by scanning TYPE for type.property.expected_type)
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/freebase-s5/'
INPUT_DATA = ROOTDIR + 's5-ontology'  # TODO implement ontology_builder_spark
INPUT_EXT = '/home/freebase/freebase-s3/freebase-s3-type'
TMPDIR = ROOTDIR + 'e2-rdfs_range-tmp'
CACHED_DATA_TMP = None  # no caching for this job
CACHED_DATA = None
OUTPUT = ROOTDIR + 'e2-rdfs_range'

LOOKUP_PRED = 'type.property.expected_type'
OUTPUT_PRED = RDFS.range


def is_property(kv_pair):
    return re.findall(r"^<f:([^>\.]+\.){2}[^>\.]+>$", kv_pair[0])


if __name__ == "__main__":
    spark = utils.create_session("FB_S5_E2")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    cached = True if os.path.exists(CACHED_DATA) else False
    if cached:
        data_rdd = utils.load_data(sc, PROT + CACHED_DATA)
    else:
        data_rdd = utils.load_data(sc, PROT + INPUT_DATA)
    ext_rdd = utils.load_data(sc, PROT + INPUT_EXT)

    results = extractor.run(
        data_rdd, ext_rdd, LOOKUP_PRED, OUTPUT_PRED, cached, True, is_property) # distinct false, given filtering function

    if not cached:
        data_tocache = results[0]
        data_tocache.repartition(1).saveAsTextFile(CACHED_DATA_TMP)
        shutil.move(f"{CACHED_DATA_TMP}/part-00000", CACHED_DATA)
        shutil.rmtree(CACHED_DATA_TMP)

    out = results[1]
    out.repartition(1).saveAsTextFile(TMPDIR)
    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)
