import os
import re
import shutil

from rdflib import RDF, Literal, Namespace
from rdflib import URIRef as URI

import utils

'''
NEW TYPE-INSTANCE-MAP BUILDER
This builder uses Spark RDDs instead of rdflib graphs.
TODO implement new TIM builder, need TTI too
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/freebase-s5/'
# TODO change this when s0, s1 and s2 are run again. for now, we keep the "old" s32 dump
# INPUT = '/home/freebase/freebase-s3/freebase-s3-smd__old'
INPUT = '/home/freebase/freebase-s3/smdtest'
TMPDIR = ROOTDIR + 's5-tim-tmp'
OUTPUT = ROOTDIR + 's5-tim'

FB = Namespace('f:')
FBO = Namespace('fbo:')


def generate_ontology(row):
    tokens = row.split("\t")
    subj, pred, obj = tokens[0], tokens[1], tokens[2]
    onto = []
    pred = pred[3:-1].split(".")

    if len(pred) == 3:
        utype = f"{pred[0]}.{pred[1]}"
        # rdf:type is for assigning labels to the instance nodes
        add_subjtype = (URI(FB+subj[3:-1]), RDF.type, URI(FBO+utype))
        # fbo:type is for establishing actual links with the onthology nodes
        add_subjtype = (URI(FB+subj[3:-1]), FBO.type, URI(FBO+utype))

    return (subj, onto)


def format_string(row):
    s, p, o = row[0], row[1], row[2]
    if isinstance(o, Literal):
        return f"<{s}>\t<{p}>\t\"{o}\"\t."
    else:
        return f"<{row[0]}>\t<{row[1]}>\t<{row[2]}>\t."


def run_job(rdd):
    # rdd = rdd \
    #     .map(generate_ontology) \
    #     .flatMapValues(lambda x: x) \
    #     .map(lambda row: row[1]) \
    #     .distinct() \
    #     .map(format_string) \
    #     .repartition(1) \
    #     .saveAsTextFile(TMPDIR)


if __name__ == "__main__":
    spark = utils.create_session("FB_S5_ONTOLOGY")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    fb_rdd = utils.load_data(sc, PROT + INPUT)
    run_job(fb_rdd)

    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)
