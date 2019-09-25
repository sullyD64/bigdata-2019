import os
import re
import shutil

from rdflib import RDF, Literal, Namespace
from rdflib import URIRef as URI

import utils

'''
NEW TYPE-INSTANCE-MAP BUILDER
This builder uses Spark RDDs instead of rdflib graphs.

Input: freebase-s3-smd, tim_reference
Output: s5-tim

This job is subdivided in the following steps:
1) type inference from SMD
2) finding anonymous pits 
    (a node is a pit if there are no outgoing arcs)
    (a node is anonymous if there are no literal properties describing it)
    (a mid is an anonymous pit if it never appears on the subject of any triple in SMD)
3) anonymous pit type lookup in TIM
4) join the anonymous pit for which a type has been found with the types inferenced from SMD.
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/freebase-s5/'
# TODO change this when s0, s1 and s2 are run again. for now, we keep the "old" s32 dump
INPUT_SMD = '/home/freebase/freebase-s3/freebase-s3-smd__old'
INPUT_TIM = '/home/freebase/freebase-s4/tim_reference'
# INPUT_SMD = '/home/freebase/freebase-s3/smdtest'
# INPUT_TIM = '/home/freebase/freebase-s4/timtest'
TMPDIR = ROOTDIR + 's5-tim-tmp'
OUTPUT = ROOTDIR + 's5-tim'

FB = Namespace('f:')
FBO = Namespace('fbo:')


# Some triples have invalid predicates!
def is_valid_predicate(row):
    return len(row.split("\t")[1][3:-1].split(".")) == 3


def extract_left(row):
    tokens = row.split("\t")
    subj, pred = tokens[0], tokens[1]
    dt = pred[3:-1].split(".")
    return (subj, f"{dt[0]}.{dt[1]}")


def is_relation(row):
    return row.split("\t")[2].startswith('<')


def extract_right(row):
    tokens = row.split("\t")
    obj = tokens[2]
    return (obj, None)


def is_instance_of_a_type(row):
    return len(row.split("\t")[2][3:-1].split('.')) == 2


def extract_tim_type(row):
    tokens = row.split("\t")
    subj, dt = tokens[0], tokens[2][3:-1]
    return (subj, dt)


def generate_triples(row):
    subj = row[0]
    dt = ''
    if (row[1][0] and not row[1][1]):
        dt = row[1][0]
    elif (row[1][1] and not row[1][0]):
        dt = row[1][1]
    
    def format_triple(s, p, o):
        return f"<{s}>\t<{p}>\t<{o}>\t."

    # rdf:type is for assigning labels to the instance nodes
    # fbo:type is for establishing actual links with the onthology nodes
    rdftype = format_triple(URI(FB+subj[3:-1]), RDF.type, URI(FBO+dt))
    fbotype = format_triple(URI(FB+subj[3:-1]), FBO.type, URI(FBO+dt))

    return [rdftype, fbotype]


def run_job(smd, tim):
    # step 0 (some triples might not be valid)
    smd = smd.filter(is_valid_predicate)

    # step 1: type-instance inference from SMD
    sleft = smd \
        .map(extract_left) \
        .distinct()

    # step 2: finding anonymous pits
    sright = smd \
        .filter(is_relation) \
        .map(extract_right) \
        .distinct()
    an_pits = sleft.fullOuterJoin(sright) \
        .filter(lambda x: not x[1][0] and not x[1][1]) \
        .map(lambda x: (x[0], None))

    # step 3: look for types of anonymous pits in TIM
    tim = tim \
        .filter(is_instance_of_a_type) \
        .map(extract_tim_type) \
        .distinct()
    an_pits_populated = tim.rightOuterJoin(an_pits) \
        .map(lambda x: (x[0],x[1][0]))

    # step 4: add the "resolved" anonymous pits types to the inferenced types
    result = sleft.fullOuterJoin(an_pits_populated) \
        .flatMap(generate_triples) \
        .repartition(1) \
        .saveAsTextFile(TMPDIR)

if __name__ == "__main__":
    spark = utils.create_session("FB_S5_ONTOLOGY")
    sc = spark.sparkContext

    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    if os.path.exists(TMPDIR):
        shutil.rmtree(TMPDIR)

    smd_rdd = utils.load_data(sc, PROT + INPUT_SMD)
    tim_rdd = utils.load_data(sc, PROT + INPUT_TIM)
    run_job(smd_rdd, tim_rdd)

    shutil.move(f"{TMPDIR}/part-00000", OUTPUT)
    shutil.rmtree(TMPDIR)
