import os
import re
import shutil

import utils
from rdflib import OWL, RDF, RDFS, Literal, Namespace
from rdflib import URIRef as URI

'''
NEW ONTOLOGY BUILDER
This builder uses Spark RDDs instead of rdflib graphs.

For every triple in SMD:
-   create N ontology triples and store them in a list [o1,o2,...,oN] where oi = (s,p,o).
        (we generate a O(9) triples, so O(1) triples)
-   map the triple (<subject>, [o1,o2,...,oN])
-   flatMap each row: each row is (<subject>, oi)
-   replace the key with the value
-   distinct the keys (this way we eliminate all the duplicates)
-   save all the triples
'''

PROT = 'file://'
ROOTDIR = '/home/freebase/freebase-s5/'
# TODO change this when s0, s1 and s2 are run again. for now, we keep the "old" s32 dump
# INPUT = '/home/freebase/freebase-s3/freebase-s3-smd__old'
INPUT = '/home/freebase/freebase-s3/smdtest'
TMPDIR = ROOTDIR + 's5-ontology-tmp' 
OUTPUT = ROOTDIR + 's5-ontology'

FB = Namespace('f:')
FBO = Namespace('fbo:')


def generate_ontology(row):
    tokens = row.split("\t")
    subj, pred, obj = tokens[0], tokens[1], tokens[2]
    onto = []
    pred = pred[3:-1].split(".")

    if len(pred) == 3: 
        fdomain = f"/{pred[0]}"
        ftype = f"/{pred[0]}/{pred[1]}"
        fprop = f"/{pred[0]}/{pred[1]}/{pred[2]}"

        udomain = f"{pred[0]}"
        utype = f"{pred[0]}.{pred[1]}"
        uprop = f"{pred[0]}.{pred[1]}.{pred[2]}"

        # domain
        onto.append((URI(FBO+udomain), RDF.type, RDFS.Class))
        # type (type is subclass of domain)
        onto.append((URI(FBO+utype), RDF.type, RDFS.Class))
        onto.append((URI(FBO+utype), RDFS.subClassOf, URI(FBO+udomain)))

        # property (either a relationship or a literal property)
        if (obj.startswith('<')):
            onto.append((URI(FBO+uprop), RDF.type, OWL.ObjectProperty))
        else:
            onto.append((URI(FBO+uprop), RDF.type, OWL.DatatypeProperty))
        # property domain
        onto.append((URI(FBO+uprop), RDFS.domain, URI(FBO+utype)))

        # labels (to improve readability)
        onto.append((URI(FBO+udomain), RDFS.label, Literal(fdomain)))
        onto.append((URI(FBO+utype), RDFS.label, Literal(ftype)))
        onto.append((URI(FBO+uprop), RDFS.label, Literal(fprop)))
    return (subj, onto)


def format_string(row):
    s, p, o = row[0], row[1], row[2]
    if isinstance(o, Literal):
        return f"<{s}>\t<{p}>\t\"{o}\"\t."
    else:
        return f"<{row[0]}>\t<{row[1]}>\t<{row[2]}>\t."

def run_job(rdd):
    rdd = rdd \
        .map(generate_ontology) \
        .flatMapValues(lambda x: x) \
        .map(lambda row: row[1]) \
        .distinct() \
        .map(format_string) \
        .repartition(1) \
        .saveAsTextFile(TMPDIR)

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